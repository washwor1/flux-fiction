"""
Flux-native replica placement for the parallel runner.

Each replica is submitted as a job in an enclosing Flux instance:

    flux run -n1 --cores-per-task=<physical_cores // max_concurrent> -o cpu-affinity=default ...

Fluxion allocates cores exclusively per job, so concurrent replicas land on
disjoint contiguous core blocks. Because the blocks are equal-sized they align
with NUMA domain boundaries whenever the split divides evenly (on a 4-domain
96-core node: P=2 -> two whole domains each, P=4 -> one domain, P=8 -> half a
domain, P=16 -> a quarter). The job shell binds every task to its allocated
cores -- covering all SMT siblings of each core -- and a nested ``flux start``
inside the job inherits the mask, so the replica's whole broker tree stays
confined. When a replica finishes, the scheduler hands its exact block to the
next queued run, so there is no static per-slot core bookkeeping to maintain
across a strong-scaling sweep.

With ``max_concurrent == 1`` the replica runs unbound (no ``flux run``
wrapper), matching the "whole node" behavior of the unpinned baseline. Memory
is NOT bound by Flux; locality still comes from Linux first-touch on the bound
cores.

Environment knobs
-----------------
``FLUX_FICTION_FLUX_LAUNCH``       enable (1/true/yes/on). Default: off.
``FLUX_FICTION_FLUX_LAUNCH_CORES`` override cores per replica. Default:
                                   physical_cores // max_concurrent.

``run_ff_parallel`` re-execs itself under ``flux start`` when the flag is set
and no enclosing instance exists (no FLUX_URI), so the runner process itself
becomes the initial program of a node-local scheduler instance.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import shutil
import sys
from typing import Optional

logger = logging.getLogger(__name__)

ENABLE_ENV = "FLUX_FICTION_FLUX_LAUNCH"
CORES_ENV = "FLUX_FICTION_FLUX_LAUNCH_CORES"

_TRUTHY = {"1", "true", "yes", "on"}

_SYS_NODE = "/sys/devices/system/node"
_SYS_CPU = "/sys/devices/system/cpu"


def _parse_cpu_list(text: str) -> list[int]:
    """Expand a Linux cpu-list such as ``"0-3,8,10-11"`` into sorted ints."""
    out: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            out.extend(range(int(lo), int(hi) + 1))
        else:
            out.append(int(part))
    return sorted(set(out))


@dataclass(frozen=True)
class _Topology:
    """NUMA topology as domains -> physical cores -> SMT-sibling OS cpu ids."""

    domains: list[list[tuple[int, ...]]]

    def total_physical_cores(self) -> int:
        return sum(len(d) for d in self.domains)


def _read_topology(sys_node: str = _SYS_NODE, sys_cpu: str = _SYS_CPU) -> Optional["_Topology"]:
    """
    Read NUMA/SMT topology from sysfs. Returns ``None`` when unavailable.

    Each domain is the list of its physical cores; each physical core is the
    tuple of its SMT-sibling OS cpu ids (restricted to cpus actually present in
    that domain). Only used to size ``--cores-per-task`` for the launched jobs.
    """
    node_root = Path(sys_node)
    if not node_root.is_dir():
        return None
    node_dirs = sorted(
        (d for d in node_root.glob("node[0-9]*") if d.is_dir()),
        key=lambda d: int(d.name[len("node"):]),
    )
    if not node_dirs:
        return None

    domains: list[list[tuple[int, ...]]] = []
    for nd in node_dirs:
        try:
            cpus = _parse_cpu_list((nd / "cpulist").read_text())
        except OSError:
            continue
        cpu_set = set(cpus)
        seen: set[int] = set()
        cores: list[tuple[int, ...]] = []
        for cpu in cpus:
            if cpu in seen:
                continue
            sib_path = Path(sys_cpu) / f"cpu{cpu}" / "topology" / "thread_siblings_list"
            try:
                siblings = tuple(c for c in _parse_cpu_list(sib_path.read_text()) if c in cpu_set)
            except OSError:
                siblings = (cpu,)
            if not siblings:
                siblings = (cpu,)
            seen.update(siblings)
            cores.append(siblings)
        if cores:
            domains.append(cores)

    if not domains:
        return None
    return _Topology(domains=domains)


def enabled() -> bool:
    return (os.environ.get(ENABLE_ENV) or "").strip().lower() in _TRUTHY


def _total_physical_cores() -> Optional[int]:
    topology = _read_topology()
    if topology is not None:
        return topology.total_physical_cores()
    return os.cpu_count()


def cores_per_replica(max_concurrent: int) -> Optional[int]:
    raw = os.environ.get(CORES_ENV)
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            logger.warning("Ignoring non-integer %s=%r", CORES_ENV, raw)
    total = _total_physical_cores()
    if not total:
        return None
    return max(1, total // max(1, max_concurrent))


def launch_prefix(name: str, max_concurrent: int) -> list[str]:
    """
    Return a ``flux run`` argv prefix sizing this replica's job, or ``[]`` when
    flux launch is off, unavailable, or unnecessary (``max_concurrent == 1``).
    """
    if not enabled():
        return []
    if max_concurrent <= 1:
        return []
    if not os.environ.get("FLUX_URI"):
        logger.warning(
            "%s set but no enclosing Flux instance (FLUX_URI unset); launching unmanaged",
            ENABLE_ENV,
        )
        return []
    if shutil.which("flux") is None:
        logger.warning("%s set but no flux executable on PATH; launching unmanaged", ENABLE_ENV)
        return []
    cores = cores_per_replica(max_concurrent)
    if cores is None:
        logger.warning("%s set but core count is undiscoverable; launching unmanaged", ENABLE_ENV)
        return []
    return [
        "flux",
        "run",
        "-n1",
        f"--cores-per-task={cores}",
        "-o",
        "cpu-affinity=default",
        f"--job-name={name}",
        "--",
    ]


def maybe_reexec_under_flux_start() -> None:
    """
    When flux launch is requested but there is no enclosing instance, replace
    this process with ``flux start -- <same runner command>`` so the runner
    becomes the initial program of a fresh node-local instance. No-op (with a
    warning) when flux is missing; launch_prefix then falls back to unmanaged.
    """
    if not enabled() or os.environ.get("FLUX_URI"):
        return
    flux = shutil.which("flux")
    if flux is None:
        logger.warning("%s set but no flux executable on PATH; cannot start an instance", ENABLE_ENV)
        return
    argv = [
        flux,
        "start",
        "--",
        sys.executable,
        "-m",
        "flux_fiction.cli.run_ff_parallel",
        *sys.argv[1:],
    ]
    os.execv(flux, argv)
