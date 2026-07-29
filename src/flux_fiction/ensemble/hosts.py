"""Multi-host experiment splitting.

One campaign, several clusters. The task grid is generated deterministically
from the campaign spec, so every host derives byte-identical task definitions
(same shake seeds, same rabbit seeds, same generated traces) without any
coordination -- that is what keeps a split experiment comparable across
policies. This module only decides *which* tasks each host runs and *how* that
host should run them.

Layout:
  shared_root (on a globally-mounted filesystem, e.g. /usr/WS1)
      campaign_spec.json      the spec every host agreed on
      tasks_all.jsonl         the full task grid, for auditing/merging
      hosts/<host>/assignment.json
      hosts/<host>/results.csv    published when that host drains
  output_root (per host, on that cluster's own lustre)
      <campaign>-<host>/...       the usual campaign root, heavy outputs
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import os
from pathlib import Path
import socket
from typing import Any

from flux_fiction.ensemble.config import (
    CampaignSpec,
    EnsembleConfigError,
    QueueLimit,
    SubmissionSettings,
)


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - py<3.11
        import tomli as tomllib  # type: ignore

    with path.open("rb") as f:
        data = tomllib.load(f)
    if not isinstance(data, dict):
        raise EnsembleConfigError(f"Host config is not a TOML table: {path}")
    return data


@dataclass(frozen=True)
class HostSettings:
    name: str
    output_root: str
    backend: str = "flux"
    queues: list[str] = field(default_factory=lambda: ["pdebug"])
    # Concurrent simulations packed onto one node == campaign batch_size.
    # Scale this to the node's core count: a 48-core node should not run the
    # same number of replicas as a 112-core one.
    runs_per_node: int = 1
    max_active: int | None = None
    max_submit_per_hour: int | None = None
    walltime_minutes: int = 55
    # Relative weight when dividing the task grid between hosts.
    share: int = 1
    hostname_match: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class HostConfig:
    path: str
    shared_root: str
    hosts: dict[str, HostSettings]

    def require(self, name: str) -> HostSettings:
        if name not in self.hosts:
            known = ", ".join(sorted(self.hosts)) or "<none>"
            raise EnsembleConfigError(f"Unknown host {name!r}; configured hosts: {known}")
        return self.hosts[name]


def load_host_config(path: str | os.PathLike[str]) -> HostConfig:
    cfg_path = Path(path).expanduser().resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Host config not found: {cfg_path}")
    raw = _load_toml(cfg_path)

    version = int(raw.get("version", 1))
    if version != 1:
        raise EnsembleConfigError(f"Unsupported host config version {version!r}; expected 1")

    shared_root = raw.get("shared_root")
    if not shared_root:
        raise EnsembleConfigError(
            "host config requires shared_root (a path mounted on every host) so all "
            "hosts share one experiment context"
        )

    hosts_raw = raw.get("hosts") or {}
    if not isinstance(hosts_raw, dict) or not hosts_raw:
        raise EnsembleConfigError("host config requires at least one [hosts.<name>] table")

    hosts: dict[str, HostSettings] = {}
    for name, item in hosts_raw.items():
        if not isinstance(item, dict):
            raise EnsembleConfigError(f"hosts.{name} must be a table")
        output_root = item.get("output_root")
        if not output_root:
            raise EnsembleConfigError(f"hosts.{name}.output_root is required")
        backend = str(item.get("backend", "flux")).strip().lower()
        if backend not in {"flux", "slurm", "auto"}:
            raise EnsembleConfigError(
                f"hosts.{name}.backend must be 'flux', 'slurm', or 'auto'; got {backend!r}"
            )
        share = int(item.get("share", 1))
        if share < 0:
            raise EnsembleConfigError(f"hosts.{name}.share must be >= 0")
        runs_per_node = int(item.get("runs_per_node", 1))
        if runs_per_node < 1:
            raise EnsembleConfigError(f"hosts.{name}.runs_per_node must be >= 1")
        hosts[str(name)] = HostSettings(
            name=str(name),
            output_root=str(output_root),
            backend=backend,
            queues=[str(q) for q in (item.get("queues") or ["pdebug"])],
            runs_per_node=runs_per_node,
            max_active=(int(item["max_active"]) if item.get("max_active") is not None else None),
            max_submit_per_hour=(
                int(item["max_submit_per_hour"])
                if item.get("max_submit_per_hour") is not None
                else None
            ),
            walltime_minutes=int(item.get("walltime_minutes", 55)),
            share=share,
            hostname_match=[str(m) for m in (item.get("hostname_match") or [])],
        )
    return HostConfig(path=str(cfg_path), shared_root=str(shared_root), hosts=hosts)


def detect_host(config: HostConfig, hostname: str | None = None) -> str | None:
    """Best-effort identification of which configured host we are running on."""
    name = (hostname or socket.gethostname() or "").strip().lower()
    if not name:
        return None
    for host in config.hosts.values():
        for pattern in host.hostname_match or [host.name]:
            if name.startswith(str(pattern).lower()):
                return host.name
    return None


def assignment_cycle(config: HostConfig) -> list[str]:
    """Weighted round-robin slot order; stable regardless of TOML key order."""
    cycle: list[str] = []
    for name in sorted(config.hosts):
        cycle.extend([name] * max(0, config.hosts[name].share))
    return cycle


def partition_tasks(
    tasks: list[dict[str, Any]], config: HostConfig
) -> dict[str, list[dict[str, Any]]]:
    """Split the task grid across hosts by share weight.

    Deterministic and coordination-free: every host computes the same mapping
    from the same inputs, so each can run its slice without a broker. Tasks are
    interleaved rather than blocked so each host gets a representative mix of
    the policy grid instead of, say, all the conservative runs.
    """
    cycle = assignment_cycle(config)
    out: dict[str, list[dict[str, Any]]] = {name: [] for name in config.hosts}
    if not cycle:
        raise EnsembleConfigError("no host has a non-zero share; nothing to assign")
    for index, task in enumerate(tasks):
        out[cycle[index % len(cycle)]].append(task)
    return out


def host_adjusted_spec(spec: CampaignSpec, host: HostSettings) -> CampaignSpec:
    """Overlay this host's runtime settings onto the shared campaign spec.

    The experiment definition (trace, slice, shake, grid) is untouched -- only
    where it runs and how it is packed. The campaign name is suffixed with the
    host so two clusters sharing a filesystem cannot collide, and so job names
    identify their origin.
    """
    limits = {
        queue: QueueLimit(
            name=queue,
            max_active=host.max_active,
            max_submit_per_hour=host.max_submit_per_hour,
        )
        for queue in host.queues
    }
    campaign = replace(
        spec.campaign,
        name=f"{spec.campaign.name}-{host.name}",
        output_root=host.output_root,
        batch_size=host.runs_per_node,
        walltime_minutes=host.walltime_minutes,
    )
    submission = SubmissionSettings(
        queues=list(host.queues),
        queue_limits=limits,
        job_name_prefix=spec.submission.job_name_prefix,
        backend=host.backend,
    )
    return replace(spec, campaign=campaign, submission=submission)


def shared_host_dir(config: HostConfig, host_name: str) -> Path:
    return Path(config.shared_root).expanduser() / "hosts" / host_name
