from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import glob
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any

from flux_fiction.ensemble.config import CampaignSpec
from flux_fiction.ensemble.trace import (
    apply_rabbit_requests,
    apply_shake,
    load_normalized_trace,
    write_trace_csv,
)


# Flux states first, then the Slurm equivalents (squeue/sacct long forms).
PENDING_STATES = {"DEPEND", "PRIORITY", "SCHED", "S", "PENDING", "PD", "REQUEUED"}
RUNNING_STATES = {
    "RUN", "R", "CLEANUP", "C", "RUNNING",
    "COMPLETING", "CONFIGURING", "CG", "CF", "RESIZING", "SUSPENDED",
}
TERMINAL_STATES = {"succeeded", "failed"}
DEFAULT_CONTAINER_IMAGE = "localhost/flux-fiction-dev:latest"

# Settings a campaign may declare in `worker_env` that configure the worker's
# container runtime rather than the simulation inside it. They are read when
# worker.sh is generated and are deliberately not forwarded to the container.
_CONTAINER_RUNTIME_SETTINGS = frozenset({
    "FLUX_FICTION_WORKSPACE_ROOT",
    "FLUX_FICTION_CONTAINER_INSTALLS",
    "FLUX_FICTION_CONTAINER_IMAGE",
    "FLUX_FICTION_CONTAINER_IMAGE_TAR",
    "FLUX_FICTION_CONTAINER_PYTHONPATH",
})
DEFAULT_SPACK_FLUXION_PREFIX = (
    "/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/"
    "gcc-12.2.0/flux-sched-0.53.0-rben2bzg2txikxftd7w34mcio3u6mzdc"
)
DEFAULT_SPACK_FAKETIME_PREFIX = (
    "/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/"
    "gcc-12.2.0/faketime-0.9.10-n2lvnlfsett6t3xoh4h4hrfb7ew3bppo"
)
DEFAULT_SPACK_GCC_RUNTIME = (
    "/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/"
    "gcc-12.2.0/gcc-runtime-12.2.0-w244juoi3w2lwcowcvtkm6t5vqu5wd2t"
)
DEFAULT_SPACK_VIEW = (
    "/usr/WS1/ashworth12/package_managers/spack/var/spack/environments/"
    "emulator/.spack-env/view"
)
DEFAULT_SPACK_NINJA_PREFIX = (
    "/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/"
    "gcc-12.2.0/ninja-1.12.1-buf67bdsc6njmhhy3ipymclhnni74us2"
)
DEFAULT_MESON_PYTHONPATH = (
    "/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/"
    "host-python-deps/meson-1.8.2"
)
MISSING_FLUX_ROW_GRACE_SECONDS = 300.0
TERMINAL_RELEASE_GRACE_SECONDS = float(
    os.environ.get("FLUX_FICTION_TERMINAL_RELEASE_GRACE_SECONDS", "120")
)
REACTOR_FALLBACK_POLL_SECONDS = 10.0
FLUX_PYTHON_TIMEOUT_SECONDS = 120.0


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_run_id(root: Path) -> str:
    material = f"{utcnow_iso()}:{os.getpid()}:{root}:{time.time_ns()}"
    return "r" + hashlib.sha1(material.encode("utf-8")).hexdigest()[:12]


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    finally:
        tmp.unlink(missing_ok=True)


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    with path.open("rb") as f:
        data = tomllib.load(f)
    return data if isinstance(data, dict) else {}


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if value is None:
        return '""'
    return json.dumps(str(value))


def write_flux_fiction_toml(path: Path, data: dict[str, Any]) -> None:
    lines = ["[flux_fiction]"]
    for key, value in data.items():
        lines.append(f"{key} = {_toml_value(value)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _slug(value: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in str(value))
    clean = clean.strip(".-_")
    return clean or "x"


def campaign_root(spec: CampaignSpec) -> Path:
    return Path(spec.campaign.output_root).expanduser().resolve() / spec.campaign.name


def tasks_path(root: Path) -> Path:
    return root / "tasks.jsonl"


def batches_path(root: Path) -> Path:
    return root / "batches.jsonl"


def state_path(root: Path) -> Path:
    return root / "state.json"


def status_path(root: Path) -> Path:
    return root / "status.json"


def _task_seed(*parts: Any) -> int:
    digest = hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _rabbit_is_active(spec: CampaignSpec, job_pct: float, ceiling_pct: float) -> bool:
    """Whether this pairing actually puts rabbit storage in the trace.

    Mirrors the guard in apply_rabbit_requests: with either knob at zero (or no
    known capacity) every request is written as 0, so the trace is identical to
    the plain control regardless of the other knob's value.
    """
    capacity = float(getattr(spec.rabbit, "ceiling_capacity_gib", 0.0) or 0.0)
    return float(job_pct) > 0 and float(ceiling_pct) > 0 and capacity > 0


def generate_tasks(spec: CampaignSpec) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    ordinal = 0
    collapse = bool(getattr(spec.grid, "collapse_zero_rabbit", True))
    distributions = list(
        getattr(spec.grid, "rabbit_distributions", None) or [spec.rabbit.distribution]
    )
    for duplicate in range(spec.shake.duplicates):
        shake_seed = int(spec.shake.seed) + duplicate
        for queue_policy in spec.grid.queue_policies:
            for match_policy in spec.grid.match_policies:
                # One zero-rabbit control per policy cell; the rest of the
                # degenerate pairings would be exact duplicates of it. The
                # control is shared across distributions too -- with no
                # requests written, the draw shape cannot matter.
                control_emitted = False
                for distribution in distributions:
                    for rabbit_job_pct in spec.grid.rabbit_job_percentages:
                        for rabbit_ceiling_pct in spec.grid.rabbit_ceiling_percentages:
                            rabbit_active = _rabbit_is_active(
                                spec, rabbit_job_pct, rabbit_ceiling_pct
                            )
                            if collapse and not rabbit_active:
                                if control_emitted:
                                    continue
                                control_emitted = True
                            ordinal += 1
                            seed_tag = f"s{shake_seed}"
                            parts = [
                                f"t{ordinal:06d}",
                                seed_tag,
                                f"q{_slug(queue_policy)}",
                                f"m{_slug(match_policy)}",
                                f"rj{int(rabbit_job_pct):03d}",
                                f"rc{int(rabbit_ceiling_pct):03d}",
                            ]
                            # Only tag the distribution when more than one is
                            # swept, so single-distribution campaigns keep the
                            # task ids they have always had.
                            if len(distributions) > 1:
                                parts.append(f"d{_slug(distribution)}")
                            tasks.append(
                                {
                                    "task_id": "__".join(parts),
                                    "ordinal": ordinal,
                                    "duplicate": duplicate,
                                    "shake_seed": shake_seed,
                                    "rabbit_seed": _task_seed(
                                        spec.shake.seed,
                                        duplicate,
                                        queue_policy,
                                        match_policy,
                                        rabbit_job_pct,
                                        rabbit_ceiling_pct,
                                        *( [distribution] if len(distributions) > 1 else [] ),
                                    ),
                                    "queue_policy": queue_policy,
                                    "match_policy": match_policy,
                                    "rabbit_job_percentage": rabbit_job_pct,
                                    "rabbit_ceiling_percentage": rabbit_ceiling_pct,
                                    "rabbit_distribution": distribution,
                                    # False means the trace carries no rabbit
                                    # requests at all -- this is the control, even
                                    # if one of the percentages above is non-zero.
                                    "rabbit_active": rabbit_active,
                                }
                            )
    return tasks


def batch_tasks(tasks: list[dict[str, Any]], *, batch_size: int) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    batch_size = max(1, int(batch_size))
    for idx in range(0, len(tasks), batch_size):
        chunk = tasks[idx:idx + batch_size]
        batch_id = f"b{len(batches) + 1:06d}"
        batches.append(
            {
                "batch_id": batch_id,
                "task_ids": [task["task_id"] for task in chunk],
                "task_count": len(chunk),
            }
        )
    return batches


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                data = json.loads(line)
                if isinstance(data, dict):
                    rows.append(data)
    return rows


def initialize_campaign(spec: CampaignSpec, *, force: bool = False) -> Path:
    root = campaign_root(spec)
    root.mkdir(parents=True, exist_ok=True)
    spec_snapshot = root / "campaign_spec.json"
    if tasks_path(root).exists() and not force:
        return root

    tasks = generate_tasks(spec)
    batches = batch_tasks(tasks, batch_size=spec.campaign.batch_size)
    _write_jsonl(tasks_path(root), tasks)
    _write_jsonl(batches_path(root), batches)
    write_json(spec_snapshot, spec.to_jsonable())
    state = {
        "version": 1,
        "campaign": spec.campaign.name,
        "created_at": utcnow_iso(),
        "run_id": _new_run_id(root),
        "submitted": {},
        "submission_history": [],
    }
    if force or not state_path(root).exists():
        write_json(state_path(root), state)
    update_status(root, spec=spec)
    return root


def initialize_partial_campaign(
    spec: CampaignSpec,
    host_name: str,
    host_config: Any,
    *,
    force: bool = False,
) -> tuple[Path, dict[str, Any]]:
    """Initialize this host's slice of a multi-host campaign.

    The full grid is generated from the shared spec (deterministic, so every
    host derives identical task definitions), then partitioned by share weight.
    Only this host's tasks are written into its campaign root, batched at this
    host's runs_per_node -- from the launcher's perspective it is an ordinary
    campaign, so every existing mechanism (tracking, timeout harvest,
    progress.csv, results.csv) works unchanged.
    """
    from flux_fiction.ensemble.hosts import (
        assignment_cycle,
        host_adjusted_spec,
        partition_tasks,
        shared_host_dir,
    )

    host = host_config.require(host_name)
    adjusted = host_adjusted_spec(spec, host)
    root = campaign_root(adjusted)

    all_tasks = generate_tasks(spec)
    assigned = partition_tasks(all_tasks, host_config)[host_name]
    assignment = {
        "campaign": spec.campaign.name,
        "host": host_name,
        "created_at": utcnow_iso(),
        "host_config": getattr(host_config, "path", None),
        # Recorded so a later run can detect that the host set or weights
        # changed, which would silently re-shuffle the partition.
        "hosts": {name: h.share for name, h in sorted(host_config.hosts.items())},
        "assignment_cycle": assignment_cycle(host_config),
        "total_tasks": len(all_tasks),
        "assigned_tasks": len(assigned),
        "runs_per_node": host.runs_per_node,
        "backend": host.backend,
        "queues": list(host.queues),
        "campaign_root": str(root),
        "task_ids": [task["task_id"] for task in assigned],
    }

    root.mkdir(parents=True, exist_ok=True)
    if tasks_path(root).exists() and not force:
        existing = read_json(root / "assignment.json")
        if existing.get("hosts") and existing.get("hosts") != assignment["hosts"]:
            raise RuntimeError(
                f"Host shares changed since this campaign root was created "
                f"({existing.get('hosts')} -> {assignment['hosts']}). The partition would "
                f"shift under the already-submitted work; use --force on a fresh root."
            )
        return root, read_json(root / "assignment.json") or assignment

    if not assigned:
        raise RuntimeError(
            f"Host {host_name!r} was assigned no tasks (share={host.share}); "
            "give it a non-zero share or run it from another host."
        )

    batches = batch_tasks(assigned, batch_size=host.runs_per_node)
    _write_jsonl(tasks_path(root), assigned)
    _write_jsonl(batches_path(root), batches)
    write_json(root / "campaign_spec.json", adjusted.to_jsonable())
    write_json(root / "assignment.json", assignment)
    state = {
        "version": 1,
        "campaign": adjusted.campaign.name,
        "created_at": utcnow_iso(),
        "run_id": _new_run_id(root),
        "host": host_name,
        "submitted": {},
        "submission_history": [],
    }
    if force or not state_path(root).exists():
        write_json(state_path(root), state)

    # Publish the shared context: the full grid plus this host's assignment,
    # on a filesystem every cluster can see (per-host lustre is not shared).
    try:
        shared = Path(host_config.shared_root).expanduser()
        _write_jsonl(shared / "tasks_all.jsonl", all_tasks)
        write_json(shared / "campaign_spec.json", spec.to_jsonable())
        write_json(shared_host_dir(host_config, host_name) / "assignment.json", assignment)
    except OSError as exc:
        print(f"WARNING: could not publish shared context: {exc}", file=sys.stderr)

    update_status(root, spec=adjusted)
    return root, assignment


def publish_host_results(root: Path, host_config: Any, host_name: str) -> Path | None:
    """Copy this host's results.csv into the shared context for merging."""
    from flux_fiction.ensemble.hosts import shared_host_dir

    source = results_csv_path(root)
    if not source.exists():
        return None
    try:
        target_dir = shared_host_dir(host_config, host_name)
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / "results.csv"
        shutil.copyfile(source, target)
        progress = progress_csv_path(root)
        if progress.exists():
            shutil.copyfile(progress, target_dir / "progress.csv")
        return target
    except OSError as exc:
        print(f"WARNING: could not publish results for {host_name}: {exc}", file=sys.stderr)
        return None


def merge_host_results(host_config: Any, out_path: Path | None = None) -> tuple[Path, dict[str, int]]:
    """Concatenate every host's published results.csv into one table."""
    shared = Path(host_config.shared_root).expanduser()
    out_path = out_path or (shared / "results_all.csv")
    rows: list[dict[str, str]] = []
    per_host: dict[str, int] = {}
    for name in sorted(host_config.hosts):
        results = shared / "hosts" / name / "results.csv"
        # Open and catch rather than pre-checking exists(). The shared root is
        # NFS, and a file another cluster published seconds ago can be readable
        # by name while the cached directory entry still says it is not there --
        # observed 2026-07-27, when dane's freshly published table was silently
        # dropped from the merge. Opening it forces the lookup.
        try:
            with results.open(newline="", encoding="utf-8") as f:
                host_rows = [dict(row) for row in csv.DictReader(f)]
        except OSError:
            per_host[name] = 0
            continue
        for row in host_rows:
            row["host"] = name
        rows.extend(host_rows)
        per_host[name] = len(host_rows)

    fieldnames = ["host"] + [c for c in RESULTS_COLUMNS]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return out_path, per_host


def load_campaign_snapshot(root: Path) -> CampaignSpec:
    from flux_fiction.ensemble.config import (
        CampaignSettings,
        CampaignSpec,
        FluxSettings,
        GridSettings,
        InputSettings,
        RabbitSettings,
        ShakeSettings,
        SubmissionSettings,
        QueueLimit,
    )

    raw = read_json(root / "campaign_spec.json")
    if not raw:
        raise FileNotFoundError(f"Campaign snapshot not found: {root / 'campaign_spec.json'}")
    limits = {
        name: QueueLimit(**limit)
        for name, limit in raw["submission"].get("queue_limits", {}).items()
    }
    submission_raw = raw["submission"]
    return CampaignSpec(
        path=raw["path"],
        version=int(raw["version"]),
        campaign=CampaignSettings(**raw["campaign"]),
        input=InputSettings(**raw["input"]),
        shake=ShakeSettings(**raw["shake"]),
        rabbit=RabbitSettings(**raw["rabbit"]),
        grid=GridSettings(**raw["grid"]),
        flux=FluxSettings(**raw["flux"]),
        submission=SubmissionSettings(
            queues=list(submission_raw["queues"]),
            job_name_prefix=submission_raw.get("job_name_prefix", "ffe"),
            queue_limits=limits,
            backend=submission_raw.get("backend", "flux"),
        ),
    )


def _load_base_config(spec: CampaignSpec) -> dict[str, Any]:
    cfg_doc = _load_toml(Path(spec.flux.base_config))
    cfg = dict(cfg_doc.get("flux_fiction", cfg_doc))
    cfg.update(spec.flux.config_overrides or {})
    if spec.flux.resource_file:
        cfg["resource_file"] = spec.flux.resource_file
        cfg.pop("resource_R", None)
    if spec.flux.resource_R:
        cfg["resource_R"] = spec.flux.resource_R
        cfg.pop("resource_file", None)
    return cfg


def patch_scheduler_config(spec: CampaignSpec, task: dict[str, Any]) -> dict[str, Any]:
    if spec.flux.base_config_json:
        data = json.loads(Path(spec.flux.base_config_json).read_text(encoding="utf-8"))
    else:
        data = {}
    qmanager = data.setdefault("sched-fluxion-qmanager", {})
    resource = data.setdefault("sched-fluxion-resource", {})
    qmanager["queue-policy"] = task["queue_policy"]
    resource["match-policy"] = task["match_policy"]
    return data


def materialize_task_inputs(
    spec: CampaignSpec,
    task: dict[str, Any],
    task_dir: Path,
    *,
    base_rows: list[dict[str, str]] | None = None,
) -> dict[str, Path]:
    rows = base_rows
    if rows is None:
        rows = load_normalized_trace(
            spec.input.trace,
            trace_format=spec.input.trace_format,
            slice_start=spec.input.slice_start,
            slice_count=spec.input.slice_count,
            cpus_per_node=spec.input.cpus_per_node,
            rabbit_field=spec.rabbit.field_name,
        )

    shaken = apply_shake(
        rows,
        seed=int(task["shake_seed"]),
        attributes=list(spec.shake.attributes),
        job_percentage=spec.shake.job_percentage,
        degree_seconds=spec.shake.degree_seconds,
        relative_cap_percent=spec.shake.relative_cap_percent,
        relative_degree_percent=spec.shake.relative_degree_percent,
        max_nodes=spec.shake.max_nodes,
    )
    with_rabbit = apply_rabbit_requests(
        shaken,
        seed=int(task["rabbit_seed"]),
        job_percentage=float(task["rabbit_job_percentage"]),
        # Resolved per ceiling_basis: cluster total, or one rabbit node.
        capacity_gib=float(spec.rabbit.ceiling_capacity_gib or 0.0),
        ceiling_percent=float(task["rabbit_ceiling_percentage"]),
        distribution=str(task.get("rabbit_distribution") or spec.rabbit.distribution),
        field_name=spec.rabbit.field_name,
        mean_fraction=getattr(spec.rabbit, "mean_fraction", 0.5),
        sigma_fraction=getattr(spec.rabbit, "sigma_fraction", 1.0 / 6.0),
        tail_alpha=getattr(spec.rabbit, "tail_alpha", 1.5),
        tail_min_gib=getattr(spec.rabbit, "tail_min_gib", 1.0),
    )

    trace_path = task_dir / "trace.csv"
    scheduler_path = task_dir / "scheduler.json"
    config_path = task_dir / "config.toml"

    write_trace_csv(trace_path, with_rabbit, rabbit_field=spec.rabbit.field_name)
    write_json(scheduler_path, patch_scheduler_config(spec, task))

    cfg = _load_base_config(spec)
    cfg["job_traces"] = str(trace_path)
    cfg["config_json"] = str(scheduler_path)
    write_flux_fiction_toml(config_path, cfg)

    write_json(
        task_dir / "task.json",
        {
            **task,
            "trace_rows": len(with_rabbit),
            "generated_trace": str(trace_path),
            "generated_config": str(config_path),
            "generated_scheduler_config": str(scheduler_path),
        },
    )
    return {
        "trace": trace_path,
        "config": config_path,
        "scheduler": scheduler_path,
    }


def render_parallel_manifest(
    path: Path,
    spec: CampaignSpec,
    tasks: list[dict[str, Any]],
    generated: dict[str, dict[str, Path]],
    output_root: Path,
) -> None:
    lines = [
        "version = 1",
        "",
        "[parallel]",
        f"max_concurrent = {max(1, min(spec.campaign.batch_size, len(tasks)))}",
        "fail_fast = false",
        'progress_mode = "summary"',
        f"default_no_faketime = {'true' if spec.campaign.no_faketime else 'false'}",
        f"default_broker_log_level = {spec.campaign.broker_log_level}",
        f"output_root = {json.dumps(str(output_root))}",
        "summary_interval = 15.0",
        "",
    ]
    for task in tasks:
        task_id = task["task_id"]
        metadata = {
            "task_id": task_id,
            "duplicate": task["duplicate"],
            "shake_seed": task["shake_seed"],
            "queue_policy": task["queue_policy"],
            "match_policy": task["match_policy"],
            "rabbit_job_percentage": task["rabbit_job_percentage"],
            "rabbit_ceiling_percentage": task["rabbit_ceiling_percentage"],
        }
        meta = ", ".join(f"{key} = {json.dumps(str(value))}" for key, value in metadata.items())
        lines.extend(
            [
                "[[run]]",
                f"name = {json.dumps(task_id)}",
                f"config_file = {json.dumps(str(generated[task_id]['config']))}",
                f"metadata = {{ {meta} }}",
                "",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _reap_lingering_flux_processes() -> None:
    for pattern in ("flux-broker", "flux-shell", "flux-imp"):
        try:
            subprocess.run(
                ["pkill", "-9", "-f", pattern],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
            )
        except Exception:
            pass


def run_worker(root: str | os.PathLike[str], batch_id: str) -> int:
    worker_started = time.monotonic()
    campaign_dir = Path(root).expanduser().resolve()
    spec = load_campaign_snapshot(campaign_dir)
    all_tasks = {task["task_id"]: task for task in read_jsonl(tasks_path(campaign_dir))}
    batches = {batch["batch_id"]: batch for batch in read_jsonl(batches_path(campaign_dir))}
    if batch_id not in batches:
        raise SystemExit(f"Unknown batch id: {batch_id}")
    batch = batches[batch_id]
    selected = [all_tasks[task_id] for task_id in batch["task_ids"]]
    batch_root = campaign_dir / "batches" / batch_id
    generated_root = batch_root / "generated"
    parallel_root = batch_root / "parallel"
    batch_root.mkdir(parents=True, exist_ok=True)
    write_json(
        batch_root / "batch_status.json",
        {
            "batch_id": batch_id,
            "state": "preparing",
            "started_at": utcnow_iso(),
            "task_count": len(selected),
            "tasks": [task["task_id"] for task in selected],
        },
    )

    try:
        base_rows = load_normalized_trace(
            spec.input.trace,
            trace_format=spec.input.trace_format,
            slice_start=spec.input.slice_start,
            slice_count=spec.input.slice_count,
            cpus_per_node=spec.input.cpus_per_node,
            rabbit_field=spec.rabbit.field_name,
        )
        generated: dict[str, dict[str, Path]] = {}
        for task in selected:
            generated[task["task_id"]] = materialize_task_inputs(
                spec,
                task,
                generated_root / task["task_id"],
                base_rows=base_rows,
            )
        manifest = batch_root / "manifest.toml"
        render_parallel_manifest(manifest, spec, selected, generated, parallel_root)
        write_json(
            batch_root / "batch_status.json",
            {
                "batch_id": batch_id,
                "state": "running",
                "started_at": utcnow_iso(),
                "task_count": len(selected),
                "manifest": str(manifest),
            },
        )
        cmd = [
            sys.executable,
            "-m",
            "flux_fiction.cli.run_ff_parallel",
            str(manifest),
            "--max-concurrent",
            str(max(1, min(spec.campaign.batch_size, len(selected)))),
            "--output-root",
            str(parallel_root),
        ]
        run_env = dict(os.environ)
        if spec.campaign.no_broker_log_file:
            # Honored by flux_fiction.parallel.runner: omits the broker's
            # --setattr=log-filename so no broker.log is written at all.
            run_env["FLUX_FICTION_NO_BROKER_LOG_FILE"] = "true"
        # Real time left in this batch's allocation, so each simulation can
        # finalize and write partial results rather than being killed at the
        # wall with an empty output directory.
        if "FLUX_FICTION_WALLTIME_BUDGET_SECONDS" not in run_env:
            # Measure from the job's start, not from this process's start:
            # worker.sh stamps FLUX_FICTION_WORKER_EPOCH before pulling the
            # container image, and that image load can be tens of seconds. Using
            # the in-container start would over-estimate the remaining time by
            # exactly that much and the wall would land during finalization.
            elapsed = time.monotonic() - worker_started
            job_epoch = run_env.get("FLUX_FICTION_WORKER_EPOCH", "").strip()
            if job_epoch:
                try:
                    elapsed = max(elapsed, time.time() - float(job_epoch))
                except ValueError:
                    pass
            remaining = max(1, int(spec.campaign.walltime_minutes)) * 60.0 - elapsed
            if remaining > 0:
                run_env["FLUX_FICTION_WALLTIME_BUDGET_SECONDS"] = f"{remaining:.0f}"
        run_env.setdefault(
            "FLUX_FICTION_FINALIZE_RESERVE_SECONDS",
            f"{float(getattr(spec.campaign, 'finalize_reserve_seconds', 120.0)):.0f}",
        )
        start = time.monotonic()
        proc = subprocess.run(cmd, cwd=str(campaign_dir), env=run_env)
        elapsed = time.monotonic() - start
        _reap_lingering_flux_processes()
        result = {
            "batch_id": batch_id,
            "state": "succeeded" if proc.returncode == 0 else "failed",
            "return_code": proc.returncode,
            "started_at": read_json(batch_root / "batch_status.json").get("started_at"),
            "finished_at": utcnow_iso(),
            "wall_seconds": elapsed,
            "task_count": len(selected),
            "tasks": [task["task_id"] for task in selected],
            "parallel_root": str(parallel_root),
        }
        if proc.returncode != 0:
            if proc.returncode < 0:
                # Negative rc from subprocess means "killed by signal N", which
                # for these batches is the walltime kill arriving mid-run. Record
                # it so reporting does not present a timeout as a crash.
                result["terminated_by_signal"] = abs(int(proc.returncode))
                result["failure_reason"] = (
                    f"flux-fiction-run-parallel was terminated by signal "
                    f"{abs(int(proc.returncode))} (walltime kill or cancel); "
                    f"see {batch_root / 'batch.log'}"
                )
            else:
                result["failure_reason"] = (
                    f"flux-fiction-run-parallel exited rc={proc.returncode}; "
                    f"see {batch_root / 'batch.log'}"
                )
        write_json(batch_root / "batch_result.json", result)
        write_json(batch_root / "batch_status.json", result)
        if not spec.campaign.keep_generated_traces:
            for trace_file in generated_root.glob("*/trace.csv"):
                trace_file.unlink(missing_ok=True)
        return int(proc.returncode)
    except Exception as exc:
        _reap_lingering_flux_processes()
        result = {
            "batch_id": batch_id,
            "state": "failed",
            "return_code": 1,
            "finished_at": utcnow_iso(),
            "failure_reason": repr(exc),
            "task_count": len(selected),
            "tasks": [task["task_id"] for task in selected],
        }
        write_json(batch_root / "batch_result.json", result)
        write_json(batch_root / "batch_status.json", result)
        raise


def _flux_python_json(
    source: str,
    payload: dict[str, Any],
    *,
    timeout: float = FLUX_PYTHON_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    # The Flux python bindings on this system are built for the `flux python`
    # interpreter only (python3.6 on TOSS), so all bindings work runs in short
    # `flux python -c` helper subprocesses instead of in-process imports.
    try:
        proc = subprocess.run(
            ["flux", "python", "-c", source],
            input=json.dumps(payload),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"flux python timed out after {timeout:.0f}s") from exc
    except OSError as exc:
        raise RuntimeError(f"failed to run flux python: {exc}") from exc
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"flux python failed: {detail}")
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"flux python returned invalid JSON: {proc.stdout!r}") from exc
    return data if isinstance(data, dict) else {}


def _flux_python_rows(prefix: str, *, include_inactive: bool = False) -> list[dict[str, str]]:
    script = r'''
import errno
import json
import sys

import flux
import flux.job
from flux.job import _wrapper
from flux.job.info import resulttostr, statetostr

payload = json.load(sys.stdin)
prefix = payload["prefix"]
max_entries = int(payload.get("max_entries", 100000))
include_inactive = bool(payload.get("include_inactive", False))
states = _wrapper.lib.FLUX_JOB_STATE_ACTIVE
if include_inactive:
    states |= _wrapper.lib.FLUX_JOB_STATE_INACTIVE

h = flux.Flux()
try:
    response = flux.job.job_list(
        h,
        max_entries=max_entries,
        attrs=["all"],
        states=states,
    ).get()
    jobs = response.get("jobs", [])
except OSError as exc:
    if getattr(exc, "errno", None) == errno.ENODATA:
        jobs = []
    else:
        raise

rows = []
for job in jobs:
    name = str(job.get("name", ""))
    if not name.startswith(prefix):
        continue
    result = job.get("result", 0) or 0
    rows.append({
        "id": flux.job.id_encode(job["id"]),
        "queue": str(job.get("queue", "")),
        "state": statetostr(job.get("state", 0)).upper(),
        "result": resulttostr(result).upper() if result else "-",
        "name": name,
        "t_submit": job.get("t_submit"),
        "t_run": job.get("t_run"),
    })
print(json.dumps({"rows": rows}, sort_keys=True))
'''
    payload = {
        "prefix": prefix,
        "include_inactive": include_inactive,
        "max_entries": 100000,
    }
    rows = _flux_python_json(script, payload).get("rows", [])
    return [row for row in rows if isinstance(row, dict)]


def _flux_python_rows_by_ids(jobids: list[str]) -> list[dict[str, str]]:
    if not jobids:
        return []
    script = r'''
import errno
import json
import sys

import flux
import flux.job
from flux.job.info import resulttostr, statetostr

payload = json.load(sys.stdin)
h = flux.Flux()
rows = []
for raw_jobid in payload.get("jobids", []):
    try:
        jobid = flux.job.id_parse(str(raw_jobid))
        response = flux.job.job_list_id(h, jobid, attrs=["all"]).get()
    except (OSError, ValueError):
        continue
    job = response.get("job")
    if not job:
        jobs = response.get("jobs", [])
        job = jobs[0] if jobs else None
    if not job:
        continue
    result = job.get("result", 0) or 0
    rows.append({
        "id": flux.job.id_encode(job["id"]),
        "queue": str(job.get("queue", "")),
        "state": statetostr(job.get("state", 0)).upper(),
        "result": resulttostr(result).upper() if result else "-",
        "name": str(job.get("name", "")),
        "t_submit": job.get("t_submit"),
        "t_run": job.get("t_run"),
    })
print(json.dumps({"rows": rows}, sort_keys=True))
'''
    rows = _flux_python_json(script, {"jobids": jobids}).get("rows", [])
    return [row for row in rows if isinstance(row, dict)]


def _flux_python_cancel(jobids: list[str], *, reason: str) -> int:
    if not jobids:
        return 0
    script = r'''
import json
import sys

import flux
import flux.job

payload = json.load(sys.stdin)
h = flux.Flux()
canceled = 0
for raw_jobid in payload.get("jobids", []):
    try:
        jobid = flux.job.id_parse(str(raw_jobid))
        flux.job.cancel(h, jobid, payload.get("reason"))
        canceled += 1
    except Exception:
        pass
print(json.dumps({"canceled": canceled}, sort_keys=True))
'''
    data = _flux_python_json(script, {"jobids": jobids, "reason": reason})
    return int(data.get("canceled") or 0)


def _flux_python_watch_job_events(
    jobids: list[str],
    *,
    since: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    if not jobids:
        return {"events": [], "errors": [], "timed_out": True, "watched": 0}
    script = r'''
import json
import sys

import flux
import flux.job
from flux.core.watchers import TimerWatcher

payload = json.load(sys.stdin)
raw_jobids = list(dict.fromkeys(str(item) for item in payload.get("jobids", []) if item))
since = float(payload.get("since", 0.0))
timeout = max(0.1, float(payload.get("timeout_seconds", 60.0)))

h = flux.Flux()
events = []
errors = []
ended = []
watchers = []
timed_out = {"value": False}


def stop_reactor():
    try:
        h.reactor_stop()
    except Exception:
        pass


def event_cb(future, raw_jobid):
    try:
        event = future.get_event()
    except Exception as exc:
        errors.append({"id": raw_jobid, "error": repr(exc)})
        try:
            future.cancel(stop=True)
        except Exception:
            pass
        stop_reactor()
        return

    if event is None:
        ended.append(raw_jobid)
        return

    event_timestamp = float(event.timestamp)
    if event_timestamp > since + 1.0e-9:
        events.append(
            {
                "id": raw_jobid,
                "name": str(event.name),
                "timestamp": event_timestamp,
                "context": dict(event.context),
            }
        )
        try:
            future.cancel(stop=True)
        except Exception:
            pass
        stop_reactor()


def timer_cb(flux_handle, watcher, revents, args):
    timed_out["value"] = True
    stop_reactor()


for raw_jobid in raw_jobids:
    try:
        jobid = flux.job.id_parse(raw_jobid)
        watcher = flux.job.event_watch_async(h, jobid)
        watcher.then(event_cb, raw_jobid)
        watchers.append(watcher)
    except Exception as exc:
        errors.append({"id": raw_jobid, "error": repr(exc)})

timer = TimerWatcher(h, timeout, timer_cb)
timer.start()
try:
    if watchers:
        h.reactor_run()
finally:
    try:
        timer.stop()
    except Exception:
        pass
    for watcher in watchers:
        try:
            watcher.cancel(stop=True)
        except Exception:
            pass

print(
    json.dumps(
        {
            "events": events,
            "errors": errors,
            "ended": ended,
            "timed_out": bool(timed_out["value"]),
            "watched": len(watchers),
        },
        sort_keys=True,
    )
)
'''
    return _flux_python_json(
        script,
        {
            "jobids": jobids,
            "since": float(since),
            "timeout_seconds": float(timeout_seconds),
        },
        timeout=float(timeout_seconds) + 60.0,
    )


def _merge_flux_rows(*groups: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for group in groups:
        for row in group:
            row_id = row.get("id")
            if row_id:
                merged[row_id] = row
    return list(merged.values())


def _flux_python_submit_batch(
    *,
    worker_script: Path,
    root: Path,
    batch_id: str,
    job_name: str,
    queue: str,
    output: Path,
    walltime_minutes: int,
    env: dict[str, str],
) -> str:
    script = r'''
import json
import sys

import flux
import flux.job
from flux.job import JobspecV1

payload = json.load(sys.stdin)
# An empty queue name means "do not set a queue attribute": a personal Flux
# instance (flux alloc / flux start on a Slurm cluster) has no named queues,
# and stamping one there makes every submit fail with "invalid queue".
queue = payload.get("queue") or None
jobspec = JobspecV1.from_command(
    ["bash", payload["worker_script"], payload["root"], payload["batch_id"]],
    num_tasks=1,
    cores_per_task=1,
    num_nodes=1,
    exclusive=True,
    duration=float(payload["walltime_seconds"]),
    cwd=payload["cwd"],
    name=payload["job_name"],
    output=payload["output"],
    queue=queue,
)
for key, value in payload.get("env", {}).items():
    jobspec.environment[str(key)] = str(value)
jobid = flux.job.submit(flux.Flux(), jobspec)
print(json.dumps({"id": flux.job.id_encode(jobid)}, sort_keys=True))
'''
    payload = {
        "worker_script": str(worker_script),
        "root": str(root),
        "batch_id": batch_id,
        "job_name": job_name,
        "queue": queue,
        "output": str(output),
        "walltime_seconds": max(1, int(walltime_minutes)) * 60,
        "cwd": str(Path.cwd()),
        "env": env,
    }
    fluxid = _flux_python_json(script, payload).get("id")
    if not fluxid:
        raise RuntimeError("flux python submit did not return a job id")
    return str(fluxid)


def submission_backend(spec: CampaignSpec) -> str:
    """Resolve the configured RJMS backend, honoring 'auto'."""
    backend = (getattr(spec.submission, "backend", "flux") or "flux").lower()
    if backend != "auto":
        return backend
    has_flux = bool(os.environ.get("FLUX_URI")) or Path("/run/flux/local").exists()
    if not has_flux and shutil.which("sbatch"):
        return "slurm"
    return "flux"


def _slurm_submit_batch(
    *,
    worker_script: Path,
    root: Path,
    batch_id: str,
    job_name: str,
    queue: str,
    output: Path,
    walltime_minutes: int,
    env: dict[str, str],
) -> str:
    """Submit one single-node Slurm job per batch (mirrors the Flux path).

    Deliberately one sbatch per batch rather than a single multi-node
    allocation: it matches how the Flux backend hands each batch to the system
    scheduler independently, so batches queue and start on their own instead of
    sharing fate, and no worker ends up co-resident with the launcher.
    """
    cmd = [
        "sbatch",
        "--parsable",
        "-N1",
        "--exclusive",
        f"--time={max(1, int(walltime_minutes))}",
        f"--job-name={job_name}",
        f"--output={output}",
        f"--chdir={Path.cwd()}",
    ]
    if queue:
        cmd.append(f"--partition={queue}")
    # Launch via srun rather than running the worker directly in the batch
    # script: rootless podman needs the per-task session srun sets up. A bare
    # sbatch --wrap has none, and podman fails to create its user namespace.
    # --cpu-bind=none: the step must see every core on the exclusively-allocated
    # node, because one worker fans out into runs_per_node concurrent
    # simulations. Slurm's default binding for a 1-task step is permissive on an
    # exclusive allocation (measured: 224/224 CPUs visible), but that depends on
    # site TaskPlugin config, so say it explicitly rather than rely on it.
    cmd.append(
        "--wrap=srun -N1 -n1 --cpu-bind=none bash {} {} {}".format(
            _shell_quote(worker_script), _shell_quote(root), _shell_quote(batch_id)
        )
    )
    run_env = dict(os.environ)
    run_env.update({str(k): str(v) for k, v in (env or {}).items()})
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=run_env,
            timeout=FLUX_PYTHON_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"sbatch failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"sbatch failed: {(proc.stderr or proc.stdout or '').strip()}")
    # --parsable prints "<jobid>" or "<jobid>;<cluster>"
    jobid = (proc.stdout or "").strip().split(";")[0].strip()
    if not jobid:
        raise RuntimeError("sbatch did not return a job id")
    return jobid


def _slurm_state_result(raw_state: str) -> tuple[str, str]:
    """Map a Slurm state to this module's (state, result) row convention."""
    state = (raw_state or "").strip().upper().split()[0] if raw_state else ""
    # sacct decorates cancellations as "CANCELLED by 12345"
    if state.startswith("CANCELLED"):
        state = "CANCELLED"
    if state in PENDING_STATES or state in RUNNING_STATES:
        return state, "-"
    return "INACTIVE", state or "-"


def _slurm_rows(prefix: str, *, include_inactive: bool = False) -> list[dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    user = os.environ.get("USER") or ""
    try:
        proc = subprocess.run(
            ["squeue", "-h", "-u", user, "-o", "%i|%P|%T|%j"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=FLUX_PYTHON_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"failed to query Slurm jobs (squeue): {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"squeue failed: {(proc.stderr or '').strip()}")
    for line in (proc.stdout or "").splitlines():
        parts = line.split("|")
        if len(parts) < 4:
            continue
        jobid, partition, raw_state, name = (p.strip() for p in parts[:4])
        if not name.startswith(prefix):
            continue
        state, result = _slurm_state_result(raw_state)
        rows[jobid] = {
            "id": jobid,
            "queue": partition,
            "state": state,
            "result": result,
            "name": name,
        }
    if include_inactive:
        for row in _slurm_sacct_rows(prefix=prefix):
            rows.setdefault(row["id"], row)
    return list(rows.values())


def _slurm_sacct_rows(*, prefix: str = "", jobids: list[str] | None = None) -> list[dict[str, str]]:
    cmd = ["sacct", "-n", "-X", "-P", "-o", "JobID,Partition,State,JobName"]
    if jobids:
        cmd += ["-j", ",".join(str(j) for j in jobids)]
    else:
        # Bound the history scan; campaigns do not outlive this window.
        cmd += ["--starttime", "now-7days"]
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=FLUX_PYTHON_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if proc.returncode != 0:
        return []
    rows = []
    for line in (proc.stdout or "").splitlines():
        parts = line.split("|")
        if len(parts) < 4:
            continue
        jobid, partition, raw_state, name = (p.strip() for p in parts[:4])
        if "." in jobid:  # skip job steps
            continue
        if prefix and not name.startswith(prefix):
            continue
        state, result = _slurm_state_result(raw_state)
        rows.append(
            {
                "id": jobid,
                "queue": partition,
                "state": state,
                "result": result,
                "name": name,
            }
        )
    return rows


def _slurm_cancel(jobids: list[str], *, reason: str) -> int:
    if not jobids:
        return 0
    canceled = 0
    for jobid in jobids:
        try:
            proc = subprocess.run(
                ["scancel", str(jobid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            if proc.returncode == 0:
                canceled += 1
        except (OSError, subprocess.TimeoutExpired):
            pass
    return canceled


def query_job_rows(
    spec: CampaignSpec,
    prefix: str,
    submitted_ids: list[str],
    *,
    include_inactive: bool = True,
) -> list[dict[str, str]]:
    """Backend-agnostic job listing: by name prefix, plus explicit ids."""
    if submission_backend(spec) == "slurm":
        return _merge_flux_rows(
            _slurm_rows(prefix, include_inactive=include_inactive),
            _slurm_sacct_rows(jobids=submitted_ids) if submitted_ids else [],
        )
    return _merge_flux_rows(
        _flux_rows(prefix, include_inactive=include_inactive),
        _flux_python_rows_by_ids(submitted_ids),
    )


def cancel_jobs(spec: CampaignSpec, jobids: list[str], *, reason: str) -> int:
    if submission_backend(spec) == "slurm":
        return _slurm_cancel(jobids, reason=reason)
    return _flux_python_cancel(jobids, reason=reason)


def _flux_rows(prefix: str, *, include_inactive: bool = False) -> list[dict[str, str]]:
    return _flux_python_rows(prefix, include_inactive=include_inactive)


def _flux_rows_cli(prefix: str, *, include_inactive: bool = False) -> list[dict[str, str]]:
    cmd = [
        "flux",
        "jobs",
        "-c",
        "100000",
        "--no-header",
        "-o",
        "{id} {queue} {state} {result} {name}",
    ]
    if include_inactive:
        cmd.insert(2, "-a")
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.PIPE)
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise RuntimeError(f"failed to query Flux jobs; refusing to submit blindly: {detail.strip()}") from exc
    rows = []
    for line in out.splitlines():
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        fluxid, queue, state, result, name = parts
        if name.startswith(prefix):
            rows.append(
                {
                    "id": fluxid,
                    "queue": queue,
                    "state": state.upper(),
                    "result": result.upper(),
                    "name": name,
                }
            )
    return rows


def _active_flux_rows(prefix: str) -> list[dict[str, str]]:
    return _flux_rows(prefix, include_inactive=False)


def _is_active_flux_row(row: dict[str, str] | None) -> bool:
    return bool(row and (row["state"] in PENDING_STATES or row["state"] in RUNNING_STATES))


def _active_counts_by_queue(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        if _is_active_flux_row(row):
            counts[row["queue"]] = counts.get(row["queue"], 0) + 1
    return counts


def _recent_submits(state: dict[str, Any], queue: str, *, now: float) -> int:
    cutoff = now - 3600.0
    return sum(
        1
        for item in state.get("submission_history", [])
        if item.get("queue") == queue and float(item.get("submitted_at", 0.0)) >= cutoff
    )


def _batch_name(spec: CampaignSpec, batch_id: str, *, run_id: str | None = None) -> str:
    if run_id:
        return f"{spec.submission.job_name_prefix}:{spec.campaign.name}:{run_id}:{batch_id}"
    return f"{spec.submission.job_name_prefix}:{spec.campaign.name}:{batch_id}"


def _state_run_id(state: dict[str, Any]) -> str | None:
    run_id = str(state.get("run_id") or "").strip()
    return run_id or None


def _ensure_run_id(root: Path, state: dict[str, Any]) -> str:
    run_id = _state_run_id(state)
    if run_id is None:
        run_id = _new_run_id(root)
        state["run_id"] = run_id
        state["updated_at"] = utcnow_iso()
        write_json(state_path(root), state)
    return run_id


def _batch_name_for_state(spec: CampaignSpec, batch_id: str, state: dict[str, Any]) -> str:
    return _batch_name(spec, batch_id, run_id=_state_run_id(state))


def _batch_prefix_for_state(spec: CampaignSpec, state: dict[str, Any]) -> str:
    run_id = _state_run_id(state)
    if run_id:
        return f"{spec.submission.job_name_prefix}:{spec.campaign.name}:{run_id}:"
    return f"{spec.submission.job_name_prefix}:{spec.campaign.name}:"


def _shell_quote(value: str | os.PathLike[str]) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


def _default_workspace_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "container-installs").exists() or (parent / "flux-fiction-dev.tar").exists():
            return parent
    return current.parents[4]


def _worker_script_path(root: Path) -> Path:
    return root / "worker.sh"


def ensure_worker_script(root: Path, spec: CampaignSpec | None = None) -> Path:
    source_root = Path(__file__).resolve().parents[2]
    repo_root = Path(__file__).resolve().parents[3]
    campaign = getattr(spec, "campaign", None)
    default_worker_runtime = str(
        getattr(campaign, "worker_runtime", "container") or "container"
    )
    default_flux_launch = "1" if bool(getattr(campaign, "flux_launch", False)) else ""
    default_flux_launch_cores = (
        str(getattr(campaign, "flux_launch_cores"))
        if getattr(campaign, "flux_launch_cores", None)
        else ""
    )
    default_spack_fluxion_prefix = (
        getattr(campaign, "spack_fluxion_prefix", None) or DEFAULT_SPACK_FLUXION_PREFIX
    )
    default_spack_faketime_prefix = (
        getattr(campaign, "spack_faketime_prefix", None) or DEFAULT_SPACK_FAKETIME_PREFIX
    )
    default_spack_gcc_runtime = (
        getattr(campaign, "spack_gcc_runtime", None) or DEFAULT_SPACK_GCC_RUNTIME
    )
    default_spack_view = getattr(campaign, "spack_view", None) or DEFAULT_SPACK_VIEW
    default_spack_ninja_prefix = (
        getattr(campaign, "spack_ninja_prefix", None) or DEFAULT_SPACK_NINJA_PREFIX
    )
    default_meson_pythonpath = (
        getattr(campaign, "meson_pythonpath", None) or DEFAULT_MESON_PYTHONPATH
    )
    default_host_jobtap_cc = getattr(campaign, "host_jobtap_cc", None) or "gcc"
    default_host_tmpdir = getattr(campaign, "host_tmpdir", None) or ""
    worker_env = dict(getattr(campaign, "worker_env", None) or {})

    def _runtime_setting(name: str, default: str | os.PathLike[str]) -> str:
        """Resolve a container-runtime setting: environment, then spec, then default.

        These are consumed by worker.sh itself rather than exported to the
        simulation, so they are baked in as defaults here. Taking them from
        `worker_env` lets a campaign that needs a particular image -- one with
        dftracer, say -- say so in its spec instead of relying on whatever
        shell launched it.
        """
        return os.environ.get(name) or worker_env.get(name) or str(default)

    workspace_root = Path(
        _runtime_setting("FLUX_FICTION_WORKSPACE_ROOT", _default_workspace_root())
    ).resolve()
    container_installs = Path(
        _runtime_setting(
            "FLUX_FICTION_CONTAINER_INSTALLS", workspace_root / "container-installs"
        )
    ).resolve()
    image_tar = Path(
        _runtime_setting(
            "FLUX_FICTION_CONTAINER_IMAGE_TAR", workspace_root / "flux-fiction-dev.tar"
        )
    ).resolve()
    image = _runtime_setting("FLUX_FICTION_CONTAINER_IMAGE", DEFAULT_CONTAINER_IMAGE)
    container_pythonpath = _runtime_setting("FLUX_FICTION_CONTAINER_PYTHONPATH", source_root)
    script = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            "if [[ $# -ne 2 ]]; then",
            "  echo \"usage: worker.sh <campaign-root> <batch-id>\" >&2",
            "  exit 64",
            "fi",
            "",
            "CAMPAIGN_ROOT=\"$(cd \"$1\" && pwd)\"",
            "BATCH_ID=\"$2\"",
            "# Stamp job start BEFORE the container image load so the",
            "# walltime budget handed to the simulations counts that time.",
            "FLUX_FICTION_WORKER_EPOCH=\"${FLUX_FICTION_WORKER_EPOCH:-$(date +%s)}\"",
            f"WORKSPACE_ROOT=\"${{FLUX_FICTION_WORKSPACE_ROOT:-{workspace_root}}}\"",
            f"CONTAINER_INSTALLS=\"${{FLUX_FICTION_CONTAINER_INSTALLS:-{container_installs}}}\"",
            f"IMAGE=\"${{FLUX_FICTION_CONTAINER_IMAGE:-{image}}}\"",
            f"IMAGE_TAR=\"${{FLUX_FICTION_CONTAINER_IMAGE_TAR:-{image_tar}}}\"",
            f"CONTAINER_PYTHONPATH=\"${{FLUX_FICTION_CONTAINER_PYTHONPATH:-{container_pythonpath}}}\"",
            f"SOURCE_ROOT=\"${{FLUX_FICTION_SOURCE_ROOT:-{source_root}}}\"",
            f"REPO_ROOT=\"${{FLUX_FICTION_REPO_ROOT:-{repo_root}}}\"",
            f"DEFAULT_WORKER_RUNTIME={json.dumps(default_worker_runtime)}",
            f"DEFAULT_FLUX_LAUNCH={json.dumps(default_flux_launch)}",
            f"DEFAULT_FLUX_LAUNCH_CORES={json.dumps(default_flux_launch_cores)}",
            f"SPACK_FLUXION_PREFIX=\"${{SPACK_FLUXION_PREFIX:-{default_spack_fluxion_prefix}}}\"",
            f"SPACK_FAKETIME_PREFIX=\"${{SPACK_FAKETIME_PREFIX:-{default_spack_faketime_prefix}}}\"",
            f"SPACK_GCC_RUNTIME=\"${{SPACK_GCC_RUNTIME:-{default_spack_gcc_runtime}}}\"",
            f"SPACK_VIEW=\"${{SPACK_VIEW:-{default_spack_view}}}\"",
            f"SPACK_NINJA_PREFIX=\"${{SPACK_NINJA_PREFIX:-{default_spack_ninja_prefix}}}\"",
            f"MESON_PYTHONPATH=\"${{MESON_PYTHONPATH:-{default_meson_pythonpath}}}\"",
            f"HOST_JOBTAP_CC=\"${{HOST_JOBTAP_CC:-{default_host_jobtap_cc}}}\"",
            f"DEFAULT_HOST_TMPDIR={json.dumps(default_host_tmpdir)}",
            "WORKER_RUNTIME=\"${FLUX_FICTION_WORKER_RUNTIME:-${DEFAULT_WORKER_RUNTIME}}\"",
            "if [[ \"${WORKER_RUNTIME}\" == \"podman\" ]]; then WORKER_RUNTIME=container; fi",
            "",
            "# Campaign-declared run environment (spec table `worker_env`). The",
            "# container runtime hands podman an explicit -e for each of these,",
            "# because a bare `podman run` inherits nothing from this shell.",
            "WORKER_ENV_NAMES=()",
            *[
                line
                # The container-runtime settings above are consumed by this
                # script, not by the simulation; they are already baked in as
                # defaults and do not belong in the container's environment.
                for name, value in sorted(worker_env.items())
                if name not in _CONTAINER_RUNTIME_SETTINGS
                for line in (
                    f"{name}=\"${{{name}:-{value}}}\"",
                    f"export {name}",
                    f"WORKER_ENV_NAMES+=({name})",
                )
            ],
            "# Names listed here are forwarded when set, whether they come from the",
            "# spec, the submitting shell, or the batch job's environment.",
            "for _name in FAKETIME_LIB FLUX_FICTION_JOBTAP_SO FLUX_FICTION_NO_BROKER_LOG_FILE \\",
            "             FLUX_FICTION_FAKETIME_MODE FLUX_FICTION_FINALIZE_RESERVE_SECONDS \\",
            "             FLUX_FICTION_FLUXION_RESOURCE_MODULE FLUX_FICTION_FLUXION_FEASIBILITY_MODULE \\",
            "             FLUX_FICTION_FLUXION_QMANAGER_MODULE NO_FAKE_STAT FLUX_CONF_DIR \\",
            "             ${FLUX_FICTION_WORKER_EXTRA_ENV:-}; do",
            # `test && append` as the loop body would make a false test on the
            # FINAL iteration the loop's exit status, which `set -e` treats as
            # a fatal error. Use if/fi so the body always succeeds.
            "  if [[ -n \"${!_name:-}\" ]]; then WORKER_ENV_NAMES+=(\"${_name}\"); fi",
            "done",
            "# DFTracer is configured through a whole family of variables; forward",
            "# every one that is set rather than tracking them individually.",
            "while IFS= read -r _name; do",
            "  if [[ -n \"${_name}\" ]]; then WORKER_ENV_NAMES+=(\"${_name}\"); fi",
            "done < <(compgen -v | grep '^DFTRACER_' || true)",
            "",
            "container_env_args() {",
            "  local seen=\" \" name",
            "  CONTAINER_ENV_ARGS=()",
            "  for name in ${WORKER_ENV_NAMES[@]+\"${WORKER_ENV_NAMES[@]}\"}; do",
            "    [[ \"${seen}\" == *\" ${name} \"* ]] && continue",
            "    seen+=\"${name} \"",
            "    [[ -n \"${!name:-}\" ]] || continue",
            "    CONTAINER_ENV_ARGS+=(-e \"${name}=${!name}\")",
            "  done",
            "  return 0",
            "}",
            "if [[ -z \"${FLUX_FICTION_FLUX_LAUNCH+x}\" && -n \"${DEFAULT_FLUX_LAUNCH}\" ]]; then",
            "  export FLUX_FICTION_FLUX_LAUNCH=\"${DEFAULT_FLUX_LAUNCH}\"",
            "fi",
            "if [[ -z \"${FLUX_FICTION_FLUX_LAUNCH_CORES+x}\" && -n \"${DEFAULT_FLUX_LAUNCH_CORES}\" ]]; then",
            "  export FLUX_FICTION_FLUX_LAUNCH_CORES=\"${DEFAULT_FLUX_LAUNCH_CORES}\"",
            "fi",
            "",
            "RESULT_PATH=\"${CAMPAIGN_ROOT}/batches/${BATCH_ID}/batch_result.json\"",
            "# A stale result from an earlier attempt would make this wrapper exit",
            "# immediately without running anything.",
            "rm -f \"${RESULT_PATH}\"",
            "",
            "result_return_code() {",
            "  python3 - \"$RESULT_PATH\" <<'PY'",
            "import json",
            "import sys",
            "try:",
            "    data = json.load(open(sys.argv[1], encoding='utf-8'))",
            "    print(int(data.get('return_code', 1)))",
            "except Exception:",
            "    print(1)",
            "PY",
            "}",
            "",
            "choose_scratch_root() {",
            "  if [[ -n \"${FLUX_FICTION_SCRATCH_HOST_ROOT:-}\" ]]; then",
            "    printf '%s\\n' \"${FLUX_FICTION_SCRATCH_HOST_ROOT}\"",
            "  elif [[ -d /l/ssd && -w /l/ssd ]]; then",
            "    printf '%s\\n' /l/ssd",
            "  else",
            "    printf '%s\\n' /var/tmp",
            "  fi",
            "}",
            "",
            "write_spack_modprobe_overlay() {",
            "  local overlay=\"$1\"",
            "  mkdir -p \"${overlay}/modprobe.d\"",
            "  cat > \"${overlay}/modprobe.d/zz-spack-fluxion.toml\" <<EOF",
            "[[modules]]",
            "name = \"sched-fluxion-resource\"",
            "module = \"${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-resource.so\"",
            "args = [\"load-allowlist=node,core,gpu\"]",
            "ranks = \"0\"",
            "after = [\"resource\"]",
            "requires = [\"resource\"]",
            "priority = 900",
            "",
            "[[modules]]",
            "name = \"sched-fluxion-feasibility\"",
            "module = \"${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-feasibility.so\"",
            "args = [\"load-allowlist=node,core,gpu\"]",
            "provides = [\"feasibility\"]",
            "ranks = \"0\"",
            "after = [\"sched-fluxion-resource\"]",
            "requires = [\"sched-fluxion-resource\"]",
            "needs = [\"sched-fluxion-resource\"]",
            "priority = 900",
            "",
            "[[modules]]",
            "name = \"sched-fluxion-qmanager\"",
            "module = \"${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-qmanager.so\"",
            "provides = [\"sched\"]",
            "ranks = \"0\"",
            "after = [\"job-manager\", \"sched-fluxion-resource\"]",
            "requires = [\"sched-fluxion-resource\", \"job-manager\"]",
            "needs = [\"sched-fluxion-resource\"]",
            "priority = 900",
            "EOF",
            "}",
            "",
            "run_container_worker() {",
            "command -v podman >/dev/null 2>&1 || { echo \"podman is required for ensemble workers\" >&2; exit 127; }",
            "",
            "# Rootless podman needs a runtime dir. Under a bare sbatch there is no",
            "# systemd user session, so /run/user/$UID does not exist and runc falls",
            "# back to /run/runc, which it cannot create (OCI permission denied).",
            "# Flux/srun sessions provide one, hence this only bites the Slurm path.",
            "# NB: XDG_RUNTIME_DIR is often exported but pointing at a directory that",
            "# does not exist, so test the path itself rather than whether it is set.",
            "if [[ ! -d \"${XDG_RUNTIME_DIR:-/nonexistent}\" ]]; then",
            "  export XDG_RUNTIME_DIR=\"/tmp/podman-run-$(id -u)\"",
            "  mkdir -p \"${XDG_RUNTIME_DIR}\" || true",
            "  chmod 700 \"${XDG_RUNTIME_DIR}\" 2>/dev/null || true",
            "  echo \"Runtime dir missing; using XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR}\"",
            "fi",
            "[[ -d \"${WORKSPACE_ROOT}\" ]] || { echo \"workspace root not found: ${WORKSPACE_ROOT}\" >&2; exit 4; }",
            "[[ -d \"${CONTAINER_INSTALLS}\" ]] || { echo \"container installs not found: ${CONTAINER_INSTALLS}\" >&2; exit 4; }",
            "",
            "# Rootless podman spawns a long-lived pause daemon (catatonit) on first use.",
            "# On nodes without a user systemd session it stays in this job's cgroup and",
            "# inherits its stdio, which keeps the Flux job RUNNING long after this",
            "# script exits. Spawn it up front with stdio detached, and kill it (plus",
            "# other podman helpers) in cleanup; the node is exclusively ours.",
            "podman info >/dev/null 2>&1 </dev/null || true",
            "",
            "if ! podman image exists \"${IMAGE}\" </dev/null; then",
            "  [[ -f \"${IMAGE_TAR}\" ]] || { echo \"container image is missing and tar was not found: ${IMAGE_TAR}\" >&2; exit 3; }",
            "  podman load -i \"${IMAGE_TAR}\" </dev/null",
            "fi",
            "",
            "# Pick a REAL node-local disk for the broker rundir/KVS. On these",
            "# compute nodes /var/tmp is tmpfs, i.e. RAM -- a 20k-job run puts",
            "# GBs of content.sqlite there and it counts against node memory.",
            "# /l/ssd is node-local NVMe; fall back to /var/tmp where absent.",
            "SCRATCH_ROOT=\"$(choose_scratch_root)\"",
            "echo \"Scratch root (broker rundir + KVS): ${SCRATCH_ROOT}\"",
            "SCRATCH_HOST=\"$(mktemp -d \"${SCRATCH_ROOT}/ffensemble.XXXXXX\")\"",
            "CONTAINER_NAME=\"ffe-${BATCH_ID}-$(date +%s)-$$\"",
            "WAIT_PATH=\"${SCRATCH_HOST}/container.exit\"",
            "WAIT_ERR_PATH=\"${SCRATCH_HOST}/container.wait.err\"",
            "cleanup() {",
            "  for pid in \"${LOGS_PID:-}\" \"${WAIT_PID:-}\"; do",
            "    [[ -n \"${pid}\" ]] && kill \"${pid}\" 2>/dev/null || true",
            "  done",
            "  if [[ -n \"${CONTAINER_ID:-}\" ]]; then",
            "    podman rm -f -t 5 \"${CONTAINER_ID}\" >/dev/null 2>&1 </dev/null || true",
            "  fi",
            "  for pid in \"${LOGS_PID:-}\" \"${WAIT_PID:-}\"; do",
            "    [[ -n \"${pid}\" ]] && kill -9 \"${pid}\" 2>/dev/null || true",
            "  done",
            "  wait 2>/dev/null || true",
            "  pkill -u \"$(id -un)\" -x catatonit 2>/dev/null || true",
            "  pkill -u \"$(id -un)\" -x conmon 2>/dev/null || true",
            "  pkill -u \"$(id -un)\" -f fuse-overlayfs 2>/dev/null || true",
            "  rm -rf \"${SCRATCH_HOST}\" 2>/dev/null || true",
            "}",
            "trap cleanup EXIT",
            "",
            "container_env_args",
            "if ((${#CONTAINER_ENV_ARGS[@]})); then",
            "  echo \"Forwarding to container: ${CONTAINER_ENV_ARGS[*]}\"",
            "fi",
            "",
            "CONTAINER_ID=\"$(podman run -d --rm --name \"${CONTAINER_NAME}\" --pull=never \\",
            "  ${CONTAINER_ENV_ARGS[@]+\"${CONTAINER_ENV_ARGS[@]}\"} \\",
            "  -e MPLBACKEND=Agg \\",
            "  -e PYTHONPATH=\"${CONTAINER_PYTHONPATH}\" \\",
            "  -e FLUX_FICTION_FAKETIME_DIR=/dev/shm \\",
            "  -e FLUX_FICTION_FLUX_LAUNCH=\"${FLUX_FICTION_FLUX_LAUNCH:-}\" \\",
            "  -e FLUX_FICTION_FLUX_LAUNCH_CORES=\"${FLUX_FICTION_FLUX_LAUNCH_CORES:-}\" \\",
            "  -e FLUX_FICTION_SCRATCH=/scratch \\",
            # The child broker derives its rundir from TMPDIR, and the KVS
            # content backing store (content.sqlite) lives there. Without this
            # it lands on the container's overlay -- fuse-overlayfs at this
            # site -- so every KVS write round-trips through a userspace FUSE
            # daemon (~647 MB for a 20k-job run). /scratch is a direct bind
            # mount of node-local host storage: no overlay, no FUSE.
            "  -e TMPDIR=/scratch \\",
            "  -e FLUX_FICTION_WORKER_EPOCH=\"${FLUX_FICTION_WORKER_EPOCH}\" \\",
            "  --shm-size=1g \\",
            "  --stop-timeout=90 \\",
            "  -v \"${SCRATCH_HOST}:/scratch\" \\",
            "  -v \"${CONTAINER_INSTALLS}:/workspace/container-installs:ro\" \\",
            "  -v \"${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:rw\" \\",
            "  -v \"${CAMPAIGN_ROOT}:${CAMPAIGN_ROOT}:rw\" \\",
            "  \"${IMAGE}\" \\",
            "  bash -lc 'if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then source /usr/local/bin/flux-dev-env.sh; fi; exec python3 -m flux_fiction_ensemble worker \"$@\"' _ \"${CAMPAIGN_ROOT}\" \"${BATCH_ID}\" </dev/null)\"",
            "echo \"Started container ${CONTAINER_NAME} (${CONTAINER_ID})\"",
            "podman logs -f \"${CONTAINER_ID}\" &",
            "LOGS_PID=\"$!\"",
            "podman wait \"${CONTAINER_ID}\" >\"${WAIT_PATH}\" 2>\"${WAIT_ERR_PATH}\" &",
            "WAIT_PID=\"$!\"",
            "",
            "container_exit_code() {",
            "  tail -n 1 \"${WAIT_PATH}\" 2>/dev/null | tr -cd '0-9' || true",
            "}",
            "",
            "while true; do",
            "  if [[ -s \"${RESULT_PATH}\" ]]; then",
            "    sleep \"${FLUX_FICTION_WORKER_RESULT_GRACE_SECONDS:-5}\"",
            "    rc=\"$(result_return_code)\"",
            "    if kill -0 \"${WAIT_PID}\" 2>/dev/null; then",
            "      podman stop -t \"${FLUX_FICTION_WORKER_STOP_SECONDS:-30}\" \"${CONTAINER_ID}\" >/dev/null 2>&1 || true",
            "      wait \"${WAIT_PID}\" >/dev/null 2>&1 || true",
            "    fi",
            "    echo \"Observed ${RESULT_PATH}; exiting worker wrapper rc=${rc}\"",
            "    exit \"${rc}\"",
            "  fi",
            "  if ! kill -0 \"${WAIT_PID}\" 2>/dev/null; then",
            "    wait \"${WAIT_PID}\" >/dev/null 2>&1 || true",
            "    if [[ -s \"${RESULT_PATH}\" ]]; then",
            "      rc=\"$(result_return_code)\"",
            "      echo \"Observed ${RESULT_PATH} after container exit; wrapper rc=${rc}\"",
            "      exit \"${rc}\"",
            "    fi",
            "    rc=\"$(container_exit_code)\"",
            "    [[ -n \"${rc}\" && \"${rc}\" != \"0\" ]] || rc=1",
            "    echo \"Container ${CONTAINER_NAME} (${CONTAINER_ID}) exited before writing ${RESULT_PATH}; wrapper rc=${rc}\" >&2",
            "    if [[ -s \"${WAIT_ERR_PATH}\" ]]; then cat \"${WAIT_ERR_PATH}\" >&2; fi",
            "    exit \"${rc}\"",
            "  fi",
            "  sleep 2",
            "done",
            "}",
            "",
            "run_spack_worker() {",
            "  command -v flux >/dev/null 2>&1 || { echo \"flux is required for Spack ensemble workers\" >&2; exit 127; }",
            "  command -v python3 >/dev/null 2>&1 || { echo \"python3 is required for Spack ensemble workers\" >&2; exit 127; }",
            "  [[ -d \"${SOURCE_ROOT}\" ]] || { echo \"source root not found: ${SOURCE_ROOT}\" >&2; exit 4; }",
            "  [[ -d \"${REPO_ROOT}\" ]] || { echo \"repo root not found: ${REPO_ROOT}\" >&2; exit 4; }",
            "  [[ -d \"${SPACK_FLUXION_PREFIX}\" ]] || { echo \"Spack Fluxion prefix not found: ${SPACK_FLUXION_PREFIX}\" >&2; exit 4; }",
            "  [[ -d \"${SPACK_FAKETIME_PREFIX}\" ]] || { echo \"Spack faketime prefix not found: ${SPACK_FAKETIME_PREFIX}\" >&2; exit 4; }",
            "  [[ -d \"${SPACK_GCC_RUNTIME}\" ]] || { echo \"Spack GCC runtime prefix not found: ${SPACK_GCC_RUNTIME}\" >&2; exit 4; }",
            "  [[ -d \"${SPACK_VIEW}\" ]] || { echo \"Spack view not found: ${SPACK_VIEW}\" >&2; exit 4; }",
            "  [[ -d \"${SPACK_NINJA_PREFIX}\" ]] || { echo \"Spack ninja prefix not found: ${SPACK_NINJA_PREFIX}\" >&2; exit 4; }",
            "",
            "  SCRATCH_ROOT=\"$(choose_scratch_root)\"",
            "  echo \"Scratch root (host Spack runtime): ${SCRATCH_ROOT}\"",
            "  SCRATCH_HOST=\"$(mktemp -d \"${SCRATCH_ROOT}/ffensemble-spack.XXXXXX\")\"",
            "  cleanup() {",
            "    pkill -9 -f flux-broker 2>/dev/null || true",
            "    pkill -9 -f flux-shell 2>/dev/null || true",
            "    pkill -9 -f flux-imp 2>/dev/null || true",
            "    rm -rf \"${SCRATCH_HOST}\" 2>/dev/null || true",
            "  }",
            "  trap cleanup EXIT",
            "",
            "  export PYTHONPATH=\"/usr/lib64/flux/python3.6:${SOURCE_ROOT}:${PYTHONPATH:-}\"",
            "  export MPLBACKEND=Agg",
            "  export FLUX_FICTION_WORKSPACE_ROOT=\"${WORKSPACE_ROOT}\"",
            "  export FLUX_FICTION_FAKETIME_DIR=\"${FLUX_FICTION_FAKETIME_DIR:-/dev/shm}\"",
            "  if [[ -n \"${FLUX_FICTION_HOST_TMPDIR:-}\" ]]; then",
            "    export TMPDIR=\"${FLUX_FICTION_HOST_TMPDIR}\"",
            "  elif [[ -n \"${DEFAULT_HOST_TMPDIR}\" ]]; then",
            "    export TMPDIR=\"${DEFAULT_HOST_TMPDIR}\"",
            "  else",
            "    export TMPDIR=\"${SCRATCH_HOST}\"",
            "  fi",
            "  mkdir -p \"${TMPDIR}\"",
            "  export FLUX_PREFIX=\"${FLUX_PREFIX:-/nonexistent-flux-prefix}\"",
            "  export FAKETIME_LIB=\"${FAKETIME_LIB:-${SPACK_FAKETIME_PREFIX}/lib/faketime/libfaketimeMT.so.1}\"",
            "  [[ -f \"${FAKETIME_LIB}\" ]] || { echo \"faketime library not found: ${FAKETIME_LIB}\" >&2; exit 4; }",
            "",
            "  SPACK_MODPROBE_OVERLAY=\"${SCRATCH_HOST}/spack-modprobe\"",
            "  write_spack_modprobe_overlay \"${SPACK_MODPROBE_OVERLAY}\"",
            "  if [[ -n \"${FLUX_MODPROBE_PATH_APPEND:-}\" ]]; then",
            "    export FLUX_MODPROBE_PATH_APPEND=\"${SPACK_MODPROBE_OVERLAY}:${FLUX_MODPROBE_PATH_APPEND}\"",
            "  else",
            "    export FLUX_MODPROBE_PATH_APPEND=\"${SPACK_MODPROBE_OVERLAY}\"",
            "  fi",
            "  export FLUX_FICTION_FLUXION_RESOURCE_MODULE=\"${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-resource.so\"",
            "  export FLUX_FICTION_FLUXION_FEASIBILITY_MODULE=\"${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-feasibility.so\"",
            "  export FLUX_FICTION_FLUXION_QMANAGER_MODULE=\"${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-qmanager.so\"",
            "  BASE_LD_LIBRARY_PATH=\"${LD_LIBRARY_PATH:-}\"",
            "  export LD_LIBRARY_PATH=\"${SPACK_GCC_RUNTIME}/lib:${SPACK_FLUXION_PREFIX}/lib64:${SPACK_FLUXION_PREFIX}/lib:${BASE_LD_LIBRARY_PATH}\"",
            "",
            "  if [[ -z \"${FLUX_FICTION_JOBTAP_SO:-}\" ]]; then",
            "    BUILD_DIR=\"${SCRATCH_HOST}/build-host-jobtap\"",
            "    INSTALL_DIR=\"${SCRATCH_HOST}/install-host-jobtap\"",
            "    rm -rf \"${BUILD_DIR}\" \"${INSTALL_DIR}\"",
            "    mkdir -p \"${BUILD_DIR}\" \"${INSTALL_DIR}\"",
            "    env \\",
            "      CC=\"${HOST_JOBTAP_CC}\" \\",
            "      PYTHONPATH=\"${MESON_PYTHONPATH}:${PYTHONPATH:-}\" \\",
            "      PATH=\"${SPACK_NINJA_PREFIX}/bin:${PATH}\" \\",
            "      python3 -m mesonbuild.mesonmain setup \"${BUILD_DIR}\" \"${REPO_ROOT}\" \\",
            "        -Duse_system_flux=true \\",
            "        -Dflux_prefix=/usr \\",
            "        --prefix \"${INSTALL_DIR}\"",
            "    env \\",
            "      CC=\"${HOST_JOBTAP_CC}\" \\",
            "      PYTHONPATH=\"${MESON_PYTHONPATH}:${PYTHONPATH:-}\" \\",
            "      PATH=\"${SPACK_NINJA_PREFIX}/bin:${PATH}\" \\",
            "      python3 -m mesonbuild.mesonmain compile -C \"${BUILD_DIR}\"",
            "    export FLUX_FICTION_JOBTAP_SO=\"${BUILD_DIR}/src/emu-jobtap.so\"",
            "  fi",
            "  [[ -f \"${FLUX_FICTION_JOBTAP_SO}\" ]] || { echo \"jobtap plugin not found: ${FLUX_FICTION_JOBTAP_SO}\" >&2; exit 4; }",
            "",
            "  echo \"Worker runtime: spack\"",
            "  echo \"Spack Fluxion: ${SPACK_FLUXION_PREFIX}\"",
            "  echo \"Flux pinning: ${FLUX_FICTION_FLUX_LAUNCH:-0} cores=${FLUX_FICTION_FLUX_LAUNCH_CORES:-auto}\"",
            "  echo \"TMPDIR: ${TMPDIR}\"",
            "  # Host workers inherit the system Flux job's FLUX_URI. Clear it so",
            "  # run_ff_parallel creates a node-local flux start instance for pinning",
            "  # instead of submitting the 16 replicas back to the system scheduler.",
            "  unset FLUX_URI FLUX_JOB_ID FLUX_KVS_NAMESPACE FLUX_INSTANCE_LEVEL",
            "  python3 -m flux_fiction_ensemble worker \"${CAMPAIGN_ROOT}\" \"${BATCH_ID}\"",
            "}",
            "",
            "case \"${WORKER_RUNTIME}\" in",
            "  container)",
            "    run_container_worker",
            "    ;;",
            "  spack)",
            "    run_spack_worker",
            "    ;;",
            "  *)",
            "    echo \"unknown worker runtime: ${WORKER_RUNTIME} (expected container or spack)\" >&2",
            "    exit 64",
            "    ;;",
            "esac",
            "",
        ]
    )
    path = _worker_script_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(script, encoding="utf-8")
    path.chmod(0o755)
    return path


def _recover_live_flux_submissions(
    root: Path,
    spec: CampaignSpec,
    batches: list[dict[str, Any]],
    submitted: dict[str, Any],
    rows_by_batch: dict[str, dict[str, str]],
    *,
    now: float,
) -> int:
    recovered = 0
    for batch in batches:
        batch_id = batch["batch_id"]
        result = read_json(root / "batches" / batch_id / "batch_result.json")
        if result.get("state") in TERMINAL_STATES:
            continue
        row = rows_by_batch.get(batch_id)
        if not _is_active_flux_row(row):
            continue
        current = submitted.get(batch_id)
        if current and current.get("fluxid") == row["id"]:
            continue
        submitted[batch_id] = {
            "fluxid": row["id"],
            "queue": row["queue"],
            "submitted_at": float(current.get("submitted_at", now)) if current else now,
            "recovered_at": utcnow_iso(),
        }
        recovered += 1
    return recovered


def _rows_by_batch_id(
    spec: CampaignSpec,
    batches: list[dict[str, Any]],
    submitted: dict[str, Any],
    rows: list[dict[str, str]],
    state: dict[str, Any],
) -> dict[str, dict[str, str]]:
    # Batches are tracked by the fluxid recorded at submit time. Matching by
    # job name is only a crash-window recovery aid (submit succeeded but
    # state.json was never written), so it is restricted to ACTIVE jobs whose
    # name carries this campaign generation's run_id; inactive jobs from
    # earlier launcher generations must never be adopted.
    rows_by_id = {row["id"]: row for row in rows}
    active_by_name: dict[str, dict[str, str]] = {}
    for row in rows:
        if _is_active_flux_row(row):
            active_by_name.setdefault(row["name"], row)

    out: dict[str, dict[str, str]] = {}
    for batch in batches:
        batch_id = batch["batch_id"]
        current = submitted.get(batch_id) or {}
        fluxid = str(current.get("fluxid") or "")
        if fluxid:
            if fluxid in rows_by_id:
                out[batch_id] = rows_by_id[fluxid]
            continue
        row = active_by_name.get(_batch_name_for_state(spec, batch_id, state))
        if row is not None:
            out[batch_id] = row
    return out


def _mark_terminal_or_lost_submissions(
    root: Path,
    spec: CampaignSpec,
    batches: list[dict[str, Any]],
    submitted: dict[str, Any],
    rows_by_batch: dict[str, dict[str, str]],
    *,
    now: float,
) -> int:
    marked = 0
    for batch in batches:
        batch_id = batch["batch_id"]
        current = submitted.get(batch_id)
        if not current:
            continue
        result_path = root / "batches" / batch_id / "batch_result.json"
        if read_json(result_path).get("state") in TERMINAL_STATES:
            continue

        row = rows_by_batch.get(batch_id)
        if _is_active_flux_row(row):
            continue

        failure_reason: str | None = None
        fluxid = str(current.get("fluxid") or "")
        queue = str(current.get("queue") or "")
        if row is not None:
            fluxid = row["id"]
            queue = row["queue"]
            failure_reason = (
                f"Flux job {row['id']} ended with state={row['state']} result={row.get('result', '-')}; "
                f"worker did not write batch_result.json. See {root / 'batches' / batch_id / 'batch.log'}"
            )
        else:
            submitted_at = float(current.get("submitted_at", now))
            if now - submitted_at < MISSING_FLUX_ROW_GRACE_SECONDS:
                continue
            failure_reason = (
                f"Flux job {fluxid or '<unknown>'} is no longer visible to flux jobs -a and "
                "worker did not write batch_result.json."
            )

        payload = {
            "batch_id": batch_id,
            "state": "failed",
            "return_code": 1,
            "finished_at": utcnow_iso(),
            "failure_reason": failure_reason,
            "task_count": int(batch.get("task_count") or 0),
            "tasks": list(batch.get("task_ids") or []),
            "fluxid": fluxid,
            "queue": queue,
            "flux_state": row.get("state") if row else None,
            "flux_result": row.get("result") if row else None,
        }
        write_json(result_path, payload)
        write_json(root / "batches" / batch_id / "batch_status.json", payload)
        marked += 1
    return marked


def _release_terminal_flux_allocations(
    root: Path,
    batches: list[dict[str, Any]],
    rows_by_batch: dict[str, dict[str, str]],
    state: dict[str, Any],
    spec: CampaignSpec,
) -> tuple[int, bool]:
    jobids: list[str] = []
    release_attempts = state.setdefault("release_attempts", {})
    if not isinstance(release_attempts, dict):
        release_attempts = {}
        state["release_attempts"] = release_attempts
    release_marks: list[tuple[str, str]] = []
    now = time.time()
    for batch in batches:
        batch_id = batch["batch_id"]
        result_path = root / "batches" / batch_id / "batch_result.json"
        result = read_json(result_path)
        if result.get("state") not in TERMINAL_STATES:
            continue
        try:
            if now - result_path.stat().st_mtime < TERMINAL_RELEASE_GRACE_SECONDS:
                continue
        except OSError:
            continue
        row = rows_by_batch.get(batch_id)
        if _is_active_flux_row(row):
            key = f"{batch_id}:{row['id']}"
            previous = release_attempts.get(key)
            if isinstance(previous, dict):
                attempted_at = float(previous.get("attempted_at_epoch") or 0.0)
                if now - attempted_at < TERMINAL_RELEASE_GRACE_SECONDS:
                    continue
            jobids.append(row["id"])
            release_marks.append((key, row["id"]))
    if not jobids:
        return 0, False
    canceled = cancel_jobs(
        spec,
        jobids,
        reason="Flux Fiction ensemble batch_result.json persisted",
    )
    for key, jobid in release_marks:
        release_attempts[key] = {
            "fluxid": jobid,
            "attempted_at": utcnow_iso(),
            "attempted_at_epoch": now,
        }
    return canceled, True


def _submit_batch(root: Path, spec: CampaignSpec, batch_id: str, queue: str, *, dry_run: bool) -> str | None:
    batch_root = root / "batches" / batch_id
    batch_root.mkdir(parents=True, exist_ok=True)
    if not dry_run:
        # A batch is only submitted when it is not terminal, so any files left
        # by an earlier attempt are stale; the worker treats an existing
        # batch_result.json as completion, so it must not see one at start.
        for stale in ("batch_result.json", "batch_status.json"):
            (batch_root / stale).unlink(missing_ok=True)
    worker_script = ensure_worker_script(root, spec)
    env: dict[str, str] = {}
    runtime = str(getattr(spec.campaign, "worker_runtime", "container") or "container")
    if runtime == "podman":
        runtime = "container"
    env["FLUX_FICTION_WORKER_RUNTIME"] = runtime
    if getattr(spec.campaign, "flux_launch", False):
        env["FLUX_FICTION_FLUX_LAUNCH"] = "1"
    if getattr(spec.campaign, "flux_launch_cores", None):
        env["FLUX_FICTION_FLUX_LAUNCH_CORES"] = str(spec.campaign.flux_launch_cores)
    campaign_env = {
        "SPACK_FLUXION_PREFIX": getattr(spec.campaign, "spack_fluxion_prefix", None),
        "SPACK_FAKETIME_PREFIX": getattr(spec.campaign, "spack_faketime_prefix", None),
        "SPACK_GCC_RUNTIME": getattr(spec.campaign, "spack_gcc_runtime", None),
        "SPACK_VIEW": getattr(spec.campaign, "spack_view", None),
        "SPACK_NINJA_PREFIX": getattr(spec.campaign, "spack_ninja_prefix", None),
        "MESON_PYTHONPATH": getattr(spec.campaign, "meson_pythonpath", None),
        "HOST_JOBTAP_CC": getattr(spec.campaign, "host_jobtap_cc", None),
        "FLUX_FICTION_HOST_TMPDIR": getattr(spec.campaign, "host_tmpdir", None),
    }
    for name, value in campaign_env.items():
        if value not in (None, ""):
            env[name] = str(value)
    for name in (
        "FLUX_FICTION_WORKSPACE_ROOT",
        "FLUX_FICTION_CONTAINER_INSTALLS",
        "FLUX_FICTION_CONTAINER_IMAGE",
        "FLUX_FICTION_CONTAINER_IMAGE_TAR",
        "FLUX_FICTION_CONTAINER_PYTHONPATH",
        "FLUX_FICTION_SOURCE_ROOT",
        "FLUX_FICTION_REPO_ROOT",
        "FLUX_FICTION_WORKER_RUNTIME",
        "FLUX_FICTION_FLUX_LAUNCH",
        "FLUX_FICTION_FLUX_LAUNCH_CORES",
        "FLUX_FICTION_SCRATCH_HOST_ROOT",
        "FLUX_FICTION_HOST_TMPDIR",
        "SPACK_FLUXION_PREFIX",
        "SPACK_FAKETIME_PREFIX",
        "SPACK_GCC_RUNTIME",
        "SPACK_VIEW",
        "SPACK_NINJA_PREFIX",
        "MESON_PYTHONPATH",
        "HOST_JOBTAP_CC",
        "FAKETIME_LIB",
        "FLUX_FICTION_JOBTAP_SO",
        "FLUX_FICTION_FAKETIME_MODE",
        "FLUX_FICTION_FLUXION_RESOURCE_MODULE",
        "FLUX_FICTION_FLUXION_FEASIBILITY_MODULE",
        "FLUX_FICTION_FLUXION_QMANAGER_MODULE",
        "FLUX_FICTION_WORKER_EXTRA_ENV",
        "NO_FAKE_STAT",
        "FLUX_CONF_DIR",
    ):
        if os.environ.get(name) and name not in env:
            env[name] = os.environ[name]
    # DFTracer's configuration is a variable family, not a fixed list.
    for name, value in os.environ.items():
        if name.startswith("DFTRACER_") and value and name not in env:
            env[name] = value
    if dry_run:
        job_name = _batch_name_for_state(spec, batch_id, read_json(state_path(root)) or {})
        if submission_backend(spec) == "slurm":
            print(
                "DRY-RUN submit via sbatch:",
                (
                    f"sbatch --parsable -N1 --exclusive --time={spec.campaign.walltime_minutes} "
                    f"--job-name={job_name} "
                    + (f"--partition={queue} " if queue else "")
                    + f"--output={batch_root / 'batch.log'} "
                    f"--wrap='bash {worker_script} {root} {batch_id}'"
                ),
            )
        else:
            print(
                "DRY-RUN submit via flux-python:",
                (
                    f"JobspecV1.from_command(['bash', {worker_script}, {root}, {batch_id}], "
                    f"name={job_name}, queue={queue}, "
                    f"walltime={spec.campaign.walltime_minutes}m, output={batch_root / 'batch.log'})"
                ),
            )
        return f"DRY-{batch_id}"
    try:
        job_name = _batch_name_for_state(spec, batch_id, read_json(state_path(root)) or {})
        submit = (
            _slurm_submit_batch
            if submission_backend(spec) == "slurm"
            else _flux_python_submit_batch
        )
        return submit(
            worker_script=worker_script,
            root=root,
            batch_id=batch_id,
            job_name=job_name,
            queue=queue,
            output=batch_root / "batch.log",
            walltime_minutes=spec.campaign.walltime_minutes,
            env=env,
        )
    except RuntimeError as exc:
        print(f"submit failed for {batch_id}: {exc}", file=sys.stderr)
        return None


def _batch_state(
    root: Path,
    batch_id: str,
    submitted: dict[str, Any],
    rows_by_batch: dict[str, dict[str, str]],
    *,
    now: float | None = None,
    flux_reconciled: bool = True,
) -> str:
    result = read_json(root / "batches" / batch_id / "batch_result.json")
    if result.get("state") in TERMINAL_STATES:
        return str(result["state"])
    row = rows_by_batch.get(batch_id)
    if row and row["state"] in PENDING_STATES:
        return "pending"
    if row and row["state"] in RUNNING_STATES:
        return "running"
    if batch_id not in submitted:
        return "queued"
    if now is None:
        now = time.time()
    submitted_at = float((submitted.get(batch_id) or {}).get("submitted_at", now))
    if now - submitted_at < MISSING_FLUX_ROW_GRACE_SECONDS:
        return "submitted"
    return "failed" if flux_reconciled else "submitted"


def launcher_tick(root: Path, spec: CampaignSpec, *, dry_run: bool = False) -> bool:
    root = root.expanduser().resolve()
    batches = read_jsonl(batches_path(root))
    state = read_json(state_path(root)) or {"submitted": {}, "submission_history": []}
    submitted = state.setdefault("submitted", {})
    state.setdefault("submission_history", [])
    if not dry_run:
        _ensure_run_id(root, state)
    prefix = _batch_prefix_for_state(spec, state)
    rows = [] if dry_run else query_job_rows(
        spec,
        prefix,
        [str(item.get("fluxid")) for item in submitted.values() if item.get("fluxid")],
        include_inactive=True,
    )
    active_rows = [row for row in rows if _is_active_flux_row(row)]
    rows_by_batch = _rows_by_batch_id(spec, batches, submitted, rows, state)
    active_by_queue = _active_counts_by_queue(active_rows)
    now = time.time()
    recovered = _recover_live_flux_submissions(
        root,
        spec,
        batches,
        submitted,
        rows_by_batch,
        now=now,
    )
    terminal_marked = 0 if dry_run else _mark_terminal_or_lost_submissions(
        root,
        spec,
        batches,
        submitted,
        rows_by_batch,
        now=now,
    )
    released = 0
    release_changed = False
    if not dry_run:
        released, release_changed = _release_terminal_flux_allocations(
            root,
            batches,
            rows_by_batch,
            state,
            spec,
        )

    states = {
        batch["batch_id"]: _batch_state(root, batch["batch_id"], submitted, rows_by_batch, now=now)
        for batch in batches
    }
    for batch_id, state_name in states.items():
        if state_name in TERMINAL_STATES:
            submitted.pop(batch_id, None)

    queued = [batch for batch in batches if states[batch["batch_id"]] == "queued"]
    submitted_this_tick = 0
    synthetic_rows: list[dict[str, str]] = []
    submit_failures: list[str] = []
    for batch in queued:
        chosen_queue = None
        for queue in spec.submission.queues:
            limit = spec.submission.queue_limits.get(queue)
            if limit and limit.max_active is not None and active_by_queue.get(queue, 0) >= limit.max_active:
                continue
            if limit and limit.max_submit_per_hour is not None:
                if _recent_submits(state, queue, now=now) >= limit.max_submit_per_hour:
                    continue
            chosen_queue = queue
            break
        if chosen_queue is None:
            break
        fluxid = _submit_batch(root, spec, batch["batch_id"], chosen_queue, dry_run=dry_run)
        if not fluxid:
            submit_failures.append(batch["batch_id"])
            continue
        submitted[batch["batch_id"]] = {
            "fluxid": fluxid,
            "queue": chosen_queue,
            "submitted_at": now,
        }
        state["submission_history"].append(
            {
                "batch_id": batch["batch_id"],
                "queue": chosen_queue,
                "submitted_at": now,
                "fluxid": fluxid,
            }
        )
        state["updated_at"] = utcnow_iso()
        if not dry_run:
            write_json(state_path(root), state)
        synthetic_rows.append(
            {
                "id": fluxid,
                "queue": chosen_queue,
                "state": "PENDING",
                "result": "-",
                "name": _batch_name_for_state(spec, batch["batch_id"], state),
            }
        )
        active_by_queue[chosen_queue] = active_by_queue.get(chosen_queue, 0) + 1
        submitted_this_tick += 1

    state["updated_at"] = utcnow_iso()
    if not dry_run:
        write_json(state_path(root), state)
    all_active_rows = active_rows + synthetic_rows
    status_payload = update_status(root, spec=spec, active_rows=all_active_rows)
    if not dry_run:
        append_progress_rows(root, status_payload)
    counts = status_payload.get("batch_counts", {})
    print(
        "[{}] batches queued={} pending={} running={} succeeded={} failed={} submitted={} flux_active={} sim_jobs={}/{}".format(
            time.strftime("%H:%M:%S"),
            counts.get("queued", 0),
            counts.get("pending", 0),
            counts.get("running", 0),
            counts.get("succeeded", 0),
            counts.get("failed", 0),
            submitted_this_tick,
            len(all_active_rows),
            status_payload.get("sim_jobs_completed", 0),
            status_payload.get("sim_jobs_total", 0),
        )
    )
    if recovered:
        print(f"Recovered {recovered} live Flux submission(s) from job names.")
    if terminal_marked:
        print(f"Marked {terminal_marked} submitted batch(es) failed from terminal/lost Flux jobs.")
    if released:
        print(f"Released {released} completed Flux allocation(s).")
    if submit_failures and submitted_this_tick == 0:
        raise RuntimeError(
            "Failed to submit queued batch(es): {}. Fix the submit error and resume from {}."
            .format(", ".join(submit_failures), state_path(root))
        )
    batches_drained = (
        counts.get("queued", 0) == 0
        and counts.get("submitted", 0) == 0
        and counts.get("pending", 0) == 0
        and counts.get("running", 0) == 0
    )
    if batches_drained and not dry_run:
        try:
            write_results_csv(root, spec=spec)
        except Exception as exc:  # never let reporting break the launcher
            print(f"Could not write results.csv: {exc!r}", file=sys.stderr)
    # Do not declare the campaign done while Flux still shows active jobs for
    # it; keep ticking so the release logic can cancel lingering allocations
    # and the reported state converges with the real queue.
    return batches_drained and (dry_run or not all_active_rows)


def _watchable_fluxids_from_status(root: Path) -> list[str]:
    payload = read_json(status_path(root))
    jobids: list[str] = []
    for row in payload.get("active_flux_jobs", []) or []:
        if isinstance(row, dict) and row.get("id"):
            jobids.append(str(row["id"]))
    state = read_json(state_path(root)) or {}
    for item in (state.get("submitted") or {}).values():
        if isinstance(item, dict) and item.get("fluxid"):
            jobids.append(str(item["fluxid"]))
    return list(dict.fromkeys(jobids))


def _wait_for_flux_reactor_event(root: Path, *, since: float, timeout_seconds: float) -> dict[str, Any]:
    jobids = _watchable_fluxids_from_status(root)
    if not jobids:
        return {"events": [], "errors": [], "timed_out": True, "watched": 0}
    return _flux_python_watch_job_events(
        jobids,
        since=since,
        timeout_seconds=timeout_seconds,
    )


def run_launcher(
    root: Path,
    spec: CampaignSpec,
    *,
    once: bool = False,
    dry_run: bool = False,
    use_reactor: bool = True,
) -> int:
    state = read_json(state_path(root)) or {}
    history = state.get("submission_history") or []
    if history:
        print(
            "Existing campaign state found at {} ({} prior submission(s), run_id={}); "
            "continuing where it left off. Use the 'reset' command first for a fresh start.".format(
                state_path(root), len(history), _state_run_id(state) or "<legacy>"
            )
        )
    backend = submission_backend(spec)
    print(f"Submission backend: {backend} (one single-node job per batch)")
    if backend == "slurm":
        # Slurm has no equivalent of Flux's event-watch reactor, so pace the
        # loop with the configured poll interval instead of blocking on events.
        use_reactor = False
    if once or dry_run or not use_reactor:
        while True:
            done = launcher_tick(root, spec, dry_run=dry_run)
            if once or dry_run or done:
                return 0
            time.sleep(max(1.0, spec.campaign.poll_interval_seconds))

    reactor_failed = False
    while True:
        watch_since = time.time()
        done = launcher_tick(root, spec, dry_run=dry_run)
        if done:
            return 0
        timeout = max(1.0, spec.campaign.poll_interval_seconds)
        if reactor_failed:
            time.sleep(timeout)
            continue
        try:
            event_result = _wait_for_flux_reactor_event(
                root,
                since=watch_since,
                timeout_seconds=timeout,
            )
        except RuntimeError as exc:
            reactor_failed = True
            print(
                f"Flux reactor watch failed; falling back to {timeout:.1f}s polling: {exc}",
                file=sys.stderr,
            )
            time.sleep(min(timeout, REACTOR_FALLBACK_POLL_SECONDS))
            continue
        events = event_result.get("events") or []
        errors = event_result.get("errors") or []
        if events:
            names = ", ".join(
                f"{event.get('id')}:{event.get('name')}" for event in events[:3]
            )
            extra = "" if len(events) <= 3 else f" (+{len(events) - 3} more)"
            print(f"Flux event wake: {names}{extra}")
        elif errors:
            print(
                "Flux reactor watch returned {} error(s); continuing with reconciliation.".format(
                    len(errors)
                ),
                file=sys.stderr,
            )
            time.sleep(min(timeout, REACTOR_FALLBACK_POLL_SECONDS))
        elif not event_result.get("watched"):
            # Nothing to watch (e.g. queued batches throttled by queue limits):
            # the watch returned immediately, so pace the loop ourselves.
            time.sleep(timeout)


def _summarize_batch_results(root: Path, batches: list[dict[str, Any]], spec: CampaignSpec, active_rows: list[dict[str, str]] | None) -> tuple[list[dict[str, Any]], dict[str, int]]:
    state = read_json(state_path(root)) or {"submitted": {}}
    rows_by_batch = _rows_by_batch_id(
        spec,
        batches,
        state.get("submitted", {}),
        active_rows or [],
        state,
    )
    counts: dict[str, int] = {
        "queued": 0,
        "submitted": 0,
        "pending": 0,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
    }
    records = []
    now = time.time()
    for batch in batches:
        batch_id = batch["batch_id"]
        result = read_json(root / "batches" / batch_id / "batch_result.json")
        state_name = _batch_state(
            root,
            batch_id,
            state.get("submitted", {}),
            rows_by_batch,
            now=now,
            flux_reconciled=active_rows is not None,
        )
        counts[state_name] = counts.get(state_name, 0) + 1
        records.append(
            {
                **batch,
                "state": state_name,
                "result": result,
                "submitted": state.get("submitted", {}).get(batch_id),
            }
        )
    return records, counts


def progress_csv_path(root: Path) -> Path:
    return root / "progress.csv"


def results_csv_path(root: Path) -> Path:
    return root / "results.csv"


PROGRESS_COLUMNS = [
    "wall_iso",
    "wall_epoch",
    "task_id",
    "batch_id",
    "jobs_completed",
    "jobs_submitted",
    "jobs_total",
    "status",
]


def append_progress_rows(root: Path, payload: dict[str, Any]) -> int:
    """Append one row per in-flight task to a campaign-level progress.csv.

    The per-sim status.json is rewritten in place every time-step, so the
    *history* of how fast each policy ran is otherwise lost. This is that
    history: the wall-clock throughput curve, recorded from the launcher's
    clock (the sims are libfaketime-preloaded, so their own clocks are
    simulated and unusable for wall-time measurement).

    Size is bounded by the poll interval: one short row per task per tick
    (~480 rows/hour for a 4-task campaign at 30s), so this stays in the
    kilobytes even for long campaigns.
    """
    rows = payload.get("task_progress") or []
    if not rows:
        return 0
    now = time.time()
    now_iso = utcnow_iso()
    path = progress_csv_path(root)
    write_header = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PROGRESS_COLUMNS)
        if write_header:
            writer.writeheader()
        for item in rows:
            writer.writerow(
                {
                    "wall_iso": now_iso,
                    "wall_epoch": f"{now:.3f}",
                    "task_id": item.get("task_id", ""),
                    "batch_id": item.get("batch_id", ""),
                    "jobs_completed": int(item.get("jobs_completed") or 0),
                    "jobs_submitted": int(item.get("jobs_submitted") or 0),
                    "jobs_total": int(item.get("jobs_total") or 0),
                    "status": item.get("status") or item.get("child_state") or "",
                }
            )
    return len(rows)


def _mtime_or_zero(path: Path) -> float:
    """Real filesystem mtime, or 0.0 if it cannot be read.

    Used to order requeue attempts. The kernel stamps this, so unlike anything
    the child writes it is not affected by the faked clock the sim runs under.
    """
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _collect_task_progress(
    root: Path,
    batches: list[dict[str, Any]],
    rows_by_batch: dict[str, dict[str, str]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Read each task's live per-sim status.json off the shared filesystem.

    The child run writes jobs_completed/jobs_total to
    ``batches/<b>/parallel/<ts>_manifest/runs/<n>_<task_id>/child/status.json``
    every sim time-step, atomically. Reading it here reports how far each
    policy got even when the batch was killed by walltime before writing
    batch_result.json -- this is the "how many completed before timeout" signal.
    """
    progress: dict[str, dict[str, Any]] = {}
    for batch in batches:
        batch_id = batch["batch_id"]
        runs_glob = (root / "batches" / batch_id / "parallel").glob(
            "*/runs/*/child/status.json"
        )
        for status_file in runs_glob:
            # runs/<ordinal>_<task_id>/child/status.json
            task_dir = status_file.parent.parent.name
            task_id = task_dir.split("_", 1)[1] if "_" in task_dir else task_dir
            data = read_json(status_file)
            if not data:
                continue
            existing = progress.get(task_id)
            # Prefer the most recent ATTEMPT if a task has more than one child
            # dir (a requeued batch). The child's own updated_at cannot be used
            # to order attempts: the child runs under libfaketime, so it stamps
            # simulated time (the trace epoch), while the terminal record for a
            # killed child is written by the parent runner in real time. A dead
            # 2026-07 attempt therefore always sorts above a live 2026-01 one
            # and the stale run wins -- which froze corona's reported progress
            # at the pre-requeue counts on 2026-07-27.
            #
            # Order on the manifest directory name instead (real wall clock at
            # launch, and a requeue always mints a later one), with the status
            # file's real mtime as tiebreak. Both come from outside the faked
            # clock.
            attempt_key = (
                status_file.parent.parent.parent.parent.name,
                _mtime_or_zero(status_file),
            )
            if existing and attempt_key <= existing.get("_attempt_key", ("", 0.0)):
                continue
            # A walltime kill freezes the child status.json at state="running",
            # so trust the batch-level terminal result for the reported status:
            # a TIMEOUT there means the count is "completed before timeout".
            child_state = data.get("state")
            status = child_state
            batch_result = read_json(root / "batches" / batch_id / "batch_result.json")
            batch_state = batch_result.get("state")
            if batch_state in TERMINAL_STATES:
                reason = str(batch_result.get("failure_reason") or "")
                flux_result = str(
                    (rows_by_batch or {}).get(batch_id, {}).get("result") or ""
                ).upper()
                # A negative return code means "killed by signal N"; for these
                # batches that is the walltime kill landing mid-run, not a
                # crash. Derived from return_code as well as the explicit flag
                # so results written before that flag existed still read right.
                signal_killed = bool(batch_result.get("terminated_by_signal")) or (
                    int(batch_result.get("return_code") or 0) < 0
                )
                timed_out = (
                    "TIMEOUT" in reason.upper()
                    or flux_result == "TIMEOUT"
                    or signal_killed
                )
                if batch_state == "failed" and timed_out:
                    status = "timeout"
                elif child_state not in TERMINAL_STATES:
                    status = batch_state
            # The child's own summary.json is written the moment the sim
            # finalizes -- including an early finalize -- whereas
            # parallel_summary.json needs the whole flux instance to tear down
            # first, and that teardown has to cancel every still-running
            # simulated job. Harvest the child summary so a truncated run still
            # reports its metrics.
            summary = read_json(status_file.parent / "summary.json")
            progress[task_id] = {
                "task_id": task_id,
                "batch_id": batch_id,
                "jobs_completed": int(data.get("jobs_completed") or 0),
                "jobs_submitted": int(data.get("jobs_submitted") or 0),
                "jobs_total": int(data.get("jobs_total") or 0),
                "child_state": child_state,
                "status": status,
                "updated_at": data.get("updated_at"),
                "_attempt_key": attempt_key,
                "makespan_seconds": summary.get("makespan_seconds"),
                "avg_queue_wait_seconds": summary.get("avg_queue_wait_seconds"),
                "max_queue_wait_seconds": summary.get("max_queue_wait_seconds"),
                "summary_jobs_completed": summary.get("jobs_completed"),
            }
    return progress


def update_status(root: Path, *, spec: CampaignSpec | None = None, active_rows: list[dict[str, str]] | None = None) -> dict[str, Any]:
    if spec is None:
        spec = load_campaign_snapshot(root)
    tasks = read_jsonl(tasks_path(root))
    batches = read_jsonl(batches_path(root))
    records, batch_counts = _summarize_batch_results(root, batches, spec, active_rows)
    succeeded_batches = batch_counts.get("succeeded", 0)
    failed_batches = batch_counts.get("failed", 0)
    completed_tasks = sum(
        int(record.get("task_count") or 0)
        for record in records
        if record.get("state") == "succeeded"
    )
    failed_tasks = sum(
        int(record.get("task_count") or 0)
        for record in records
        if record.get("state") == "failed"
    )
    active_jobs = [row for row in (active_rows or []) if _is_active_flux_row(row)]
    task_progress = _collect_task_progress(root, batches)
    sim_jobs_completed = sum(int(p.get("jobs_completed") or 0) for p in task_progress.values())
    sim_jobs_total = sum(int(p.get("jobs_total") or 0) for p in task_progress.values())
    payload = {
        "version": 1,
        "campaign": spec.campaign.name,
        "updated_at": utcnow_iso(),
        "root": str(root),
        "spec_path": spec.path,
        "total_tasks": len(tasks),
        "total_batches": len(batches),
        "task_counts": {
            "succeeded": completed_tasks,
            "failed": failed_tasks,
            "remaining": max(0, len(tasks) - completed_tasks - failed_tasks),
        },
        "sim_jobs_completed": sim_jobs_completed,
        "sim_jobs_total": sim_jobs_total,
        "task_progress": sorted(task_progress.values(), key=lambda p: p["task_id"]),
        "batch_counts": batch_counts,
        "active_flux_jobs": active_jobs,
        "queues": spec.submission.queues,
        "recent_failures": [
            {
                "batch_id": record["batch_id"],
                "failure_reason": record.get("result", {}).get("failure_reason"),
                "log": str(root / "batches" / record["batch_id"] / "batch.log"),
            }
            for record in records
            if record.get("state") == "failed"
        ][-10:],
        "complete": (
            succeeded_batches + failed_batches == len(batches) and not active_jobs
            if batches else False
        ),
    }
    write_json(status_path(root), payload)
    return payload


RESULTS_COLUMNS = [
    "task_id",
    "batch_id",
    "state",
    "queue_policy",
    "match_policy",
    "rabbit_job_percentage",
    "rabbit_ceiling_percentage",
    "rabbit_distribution",
    "rabbit_active",
    "shake_seed",
    "duplicate",
    "jobs_completed",
    "jobs_total",
    "makespan_seconds",
    "avg_queue_wait_seconds",
    "max_queue_wait_seconds",
    "wall_seconds",
    "startup_seconds",
    "sim_wall_seconds",
    "return_code",
    "run_root",
]


def _first_present(*values: Any) -> Any:
    """First non-empty value, so a truncated run still reports what it has."""
    for value in values:
        if value not in (None, ""):
            return value
    return ""


def write_results_csv(root: Path, *, spec: CampaignSpec | None = None) -> Path:
    """Join every task's grid factors to its measured metrics in one tidy CSV.

    Each replica writes its own summary deep under batches/<b>/parallel/...;
    this flattens the whole campaign into a single row-per-task table so a
    grid of hundreds of tasks can be analyzed without hand-joining.
    Falls back to the live status.json harvest for tasks that never wrote a
    summary (killed by walltime), so timed-out tasks still appear with their
    partial jobs_completed.
    """
    root = root.expanduser().resolve()
    if spec is None:
        spec = load_campaign_snapshot(root)
    tasks = {task["task_id"]: task for task in read_jsonl(tasks_path(root))}
    batches = read_jsonl(batches_path(root))

    # Measured metrics, keyed by task name, from each batch's parallel summary.
    measured: dict[str, dict[str, Any]] = {}
    for batch in batches:
        batch_id = batch["batch_id"]
        # A requeued batch has one manifest dir per attempt. Read them oldest
        # first so the newest attempt overwrites the earlier one, rather than
        # letting arbitrary glob order decide which attempt becomes the result.
        for summary_file in sorted(
            (root / "batches" / batch_id / "parallel").glob(
                "*/parallel_summary.json"
            ),
            key=lambda p: (p.parent.name, _mtime_or_zero(p)),
        ):
            payload = read_json(summary_file)
            for run in payload.get("runs", []) or []:
                if isinstance(run, dict) and run.get("name"):
                    measured[str(run["name"])] = run

    live = _collect_task_progress(root, batches)
    task_batch = {
        task_id: batch["batch_id"]
        for batch in batches
        for task_id in (batch.get("task_ids") or [])
    }

    path = results_csv_path(root)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_COLUMNS)
        writer.writeheader()
        for task_id, task in tasks.items():
            run = measured.get(task_id, {})
            progress = live.get(task_id, {})
            writer.writerow(
                {
                    "task_id": task_id,
                    "batch_id": task_batch.get(task_id, ""),
                    "state": run.get("state") or progress.get("status") or "queued",
                    "queue_policy": task.get("queue_policy", ""),
                    "match_policy": task.get("match_policy", ""),
                    "rabbit_job_percentage": task.get("rabbit_job_percentage", ""),
                    "rabbit_ceiling_percentage": task.get("rabbit_ceiling_percentage", ""),
                    "rabbit_distribution": task.get("rabbit_distribution", ""),
                    "rabbit_active": task.get("rabbit_active", ""),
                    "shake_seed": task.get("shake_seed", ""),
                    "duplicate": task.get("duplicate", ""),
                    "jobs_completed": run.get("jobs_completed", progress.get("jobs_completed", "")),
                    "jobs_total": run.get("jobs_total", progress.get("jobs_total", "")),
                    # Fall back to the child's own summary: a run cut short by
                    # the walltime writes it, but may never produce the
                    # batch-level parallel_summary.json.
                    "makespan_seconds": _first_present(
                        run.get("makespan_seconds"), progress.get("makespan_seconds")
                    ),
                    "avg_queue_wait_seconds": _first_present(
                        run.get("avg_queue_wait_seconds"),
                        progress.get("avg_queue_wait_seconds"),
                    ),
                    "max_queue_wait_seconds": _first_present(
                        run.get("max_queue_wait_seconds"),
                        progress.get("max_queue_wait_seconds"),
                    ),
                    "wall_seconds": run.get("wall_seconds", ""),
                    "startup_seconds": run.get("startup_seconds", ""),
                    "sim_wall_seconds": run.get("sim_wall_seconds", ""),
                    "return_code": run.get("return_code", ""),
                    "run_root": run.get("run_root", ""),
                }
            )
    return path


def refresh_status(root: Path, *, spec: CampaignSpec | None = None) -> dict[str, Any]:
    root = root.expanduser().resolve()
    if spec is None:
        spec = load_campaign_snapshot(root)
    state = read_json(state_path(root)) or {"submitted": {}, "submission_history": []}
    submitted = state.setdefault("submitted", {})
    prefix = _batch_prefix_for_state(spec, state)
    rows = query_job_rows(
        spec,
        prefix,
        [str(item.get("fluxid")) for item in submitted.values() if item.get("fluxid")],
        include_inactive=True,
    )
    batches = read_jsonl(batches_path(root))
    rows_by_batch = _rows_by_batch_id(spec, batches, submitted, rows, state)
    terminal_marked = _mark_terminal_or_lost_submissions(
        root,
        spec,
        batches,
        submitted,
        rows_by_batch,
        now=time.time(),
    )
    _, release_changed = _release_terminal_flux_allocations(root, batches, rows_by_batch, state, spec)
    removed_terminal = 0
    for batch in batches:
        batch_id = batch["batch_id"]
        if _batch_state(root, batch_id, submitted, rows_by_batch, now=time.time()) in TERMINAL_STATES:
            if submitted.pop(batch_id, None) is not None:
                removed_terminal += 1
    if terminal_marked or removed_terminal or release_changed:
        state["updated_at"] = utcnow_iso()
        write_json(state_path(root), state)
    return update_status(root, spec=spec, active_rows=rows)


def copy_example_spec(path: Path) -> None:
    example = """version = 1

[campaign]
name = "tuolumne-rabbit-shake"
output_root = "/p/lustre5/ashworth12/flux-fiction-ensemble"
batch_size = 16
poll_interval_seconds = 60
walltime_minutes = 58
keep_generated_traces = false

[input]
trace = "/g/g14/ashworth12/workspace/ff-podman/tuo_data.csv"
format = "tuolumne"
slice_start = 0
slice_count = 10000
cpus_per_node = 16

[shake]
duplicates = 100
seed = 71007
job_percentage = 10
degree_seconds = 60
relative_cap_percent = 10

[rabbit]
capacity_gib = 1000000
distribution = "uniform"

[grid]
queue_policies = ["easy", "fcfs"]
match_policies = ["firstnodex", "lonodex"]
rabbit_job_percentages = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
rabbit_ceiling_percentages = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

[flux]
base_config = "/g/g14/ashworth12/workspace/ff-podman/flux-fiction-develop/src/config.toml"
base_config_json = "/g/g14/ashworth12/workspace/ff-podman/flux-fiction-accuracy-tests/configs/rabbit_scheduler.json"
# resource_file = "/path/to/rabbit/resource-graph.json"

[submission]
queues = ["pdebug", "pbatch"]
job_name_prefix = "ffe"

[submission.queue_limits.pdebug]
max_active = 16
max_submit_per_hour = 120

[submission.queue_limits.pbatch]
max_active = 256
max_submit_per_hour = 1000
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(example, encoding="utf-8")


def _require_campaign_root(root: Path, *, action: str) -> None:
    if not root.is_dir():
        raise ValueError(
            f"Refusing to {action} {root}: not a directory. Pass the campaign "
            "spec TOML or the campaign root directory."
        )
    markers = ("campaign_spec.json", "tasks.jsonl", "state.json")
    if not any((root / marker).exists() for marker in markers):
        raise ValueError(
            f"Refusing to {action} {root}: it does not look like a campaign root "
            f"(none of {', '.join(markers)} present)."
        )


def batch_output_summary_count(root: Path, batch_id: str) -> int:
    """How many child runs of this batch produced a summary.json."""
    pattern = str(root / "batches" / batch_id / "parallel" / "*" / "runs" / "*" / "child" / "summary.json")
    return len(glob.glob(pattern))


def failed_batch_report(root: Path) -> list[dict[str, Any]]:
    """Every batch the launcher has marked failed, with how much it produced.

    `state == "failed"` covers two very different outcomes, and they must not be
    treated alike:

      * a batch that died early (e.g. the parent runner raised) and wrote NO
        child summaries -- a total loss, worth resubmitting;
      * a batch that ran its full walltime and was killed at the wall. That is
        recorded as failed too (flux_result=TIMEOUT, "worker did not write
        batch_result.json"), but its children finalized and produced real
        metrics. Resubmitting that one would throw away good data.

    The summary count is the discriminator.
    """
    root = Path(root).expanduser().resolve()
    out: list[dict[str, Any]] = []
    for batch in read_jsonl(batches_path(root)):
        batch_id = str(batch.get("batch_id"))
        result = read_json(root / "batches" / batch_id / "batch_result.json")
        if result.get("state") != "failed":
            continue
        summaries = batch_output_summary_count(root, batch_id)
        out.append({
            "batch_id": batch_id,
            # batches.jsonl records the list as task_ids (batch_result.json uses
            # "tasks"); prefer the manifest's own count when present.
            "task_count": int(batch.get("task_count") or len(batch.get("task_ids") or [])),
            "summaries": summaries,
            "total_loss": summaries == 0,
            "flux_result": result.get("flux_result") or "",
            "return_code": result.get("return_code"),
            "failure_reason": (result.get("failure_reason") or "")[:160],
        })
    return out


def retry_failed_batches(
    root: Path,
    *,
    batch_ids: list[str] | None = None,
    include_partial: bool = False,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Clear failure markers so the launcher resubmits those batches.

    Requeueing is exactly: remove the terminal batch_result.json and drop the
    batch from state.json's `submitted` map. _batch_state() then finds no
    terminal result, no live RJMS row and no submission record, so it reports
    "queued" and the next tick submits it. launcher_tick re-reads state.json
    from disk every tick, so this works against a LIVE launcher -- no restart.

    By default only total-loss batches (zero child summaries) are retried, so a
    batch that legitimately ran to its walltime keeps its data.
    """
    root = Path(root).expanduser().resolve()
    _require_campaign_root(root, action="retry")
    candidates = failed_batch_report(root)
    if batch_ids:
        wanted = set(batch_ids)
        candidates = [c for c in candidates if c["batch_id"] in wanted]
    if not include_partial:
        candidates = [c for c in candidates if c["total_loss"]]
    if dry_run or not candidates:
        return candidates

    for item in candidates:
        batch_dir = root / "batches" / item["batch_id"]
        for name in ("batch_result.json", "batch_status.json"):
            (batch_dir / name).unlink(missing_ok=True)

    # Re-read immediately before writing: a live launcher rewrites state.json on
    # every accepted submission, so keep the window as small as possible.
    state = read_json(state_path(root)) or {"submitted": {}, "submission_history": []}
    submitted = state.setdefault("submitted", {})
    for item in candidates:
        submitted.pop(item["batch_id"], None)
    state["updated_at"] = utcnow_iso()
    write_json(state_path(root), state)
    return candidates


def reset_campaign(root: Path) -> Path | None:
    """Archive the campaign root out of the way; returns the archive path."""
    if not root.exists():
        return None
    _require_campaign_root(root, action="reset")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive = root.with_name(f"{root.name}-archived-{stamp}")
    counter = 1
    while archive.exists():
        archive = root.with_name(f"{root.name}-archived-{stamp}-{counter}")
        counter += 1
    root.rename(archive)
    return archive


def delete_campaign(root: Path) -> bool:
    if not root.exists():
        return False
    _require_campaign_root(root, action="delete")
    shutil.rmtree(root)
    return True
