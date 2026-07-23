from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Callable

from flux_fiction.api.status import RunStatusWriter, utcnow_iso
from flux_fiction.parallel import flux_launch
from flux_fiction.parallel.config import ParallelRunPlan, ResolvedParallelPlan


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


def _write_flux_fiction_toml(path: Path, data: dict[str, Any]) -> None:
    lines = ["[flux_fiction]"]
    for key, value in data.items():
        lines.append(f"{key} = {_toml_value(value)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


@dataclass(frozen=True)
class PreparedParallelRun:
    plan: ParallelRunPlan
    run_root: Path
    child_run_dir: Path
    stampfile: Path
    launch_config: Path
    child_status_file: Path
    child_summary_file: Path
    log_file: Path
    launch_metadata_file: Path
    command: list[str]


@dataclass
class ActiveParallelRun:
    prepared: PreparedParallelRun
    proc: subprocess.Popen[str]
    log_handle: Any
    started_at: str


@dataclass(frozen=True)
class ParallelExecutionResult:
    return_code: int
    status_path: Path
    summary_path: Path
    manifest_snapshot_path: Path


def _resolve_config_relative_path(source_config: Path, value: Any) -> Any:
    if not isinstance(value, str) or not value:
        return value
    path = Path(value).expanduser()
    if path.is_absolute():
        return str(path.resolve())
    return str((source_config.parent / path).resolve())


def _normalize_config_snapshot_paths(source_config: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(cfg)
    for key in ("job_traces", "config_json", "resource_file", "resource_R", "raw_jobspec_file"):
        if key in normalized:
            normalized[key] = _resolve_config_relative_path(source_config, normalized[key])
    return normalized


def _build_run_command(prepared: PreparedParallelRun, max_concurrent: int = 1) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "flux_fiction.cli.run_ff",
        str(prepared.launch_config),
        "--run-dir",
        str(prepared.child_run_dir),
        "--stampfile",
        str(prepared.stampfile),
        "--broker-log-level",
        str(prepared.plan.broker_log_level),
    ]
    if prepared.plan.tag:
        cmd.extend(["--tag", prepared.plan.tag])
    if prepared.plan.no_faketime:
        cmd.append("--no-faketime")
    if os.environ.get("FLUX_FICTION_NO_BROKER_LOG_FILE", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        cmd.append("--no-broker-log-file")
    # Flux-native placement: the enclosing instance allocates a disjoint core
    # block per replica and the job shell binds to it. Empty prefix (plain
    # launch) unless FLUX_FICTION_FLUX_LAUNCH is set.
    flux_prefix = flux_launch.launch_prefix(prepared.plan.name, max_concurrent)
    return flux_prefix + cmd


def prepare_parallel_run(plan_run: ParallelRunPlan, max_concurrent: int = 1) -> PreparedParallelRun:
    run_root = Path(plan_run.run_root)
    child_run_dir = Path(plan_run.child_run_dir)
    stampfile = Path(plan_run.stampfile)
    run_root.mkdir(parents=True, exist_ok=False)
    child_run_dir.parent.mkdir(parents=True, exist_ok=True)

    source_config = Path(plan_run.config_file)
    cfg_doc = _load_toml(source_config)
    cfg = _normalize_config_snapshot_paths(source_config, dict(cfg_doc.get("flux_fiction", cfg_doc)))
    if plan_run.job_traces is not None:
        cfg["job_traces"] = plan_run.job_traces
    if plan_run.config_json is not None:
        cfg["config_json"] = plan_run.config_json
    if plan_run.resource_file is not None:
        cfg["resource_file"] = plan_run.resource_file
        cfg.pop("resource_R", None)
    if plan_run.resource_R is not None:
        cfg["resource_R"] = plan_run.resource_R
        cfg.pop("resource_file", None)
    cfg.update(plan_run.config_overrides or {})

    launch_config = run_root / "launch_config.toml"
    _write_flux_fiction_toml(launch_config, cfg)

    child_status_file = child_run_dir / "status.json"
    child_summary_file = child_run_dir / "summary.json"
    log_file = run_root / "launcher.log"
    launch_metadata_file = run_root / "launch.json"
    prepared = PreparedParallelRun(
        plan=plan_run,
        run_root=run_root,
        child_run_dir=child_run_dir,
        stampfile=stampfile,
        launch_config=launch_config,
        child_status_file=child_status_file,
        child_summary_file=child_summary_file,
        log_file=log_file,
        launch_metadata_file=launch_metadata_file,
        command=[],
    )
    command = _build_run_command(prepared, max_concurrent)
    prepared = PreparedParallelRun(
        plan=prepared.plan,
        run_root=prepared.run_root,
        child_run_dir=prepared.child_run_dir,
        stampfile=prepared.stampfile,
        launch_config=prepared.launch_config,
        child_status_file=prepared.child_status_file,
        child_summary_file=prepared.child_summary_file,
        log_file=prepared.log_file,
        launch_metadata_file=prepared.launch_metadata_file,
        command=command,
    )
    launch_metadata_file.write_text(
        json.dumps(
            {
                "version": 1,
                "name": plan_run.name,
                "ordinal": plan_run.ordinal,
                "run_root": str(run_root),
                "child_run_dir": str(child_run_dir),
                "launch_config": str(launch_config),
                "stampfile": str(stampfile),
                "summary_file": str(child_summary_file),
                "config_overrides": plan_run.config_overrides,
                "command": command,
                "metadata": plan_run.metadata,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return prepared


def _snapshot_run_status(record: dict[str, Any], child_status: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(record)
    for key in (
        "jobs_total",
        "jobs_submitted",
        "jobs_started",
        "jobs_running",
        "jobs_completed",
        "current_sim_time",
        "time_step",
        "config_file",
        "source_config_file",
        "trace_file",
        "faketime_timestamp_file",
        "failure_reason",
        "return_code",
    ):
        if key in child_status:
            snapshot[key] = child_status[key]
    if child_status.get("state"):
        snapshot["child_state"] = child_status["state"]
    if child_status.get("updated_at"):
        snapshot["child_updated_at"] = child_status["updated_at"]
    return snapshot


def _snapshot_run_summary(record: dict[str, Any], child_summary: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(record)
    for key in (
        "makespan_seconds",
        "makespan_hours",
        "avg_queue_wait_seconds",
        "max_queue_wait_seconds",
        "jobs_total",
        "jobs_completed",
        "resource_summary",
    ):
        if key in child_summary:
            snapshot[key] = child_summary[key]
    if child_summary:
        snapshot["summary_generated_at"] = child_summary.get("generated_at")
    return snapshot


def _summarize_records(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "queued": 0,
        "launching": 0,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "interrupted": 0,
    }
    for record in records:
        state = str(record.get("state") or "queued")
        counts[state] = counts.get(state, 0) + 1
    return counts


def _write_parallel_summary(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _elapsed_seconds(started_at: str | None, finished_at: str | None = None) -> float | None:
    if not started_at:
        return None
    try:
        start = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(finished_at).replace("Z", "+00:00")) if finished_at else datetime.now(start.tzinfo)
    except Exception:
        return None
    return max(0.0, (end - start).total_seconds())


def _aggregate_job_progress(records: list[dict[str, Any]]) -> tuple[int, int]:
    completed = 0
    total = 0
    for record in records:
        completed += int(record.get("jobs_completed") or 0)
        total += int(record.get("jobs_total") or 0)
    return completed, total


def _compute_makespan_extremes(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]] | None:
    succeeded = [
        record for record in records
        if record.get("state") == "succeeded" and record.get("makespan_seconds") is not None
    ]
    if not succeeded:
        return None
    shortest = min(succeeded, key=lambda item: float(item.get("makespan_seconds") or 0.0))
    longest = max(succeeded, key=lambda item: float(item.get("makespan_seconds") or 0.0))
    return {
        "shortest": {
            "name": shortest.get("name"),
            "makespan_seconds": float(shortest.get("makespan_seconds") or 0.0),
            "run_root": shortest.get("run_root"),
        },
        "longest": {
            "name": longest.get("name"),
            "makespan_seconds": float(longest.get("makespan_seconds") or 0.0),
            "run_root": longest.get("run_root"),
        },
    }


def _print_progress_summary(records: list[dict[str, Any]], *, started_at: str) -> None:
    counts = _summarize_records(records)
    completed_jobs, total_jobs = _aggregate_job_progress(records)
    elapsed = _elapsed_seconds(started_at)
    elapsed_text = "n/a" if elapsed is None else f"{elapsed:.1f}s"
    jobs_text = f"{completed_jobs}/{total_jobs}" if total_jobs else f"{completed_jobs}/?"
    print(
        "Progress: "
        f"queued={counts['queued']} launching={counts['launching']} running={counts['running']} "
        f"succeeded={counts['succeeded']} failed={counts['failed']} skipped={counts['skipped']} "
        f"interrupted={counts['interrupted']} "
        f"jobs={jobs_text} wall={elapsed_text}"
    )


def _print_final_run_summary(records: list[dict[str, Any]], *, show_makespan_extremes: bool) -> None:
    print("")
    print("Final run summary:")
    for record in sorted(records, key=lambda item: int(item.get("ordinal") or 0)):
        jobs_total = record.get("jobs_total")
        jobs_completed = record.get("jobs_completed")
        jobs_text = (
            f"{int(jobs_completed or 0)}/{int(jobs_total or 0)}"
            if jobs_total is not None
            else f"{int(jobs_completed or 0)}/?"
        )
        rc = record.get("return_code")
        makespan = record.get("makespan_seconds")
        wall = _elapsed_seconds(record.get("started_at"), record.get("finished_at"))
        makespan_text = "n/a" if makespan is None else f"{float(makespan):.1f}s"
        wall_text = "n/a" if wall is None else f"{wall:.1f}s"
        print(
            f"[{int(record.get('ordinal') or 0):04d}] {record.get('name')} "
            f"state={record.get('state')} rc={rc if rc is not None else 'n/a'} "
            f"jobs={jobs_text} makespan={makespan_text} wall={wall_text} "
            f"run_root={record.get('run_root')}"
        )
    if show_makespan_extremes:
        extremes = _compute_makespan_extremes(records)
        if extremes is not None:
            print("")
            print(
                "Shortest makespan: "
                f"{extremes['shortest']['name']} ({extremes['shortest']['makespan_seconds']:.1f}s)"
            )
            print(
                "Longest makespan:  "
                f"{extremes['longest']['name']} ({extremes['longest']['makespan_seconds']:.1f}s)"
            )


def _terminate_active_process(proc: subprocess.Popen[str], *, timeout: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        proc.kill()
    except ProcessLookupError:
        return
    proc.wait(timeout=timeout)


def run_parallel_plan(
    plan: ResolvedParallelPlan,
    *,
    poll_interval: float = 0.2,
    command_builder: Callable[[PreparedParallelRun], list[str]] | None = None,
    show_makespan_extremes: bool = False,
) -> ParallelExecutionResult:
    output_root = Path(plan.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_snapshot_path = output_root / "manifest.snapshot.toml"
    shutil.copy2(plan.manifest_path, manifest_snapshot_path)

    prepared_runs = [
        prepare_parallel_run(run_plan, max_concurrent=plan.max_concurrent)
        for run_plan in plan.runs
    ]
    for prepared in prepared_runs:
        if command_builder is not None:
            command = command_builder(prepared)
            prepared.launch_metadata_file.write_text(
                json.dumps(
                    {
                        **_read_json_dict(prepared.launch_metadata_file),
                        "command": command,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            object.__setattr__(prepared, "command", command)

    status_path = output_root / "parallel_status.json"
    summary_path = output_root / "parallel_summary.json"
    status = RunStatusWriter(status_path)

    run_records: list[dict[str, Any]] = []
    for prepared in prepared_runs:
        run_records.append(
            {
                "ordinal": prepared.plan.ordinal,
                "name": prepared.plan.name,
                "slug": prepared.plan.slug,
                "state": "queued",
                "run_root": str(prepared.run_root),
                "child_run_dir": str(prepared.child_run_dir),
                "status_file": str(prepared.child_status_file),
                "summary_file": str(prepared.child_summary_file),
                "log_file": str(prepared.log_file),
                "stampfile": str(prepared.stampfile),
                "launch_config": str(prepared.launch_config),
                "metadata": prepared.plan.metadata,
            }
        )

    started_at = utcnow_iso()
    status.update(
        state="running",
        manifest_path=plan.manifest_path,
        output_root=str(output_root),
        started_at=started_at,
        max_concurrent=plan.max_concurrent,
        fail_fast=plan.fail_fast,
        progress_mode=plan.progress_mode,
        summary_interval=plan.summary_interval,
        shard_index=plan.shard_index,
        shard_count=plan.shard_count,
        total_runs=len(run_records),
        runs=run_records,
        **_summarize_records(run_records),
    )

    active: list[ActiveParallelRun] = []
    pending = list(prepared_runs)
    record_by_name = {record["name"]: record for record in run_records}
    failure_seen = False
    last_summary_print = 0.0
    interrupted_reason: str | None = None
    stop_requests: dict[str, dict[str, str]] = {}
    fail_fast_applied = False
    main_thread = threading.current_thread() is threading.main_thread()
    previous_handlers: dict[int, Any] = {}
    shutdown_signal: dict[str, int | None] = {"signal": None}

    def refresh_parent_status(final_state: str | None = None) -> None:
        for idx, record in enumerate(run_records):
            child_status = _read_json_dict(Path(record["status_file"]))
            child_summary = _read_json_dict(Path(record["summary_file"]))
            updated = _snapshot_run_status(record, child_status)
            refreshed = _snapshot_run_summary(updated, child_summary)
            run_records[idx].clear()
            run_records[idx].update(refreshed)
        counts = _summarize_records(run_records)
        status.update(
            state=final_state or "running",
            total_runs=len(run_records),
            runs=run_records,
            **counts,
        )

    def request_stop_for_pending(reason: str) -> None:
        while pending:
            prepared = pending.pop(0)
            record = record_by_name[prepared.plan.name]
            record["state"] = "skipped"
            record["failure_reason"] = reason
            record["finished_at"] = utcnow_iso()

    def request_stop_for_active(reason: str, *, state: str) -> None:
        for active_run in active:
            name = active_run.prepared.plan.name
            if name in stop_requests:
                continue
            stop_requests[name] = {"reason": reason, "state": state}
            record = record_by_name[name]
            record["failure_reason"] = reason
            if plan.progress_mode == "summary":
                print(
                    f"Stopping  [{active_run.prepared.plan.ordinal:04d}] "
                    f"{active_run.prepared.plan.name}: {reason}"
                )
            _terminate_active_process(active_run.proc)

    def signal_name(signum: int | None) -> str:
        if signum is None:
            return "signal"
        try:
            return signal.Signals(signum).name
        except Exception:
            return f"signal {signum}"

    def _request_interrupt(signum, _frame) -> None:
        shutdown_signal["signal"] = int(signum)
        nonlocal interrupted_reason
        interrupted_reason = f"Interrupted by {signal_name(int(signum))}"

    if main_thread:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, _request_interrupt)

    try:
        while pending or active:
            if interrupted_reason is not None:
                failure_seen = True
                request_stop_for_pending(interrupted_reason)
                request_stop_for_active(interrupted_reason, state="interrupted")
                refresh_parent_status(final_state="interrupted")

            while pending and len(active) < plan.max_concurrent and not (plan.fail_fast and failure_seen):
                prepared = pending.pop(0)
                record = record_by_name[prepared.plan.name]
                record["state"] = "launching"
                record["started_at"] = utcnow_iso()
                log_handle = None
                try:
                    log_handle = prepared.log_file.open("w", encoding="utf-8")
                    proc = subprocess.Popen(
                        prepared.command,
                        cwd=Path(plan.manifest_path).resolve().parents[0],
                        env=os.environ.copy(),
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                except Exception as e:
                    if log_handle is not None:
                        log_handle.close()
                    record["state"] = "failed"
                    record["finished_at"] = utcnow_iso()
                    record["return_code"] = 1
                    record["failure_reason"] = f"Failed to launch child run: {e}"
                    failure_seen = True
                    if plan.progress_mode == "summary":
                        print(
                            f"Launch failed [{prepared.plan.ordinal:04d}] "
                            f"{prepared.plan.name}: {e}"
                        )
                    refresh_parent_status()
                    continue
                record["pid"] = proc.pid
                active.append(
                    ActiveParallelRun(
                        prepared=prepared,
                        proc=proc,
                        log_handle=log_handle,
                        started_at=record["started_at"],
                    )
                )
                if plan.progress_mode == "summary":
                    print(f"Launching [{prepared.plan.ordinal:04d}] {prepared.plan.name}")
                refresh_parent_status()

            completed_any = False
            for active_run in list(active):
                rc = active_run.proc.poll()
                if rc is None:
                    child_status = _read_json_dict(active_run.prepared.child_status_file)
                    record = record_by_name[active_run.prepared.plan.name]
                    if child_status.get("state") == "running":
                        record["state"] = "running"
                    continue

                completed_any = True
                active.remove(active_run)
                active_run.log_handle.close()
                record = record_by_name[active_run.prepared.plan.name]
                child_status = _read_json_dict(active_run.prepared.child_status_file)
                stop_request = stop_requests.get(active_run.prepared.plan.name)
                if stop_request is not None and rc != 0:
                    final_state = stop_request["state"]
                else:
                    final_state = "succeeded" if rc == 0 else "failed"
                record["state"] = final_state
                record["finished_at"] = utcnow_iso()
                record["return_code"] = rc
                if stop_request is not None and not child_status.get("failure_reason"):
                    record["failure_reason"] = stop_request["reason"]
                elif child_status.get("failure_reason"):
                    record["failure_reason"] = child_status["failure_reason"]
                if child_status.get("state") in {"failed", "succeeded"}:
                    record["child_state"] = child_status["state"]
                if rc != 0 and stop_request is None:
                    failure_seen = True
                if plan.progress_mode == "summary":
                    print(
                        f"Finished  [{active_run.prepared.plan.ordinal:04d}] "
                        f"{active_run.prepared.plan.name} rc={rc}"
                    )
                refresh_parent_status()

            if plan.fail_fast and failure_seen and pending:
                request_stop_for_pending(
                    "Skipped because fail_fast stopped new launches after a prior failure"
                )
                refresh_parent_status()

            if plan.fail_fast and failure_seen and active and not fail_fast_applied:
                request_stop_for_active(
                    "Terminated because fail_fast stopped the parallel run after a prior failure",
                    state="interrupted",
                )
                fail_fast_applied = True
                refresh_parent_status()

            if pending or active:
                if not completed_any:
                    refresh_parent_status()
                if plan.progress_mode == "summary":
                    now = time.monotonic()
                    if last_summary_print == 0.0 or (now - last_summary_print) >= float(plan.summary_interval):
                        _print_progress_summary(run_records, started_at=started_at)
                        last_summary_print = now
                time.sleep(poll_interval)
    finally:
        if main_thread:
            for signum, previous in previous_handlers.items():
                signal.signal(signum, previous)
        for active_run in active:
            if active_run.proc.poll() is None:
                _terminate_active_process(active_run.proc)
            active_run.log_handle.close()

    final_state = "interrupted" if interrupted_reason is not None else ("failed" if failure_seen else "succeeded")
    refresh_parent_status(final_state=final_state)
    final_payload = status.read()
    final_payload["finished_at"] = utcnow_iso()
    final_payload["state"] = final_state
    final_payload["makespan_extremes"] = _compute_makespan_extremes(run_records)
    _write_parallel_summary(summary_path, final_payload)
    status.update(
        state=final_state,
        finished_at=final_payload["finished_at"],
        makespan_extremes=final_payload["makespan_extremes"],
    )
    _print_final_run_summary(run_records, show_makespan_extremes=show_makespan_extremes)

    return ParallelExecutionResult(
        return_code=130 if interrupted_reason is not None else (0 if not failure_seen else 1),
        status_path=status_path,
        summary_path=summary_path,
        manifest_snapshot_path=manifest_snapshot_path,
    )
