from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

from flux_fiction.parallel import load_parallel_manifest, prepare_parallel_run, resolve_parallel_plan, run_parallel_plan
from flux_fiction.parallel.runner import _build_run_command


def _write_manifest(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def _success_or_fail_command(prepared) -> list[str]:
    rc = int(prepared.plan.metadata.get("expected_rc", 0))
    sleep_s = float(prepared.plan.metadata.get("sleep_s", 0.05))
    makespan_s = float(prepared.plan.metadata.get("makespan_s", 10.0))
    script = """
import json
from pathlib import Path
import sys
import time

status_path = Path(sys.argv[1])
rc = int(sys.argv[2])
name = sys.argv[3]
sleep_s = float(sys.argv[4])
summary_path = Path(sys.argv[5])
makespan_s = float(sys.argv[6])
status_path.parent.mkdir(parents=True, exist_ok=True)
status_path.write_text(json.dumps({
    "version": 1,
    "state": "running",
    "jobs_total": 4,
    "jobs_submitted": 2,
    "jobs_started": 2,
    "jobs_running": 2,
    "jobs_completed": 0,
    "run_dir": str(status_path.parent),
    "trace_file": name,
}) + "\\n", encoding="utf-8")
time.sleep(sleep_s)
status_path.write_text(json.dumps({
    "version": 1,
    "state": "succeeded" if rc == 0 else "failed",
    "jobs_total": 4,
    "jobs_submitted": 4,
    "jobs_started": 4,
    "jobs_running": 0,
    "jobs_completed": 4,
    "run_dir": str(status_path.parent),
    "trace_file": name,
    "return_code": rc,
    "failure_reason": None if rc == 0 else f"{name} failed",
}) + "\\n", encoding="utf-8")
summary_path.write_text(json.dumps({
    "version": 1,
    "state": "succeeded" if rc == 0 else "failed",
    "generated_at": "2026-06-22T00:00:00Z",
    "jobs_total": 4,
    "jobs_completed": 4,
    "makespan_seconds": makespan_s,
    "makespan_hours": makespan_s / 3600.0,
    "avg_queue_wait_seconds": 1.25,
    "max_queue_wait_seconds": 2.5,
}) + "\\n", encoding="utf-8")
print(f"child {name} rc={rc}")
sys.exit(rc)
""".strip()
    return [
        sys.executable,
        "-c",
        script,
        str(prepared.child_status_file),
        str(rc),
        prepared.plan.name,
        str(sleep_s),
        str(prepared.child_summary_file),
        str(makespan_s),
    ]


def test_prepare_parallel_run_materializes_config_override(tmp_path):
    config = tmp_path / "config.toml"
    trace_a = tmp_path / "trace-a.csv"
    trace_b = tmp_path / "trace-b.csv"
    config.write_text("[flux_fiction]\njob_traces = \"trace-a.csv\"\n", encoding="ascii")
    trace_a.write_text("Submit,Elapsed,Timelimit,NNodes,NCPUS\n", encoding="ascii")
    trace_b.write_text("Submit,Elapsed,Timelimit,NNodes,NCPUS\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        f"""
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "override"
config_file = "./config.toml"
job_traces = "./trace-b.csv"
quiet = true
backend = "mock"
account_system_latency = false
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    plan = resolve_parallel_plan(manifest)

    prepared = prepare_parallel_run(plan.runs[0])

    launch_cfg = prepared.launch_config.read_text(encoding="utf-8")
    assert str(trace_b.resolve()) in launch_cfg
    assert "quiet = true" in launch_cfg
    assert 'backend = "mock"' in launch_cfg
    assert "account_system_latency = false" in launch_cfg
    assert prepared.launch_metadata_file.exists()


def test_parallel_runner_can_disable_broker_file_sink(tmp_path, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "quiet-broker"
config_file = "./config.toml"
""".strip()
        + "\n",
    )
    prepared = prepare_parallel_run(resolve_parallel_plan(load_parallel_manifest(manifest_path)).runs[0])

    monkeypatch.setenv("FLUX_FICTION_NO_BROKER_LOG_FILE", "true")

    assert "--no-broker-log-file" in _build_run_command(prepared)


def test_run_parallel_plan_writes_parent_status_and_summary(tmp_path, capsys):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 2
output_root = "./parallel-output"

[[run]]
name = "alpha"
config_file = "./config.toml"

[run.metadata]
expected_rc = 0
makespan_s = 8.0

[[run]]
name = "beta"
config_file = "./config.toml"

[run.metadata]
expected_rc = 0
makespan_s = 12.0
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    plan = resolve_parallel_plan(manifest)

    result = run_parallel_plan(
        plan,
        poll_interval=0.01,
        command_builder=_success_or_fail_command,
        show_makespan_extremes=True,
    )

    captured = capsys.readouterr()
    assert result.return_code == 0
    status = json.loads(result.status_path.read_text(encoding="utf-8"))
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert status["state"] == "succeeded"
    assert summary["state"] == "succeeded"
    assert status["succeeded"] == 2
    assert len(status["runs"]) == 2
    assert summary["makespan_extremes"]["shortest"]["name"] == "alpha"
    assert summary["makespan_extremes"]["longest"]["name"] == "beta"
    assert Path(result.manifest_snapshot_path).exists()
    assert "Final run summary:" in captured.out
    assert "Shortest makespan: alpha (8.0s)" in captured.out
    assert "Longest makespan:  beta (12.0s)" in captured.out


def test_run_parallel_plan_fail_fast_skips_pending_runs(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1
fail_fast = true
output_root = "./parallel-output"

[[run]]
name = "fail-first"
config_file = "./config.toml"

[run.metadata]
expected_rc = 1
makespan_s = 15.0

[[run]]
name = "skip-second"
config_file = "./config.toml"

[run.metadata]
expected_rc = 0
makespan_s = 3.0
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    plan = resolve_parallel_plan(manifest)

    result = run_parallel_plan(plan, poll_interval=0.01, command_builder=_success_or_fail_command)

    assert result.return_code == 1
    status = json.loads(result.status_path.read_text(encoding="utf-8"))
    states = {run["name"]: run["state"] for run in status["runs"]}
    assert states["fail-first"] == "failed"
    assert states["skip-second"] == "skipped"


def test_run_parallel_plan_fail_fast_interrupts_active_runs(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 2
fail_fast = true
output_root = "./parallel-output"

[[run]]
name = "fail-first"
config_file = "./config.toml"

[run.metadata]
expected_rc = 1
sleep_s = 0.01
makespan_s = 15.0

[[run]]
name = "long-running"
config_file = "./config.toml"

[run.metadata]
expected_rc = 0
sleep_s = 5.0
makespan_s = 30.0
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    plan = resolve_parallel_plan(manifest)

    result = run_parallel_plan(plan, poll_interval=0.01, command_builder=_success_or_fail_command)

    assert result.return_code == 1
    status = json.loads(result.status_path.read_text(encoding="utf-8"))
    states = {run["name"]: run["state"] for run in status["runs"]}
    long_run = next(run for run in status["runs"] if run["name"] == "long-running")
    assert states["fail-first"] == "failed"
    assert states["long-running"] == "interrupted"
    assert "fail_fast" in str(long_run.get("failure_reason") or "")


def test_run_parallel_plan_interrupts_active_runs_on_sigint(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1
output_root = "./parallel-output"
summary_interval = 100.0

[[run]]
name = "long-running"
config_file = "./config.toml"

[run.metadata]
expected_rc = 0
sleep_s = 5.0
makespan_s = 30.0
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    plan = resolve_parallel_plan(manifest)

    def send_sigint():
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGINT)

    interrupter = threading.Thread(target=send_sigint, daemon=True)
    interrupter.start()
    result = run_parallel_plan(plan, poll_interval=0.01, command_builder=_success_or_fail_command)
    interrupter.join(timeout=1)

    assert result.return_code == 130
    status = json.loads(result.status_path.read_text(encoding="utf-8"))
    run = status["runs"][0]
    assert status["state"] == "interrupted"
    assert run["state"] == "interrupted"
    assert "Interrupted by SIGINT" in str(run.get("failure_reason") or "")


def test_run_parallel_plan_prints_periodic_summary(tmp_path, capsys):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1
summary_interval = 0.01
progress_mode = "summary"
output_root = "./parallel-output"

[[run]]
name = "alpha"
config_file = "./config.toml"

[run.metadata]
expected_rc = 0
sleep_s = 0.03
makespan_s = 5.0
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    plan = resolve_parallel_plan(manifest)

    result = run_parallel_plan(plan, poll_interval=0.005, command_builder=_success_or_fail_command)

    captured = capsys.readouterr()
    assert result.return_code == 0
    assert "Progress: queued=" in captured.out
