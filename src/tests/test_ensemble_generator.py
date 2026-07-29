from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from flux_fiction.ensemble.campaign import (
    _collect_task_progress,
    _slurm_state_result,
    batches_path,
    failed_batch_report,
    read_json,
    retry_failed_batches,
    state_path,
    _submit_batch,
    submission_backend,
    _release_terminal_flux_allocations,
    _wait_for_flux_reactor_event,
    batch_tasks,
    ensure_worker_script,
    generate_tasks,
    initialize_campaign,
    launcher_tick,
    materialize_task_inputs,
    read_jsonl,
    run_launcher,
    status_path,
    tasks_path,
    update_status,
    write_json,
    write_results_csv,
)
from flux_fiction.ensemble.config import EnsembleConfigError, load_campaign_spec
from flux_fiction.ensemble.trace import (
    apply_rabbit_requests,
    apply_shake,
    load_normalized_trace,
    shake_interarrival,
)


def _write_base_files(tmp_path: Path) -> tuple[Path, Path, Path]:
    trace = tmp_path / "trace.csv"
    trace.write_text(
        "\n".join(
            [
                "JobID,NNodes,NCPUS,Timelimit,Submit,Elapsed",
                "1,1,4,00:10:00,2026-01-01T00:00:00,00:01:00",
                "2,1,4,00:10:00,2026-01-01T00:10:00,00:01:00",
                "3,1,4,00:10:00,2026-01-01T00:20:00,00:01:00",
                "4,1,4,00:10:00,2026-01-01T00:30:00,00:01:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config = tmp_path / "config.toml"
    config.write_text(
        "[flux_fiction]\n"
        f"job_traces = {json.dumps(str(trace))}\n"
        "nnodes = 4\n"
        "ncpus = 4\n"
        'backend = "mock"\n',
        encoding="utf-8",
    )
    scheduler = tmp_path / "scheduler.json"
    scheduler.write_text(
        json.dumps(
            {
                "sched-fluxion-qmanager": {"queue-policy": "easy"},
                "sched-fluxion-resource": {"match-policy": "lonodex"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return trace, config, scheduler


def _write_spec(
    tmp_path: Path,
    *,
    trace: Path,
    config: Path,
    scheduler: Path,
    rabbit_capacity: int | None = 1000,
    resource_file: Path | None = None,
    campaign_extra: str = "",
) -> Path:
    rabbit_capacity_line = "" if rabbit_capacity is None else f"capacity_gib = {rabbit_capacity}"
    resource_file_line = "" if resource_file is None else f'resource_file = "{resource_file}"'
    spec = tmp_path / "campaign.toml"
    spec.write_text(
        f"""
version = 1

[campaign]
name = "unit-campaign"
output_root = "{tmp_path / "out"}"
batch_size = 3
keep_generated_traces = false
no_faketime = true
{campaign_extra}

[input]
trace = "{trace}"
format = "sacct"
slice_start = 0
slice_count = 4
cpus_per_node = 4

[shake]
duplicates = 2
seed = 100
job_percentage = 50
degree_seconds = 60
relative_cap_percent = 10

[rabbit]
{rabbit_capacity_line}
distribution = "uniform"

[grid]
queue_policies = ["easy", "fcfs"]
match_policies = ["lonodex"]
rabbit_job_percentages = [0, 50]
rabbit_ceiling_percentages = [0, 10]

[flux]
base_config = "{config}"
base_config_json = "{scheduler}"
{resource_file_line}

[submission]
queues = ["pdebug"]

[submission.queue_limits.pdebug]
max_active = 2
max_submit_per_hour = 10
""",
        encoding="utf-8",
    )
    return spec


def _campaign_batch_name(root: Path, batch_id: str) -> str:
    state = json.loads((root / "state.json").read_text(encoding="utf-8"))
    run_id = state.get("run_id")
    if run_id:
        return f"ffe:unit-campaign:{run_id}:{batch_id}"
    return f"ffe:unit-campaign:{batch_id}"


def test_shake_interarrival_is_deterministic_and_bounded(tmp_path: Path):
    rows = [
        {"Submit": "2026-01-01T00:00:00"},
        {"Submit": "2026-01-01T00:10:00"},
        {"Submit": "2026-01-01T00:20:00"},
        {"Submit": "2026-01-01T00:30:00"},
    ]

    first = shake_interarrival(
        rows,
        seed=7,
        job_percentage=100,
        degree_seconds=300,
        relative_cap_percent=10,
    )
    second = shake_interarrival(
        rows,
        seed=7,
        job_percentage=100,
        degree_seconds=300,
        relative_cap_percent=10,
    )

    assert first == second
    assert first != rows
    assert first[0]["Submit"] == rows[0]["Submit"]
    assert all("t_submit" in row for row in first[1:])


def test_rabbit_requests_select_exact_percentage():
    rows = [{"Submit": f"2026-01-01T00:0{i}:00"} for i in range(10)]
    out = apply_rabbit_requests(
        rows,
        seed=11,
        job_percentage=30,
        capacity_gib=1000,
        ceiling_percent=10,
    )
    non_zero = [row for row in out if float(row["RabbitGiB"]) > 0]
    assert len(non_zero) == 3
    assert max(float(row["RabbitGiB"]) for row in non_zero) <= 100


def test_tuolumne_trace_normalization(tmp_path: Path):
    trace = tmp_path / "tuo.csv"
    trace.write_text(
        "@timestamp,job.id,job.node.count,job.timelimit_seconds,job.submittime,event.end,event.duration_seconds\n"
        '"Feb 2, 2026 @ 12:25:51.997",123,2,599,"Feb 2, 2026 @ 12:13:40.308","Feb 2, 2026 @ 12:26:02.602",10\n',
        encoding="utf-8",
    )

    rows = load_normalized_trace(
        trace,
        trace_format="tuolumne",
        cpus_per_node=16,
    )

    # NGPUS is always emitted: the sim's trace reader requires that column when
    # the modeled machine has GPUs, and the tuolumne export carries no GPU field.
    assert rows == [
        {
            "JobID": "123",
            "NNodes": "2",
            "NCPUS": "32",
            "NGPUS": "0",
            "Timelimit": "00:09:59",
            "Submit": "2026-02-02T12:13:40.308",
            "Elapsed": "00:00:10",
            "RabbitGiB": "0",
        }
    ]


def test_campaign_task_grid_batching_and_materialization(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec_path = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    spec = load_campaign_spec(spec_path)

    tasks = generate_tasks(spec)
    # 2 duplicates x 2 queue policies x 1 match policy = 4 policy cells. The
    # rabbit grid is rj=[0,50] x rc=[0,10]; three of those four pairings put no
    # rabbit in the trace, so they collapse to one control per cell -> 2 tasks
    # per cell rather than 4.
    assert len(tasks) == 8
    assert sum(1 for task in tasks if not task["rabbit_active"]) == 4
    assert tasks[0]["task_id"].find("s100") >= 0
    assert len(batch_tasks(tasks, batch_size=3)) == 3

    root = initialize_campaign(spec)
    assert len(read_jsonl(tasks_path(root))) == 8
    assert status_path(root).exists()

    generated = materialize_task_inputs(spec, tasks[-1], tmp_path / "generated-task")
    scheduler_payload = json.loads(generated["scheduler"].read_text(encoding="utf-8"))
    assert scheduler_payload["sched-fluxion-qmanager"]["queue-policy"] == tasks[-1]["queue_policy"]
    assert scheduler_payload["sched-fluxion-resource"]["match-policy"] == tasks[-1]["match_policy"]

    config_text = generated["config"].read_text(encoding="utf-8")
    assert str(generated["trace"]) in config_text
    assert str(generated["scheduler"]) in config_text

    with generated["trace"].open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 4
    assert "RabbitGiB" in rows[0]


def test_status_keeps_submitted_state_without_flux_reconcile(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    state = root / "state.json"
    payload = json.loads(state.read_text(encoding="utf-8"))
    payload["submitted"]["b000001"] = {"queue": "pdebug", "fluxid": "f1", "submitted_at": 1}
    state.write_text(json.dumps(payload), encoding="utf-8")

    status = update_status(root, spec=spec)

    assert status["batch_counts"]["submitted"] == 1
    assert status["batch_counts"]["failed"] == 0


def test_launcher_recovers_live_flux_job_missing_from_state(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)

    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign._flux_rows",
        lambda prefix, *, include_inactive=False: [
            {
                "id": "f-live",
                "queue": "pdebug",
                "state": "RUN",
                "result": "-",
                "name": _campaign_batch_name(root, "b000001"),
            }
        ],
    )
    submitted = []

    def fake_submit(root, spec, batch_id, queue, *, dry_run):
        submitted.append(batch_id)
        return f"f-{batch_id}"

    monkeypatch.setattr("flux_fiction.ensemble.campaign._submit_batch", fake_submit)

    launcher_tick(root, spec)

    state = json.loads((root / "state.json").read_text(encoding="utf-8"))
    assert state["submitted"]["b000001"]["fluxid"] == "f-live"
    assert state["submitted"]["b000001"]["recovered_at"]
    assert "b000001" not in submitted


def test_launcher_ignores_stale_legacy_flux_job_name(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    state_path = root / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.pop("run_id", None)
    state_path.write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign._flux_rows",
        lambda prefix, *, include_inactive=False: [
            {
                "id": "f-stale",
                "queue": "pdebug",
                "state": "INACTIVE",
                "result": "CANCELED",
                "name": "ffe:unit-campaign:b000001",
                "t_submit": 1,
            }
        ],
    )
    submitted = []

    def fake_submit(root, spec, batch_id, queue, *, dry_run):
        submitted.append(batch_id)
        return f"f-{batch_id}"

    monkeypatch.setattr("flux_fiction.ensemble.campaign._submit_batch", fake_submit)

    launcher_tick(root, spec)

    state_after = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_after["submitted"]["b000001"]["fluxid"] == "f-b000001"
    assert "b000001" in submitted
    assert not (root / "batches" / "b000001" / "batch_result.json").exists()


def test_launcher_does_not_recover_terminal_batch_with_active_outer_job(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    write_json(
        root / "batches" / "b000001" / "batch_result.json",
        {"state": "succeeded", "return_code": 0},
    )

    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign._flux_rows",
        lambda prefix, *, include_inactive=False: [
            {
                "id": "f-live",
                "queue": "pdebug",
                "state": "RUN",
                "result": "-",
                "name": _campaign_batch_name(root, "b000001"),
            }
        ],
    )
    submitted = []

    def fake_submit(root, spec, batch_id, queue, *, dry_run):
        submitted.append(batch_id)
        return f"f-{batch_id}"

    monkeypatch.setattr("flux_fiction.ensemble.campaign._submit_batch", fake_submit)

    launcher_tick(root, spec)

    state = json.loads((root / "state.json").read_text(encoding="utf-8"))
    assert "b000001" not in state["submitted"]
    assert "b000001" not in submitted


def test_worker_script_is_text_batch_script(tmp_path: Path):
    path = ensure_worker_script(tmp_path)
    text = path.read_text(encoding="utf-8")

    assert text.startswith("#!/usr/bin/env bash")
    assert 'WORKER_RUNTIME="${FLUX_FICTION_WORKER_RUNTIME:-${DEFAULT_WORKER_RUNTIME}}"' in text
    assert "run_container_worker()" in text
    assert "podman run -d --rm --name" in text
    assert "podman logs -f" in text
    assert "podman wait" in text
    assert "exited before writing" in text
    assert "RESULT_PATH=" in text
    assert "localhost/flux-fiction-dev:latest" in text
    assert "FLUX_FICTION_FAKETIME_DIR=/dev/shm" in text
    assert "--shm-size=1g" in text
    assert "-m flux_fiction_ensemble worker" in text
    assert path.stat().st_mode & 0o111


def test_campaign_parses_spack_runtime_and_flux_pinning(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(
        _write_spec(
            tmp_path,
            trace=trace,
            config=config,
            scheduler=scheduler,
            campaign_extra="""
worker_runtime = "spack"
flux_launch = true
flux_launch_cores = 6
spack_fluxion_prefix = "/opt/fluxion"
spack_faketime_prefix = "/opt/faketime"
spack_gcc_runtime = "/opt/gcc-runtime"
spack_view = "/opt/view"
spack_ninja_prefix = "/opt/ninja"
meson_pythonpath = "/opt/meson"
host_jobtap_cc = "gcc-12"
host_tmpdir = "/tmp/ffe"
""",
        )
    )

    assert spec.campaign.worker_runtime == "spack"
    assert spec.campaign.flux_launch is True
    assert spec.campaign.flux_launch_cores == 6
    assert spec.campaign.spack_fluxion_prefix == "/opt/fluxion"
    assert spec.campaign.spack_faketime_prefix == "/opt/faketime"
    assert spec.campaign.spack_gcc_runtime == "/opt/gcc-runtime"
    assert spec.campaign.spack_view == "/opt/view"
    assert spec.campaign.spack_ninja_prefix == "/opt/ninja"
    assert spec.campaign.meson_pythonpath == "/opt/meson"
    assert spec.campaign.host_jobtap_cc == "gcc-12"
    assert spec.campaign.host_tmpdir == "/tmp/ffe"


def test_worker_script_has_spack_runtime_branch(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(
        _write_spec(
            tmp_path,
            trace=trace,
            config=config,
            scheduler=scheduler,
            campaign_extra="""
worker_runtime = "spack"
flux_launch = true
flux_launch_cores = 6
spack_fluxion_prefix = "/opt/fluxion"
""",
        )
    )
    path = ensure_worker_script(tmp_path, spec)
    text = path.read_text(encoding="utf-8")

    assert 'DEFAULT_WORKER_RUNTIME="spack"' in text
    assert 'DEFAULT_FLUX_LAUNCH="1"' in text
    assert 'DEFAULT_FLUX_LAUNCH_CORES="6"' in text
    assert 'SPACK_FLUXION_PREFIX="${SPACK_FLUXION_PREFIX:-/opt/fluxion}"' in text
    assert "run_spack_worker()" in text
    assert "write_spack_modprobe_overlay" in text
    assert "FLUX_MODPROBE_PATH_APPEND" in text
    assert "python3 -m mesonbuild.mesonmain setup" in text
    assert "FLUX_FICTION_JOBTAP_SO" in text
    assert "SPACK_VIEW}/lib" not in text
    assert "unset FLUX_URI FLUX_JOB_ID FLUX_KVS_NAMESPACE FLUX_INSTANCE_LEVEL" in text
    assert "python3 -m flux_fiction_ensemble worker" in text


def test_launcher_marks_terminal_flux_job_failed_without_resubmitting(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    state = root / "state.json"
    payload = json.loads(state.read_text(encoding="utf-8"))
    payload["submitted"]["b000001"] = {
        "queue": "pdebug",
        "fluxid": "f-dead",
        "submitted_at": 1,
    }
    state.write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign._flux_rows",
        lambda prefix, *, include_inactive=False: [
            {
                "id": "f-dead",
                "queue": "pdebug",
                "state": "INACTIVE",
                "result": "FAILED",
                "name": "ffe:unit-campaign:b000001",
            }
        ],
    )
    submitted = []

    def fake_submit(root, spec, batch_id, queue, *, dry_run):
        submitted.append(batch_id)
        return f"f-{batch_id}"

    monkeypatch.setattr("flux_fiction.ensemble.campaign._submit_batch", fake_submit)

    launcher_tick(root, spec)

    result = json.loads((root / "batches" / "b000001" / "batch_result.json").read_text(encoding="utf-8"))
    state_after = json.loads(state.read_text(encoding="utf-8"))
    assert result["state"] == "failed"
    assert result["flux_result"] == "FAILED"
    assert "b000001" not in state_after["submitted"]
    assert "b000001" not in submitted


def test_release_terminal_flux_allocation_retries_after_grace(tmp_path: Path, monkeypatch):
    """A cancel that does not take effect must be retried, not abandoned.

    The release is throttled by TERMINAL_RELEASE_GRACE_SECONDS rather than
    being strictly one-shot: a single lost cancel would otherwise leave the
    allocation held until walltime.
    """
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    batches = read_jsonl(root / "batches.jsonl")
    write_json(
        root / "batches" / "b000001" / "batch_result.json",
        {"state": "succeeded", "return_code": 0},
    )
    monkeypatch.setattr("flux_fiction.ensemble.campaign.TERMINAL_RELEASE_GRACE_SECONDS", 0.0)
    canceled = []
    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign._flux_python_cancel",
        lambda jobids, *, reason: canceled.extend(jobids) or len(jobids),
    )
    rows = {
        "b000001": {
            "id": "f-live",
            "queue": "pdebug",
            "state": "RUN",
            "result": "-",
            "name": "ffe:unit-campaign:b000001",
        }
    }
    state = {}

    released, changed = _release_terminal_flux_allocations(root, batches, rows, state, spec)

    assert released == 1
    assert changed is True
    assert canceled == ["f-live"]
    assert "b000001:f-live" in state["release_attempts"]

    # Grace of 0 means the next tick retries; a long grace suppresses it.
    released_again, _ = _release_terminal_flux_allocations(root, batches, rows, state, spec)
    assert released_again == 1

    monkeypatch.setattr("flux_fiction.ensemble.campaign.TERMINAL_RELEASE_GRACE_SECONDS", 900.0)
    released_throttled, changed_throttled = _release_terminal_flux_allocations(
        root, batches, rows, state, spec
    )
    assert released_throttled == 0
    assert changed_throttled is False


def test_run_launcher_uses_reactor_wait_between_ticks(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    write_json(
        status_path(root),
        {
            "active_flux_jobs": [
                {
                    "id": "f-live",
                    "queue": "pdebug",
                    "state": "RUN",
                    "result": "-",
                    "name": "ffe:unit-campaign:b000001",
                }
            ]
        },
    )

    tick_results = [False, True]
    waits = []

    def fake_tick(root_arg, spec_arg, *, dry_run=False):
        assert root_arg == root
        assert spec_arg == spec
        assert dry_run is False
        return tick_results.pop(0)

    def fake_wait(root_arg, *, since, timeout_seconds):
        waits.append((root_arg, since, timeout_seconds))
        return {
            "events": [{"id": "f-live", "name": "finish", "timestamp": 123.0}],
            "errors": [],
            "timed_out": False,
            "watched": 1,
        }

    monkeypatch.setattr("flux_fiction.ensemble.campaign.launcher_tick", fake_tick)
    monkeypatch.setattr("flux_fiction.ensemble.campaign._wait_for_flux_reactor_event", fake_wait)

    assert run_launcher(root, spec) == 0
    assert len(waits) == 1
    assert waits[0][0] == root
    assert waits[0][2] == spec.campaign.poll_interval_seconds


def test_reactor_wait_uses_persisted_event_cursors(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    write_json(
        status_path(root),
        {
            "active_flux_jobs": [
                {
                    "id": "f-live",
                    "queue": "pdebug",
                    "state": "RUN",
                    "result": "-",
                    "name": "ffe:unit-campaign:b000001",
                }
            ]
        },
    )
    state_path = root / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["submitted"] = {"b000002": {"fluxid": "f-submitted", "queue": "pdebug"}}
    state_path.write_text(json.dumps(state), encoding="utf-8")
    calls = []

    def fake_watch(jobids, *, since, timeout_seconds):
        calls.append((jobids, since, timeout_seconds))
        return {"events": [], "errors": [], "timed_out": True, "watched": 1}

    monkeypatch.setattr("flux_fiction.ensemble.campaign._flux_python_watch_job_events", fake_watch)

    _wait_for_flux_reactor_event(root, since=100.0, timeout_seconds=5.0)

    # Watches every job the campaign knows about -- those active in status.json
    # plus anything still recorded as submitted in state.json. No per-job
    # cursors: `since` is stamped before each tick, which already dedupes.
    assert calls == [(["f-live", "f-submitted"], 100.0, 5.0)]


def test_run_launcher_poll_mode_skips_reactor_wait(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    tick_results = [False, True]

    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign.launcher_tick",
        lambda root_arg, spec_arg, *, dry_run=False: tick_results.pop(0),
    )
    monkeypatch.setattr("time.sleep", lambda seconds: None)

    def fail_wait(*args, **kwargs):
        raise AssertionError("reactor wait should not run in poll mode")

    monkeypatch.setattr("flux_fiction.ensemble.campaign._wait_for_flux_reactor_event", fail_wait)

    assert run_launcher(root, spec, use_reactor=False) == 0


def test_campaign_infers_rabbit_capacity_from_resource_graph(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    resource = tmp_path / "resource.json"
    resource.write_text(
        json.dumps(
            {
                "graph": {
                    "nodes": [
                        {"id": "r0", "metadata": {"type": "rabbit"}},
                        {"id": "n0", "metadata": {"type": "node"}},
                        {"id": "s0", "metadata": {"type": "ssd", "size": 50}},
                        {"id": "s1", "metadata": {"type": "ssd", "size": 50}},
                    ],
                    "edges": [
                        {"source": "r0", "target": "n0"},
                        {"source": "r0", "target": "s0"},
                        {"source": "r0", "target": "s1"},
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    spec_path = _write_spec(
        tmp_path,
        trace=trace,
        config=config,
        scheduler=scheduler,
        rabbit_capacity=None,
        resource_file=resource,
    )

    spec = load_campaign_spec(spec_path)

    assert spec.rabbit.capacity_gib == 100


def _write_slurm_spec(tmp_path: Path, *, trace: Path, config: Path, scheduler: Path) -> Path:
    """Same campaign as _write_spec but submitting through Slurm."""
    base = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    text = base.read_text(encoding="utf-8").replace(
        "[submission]\nqueues = [\"pdebug\"]",
        "[submission]\nbackend = \"slurm\"\nqueues = [\"pdebug\"]",
    )
    spec = tmp_path / "campaign_slurm.toml"
    spec.write_text(text, encoding="utf-8")
    return spec


def test_slurm_backend_submits_one_single_node_job_per_batch(tmp_path: Path, monkeypatch):
    """The Slurm backend must mirror the Flux one: one -N1 job per batch.

    Not a single multi-node allocation -- batches have to queue and start
    independently, exactly as they do when handed to a system Flux instance.
    """
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(
        _write_slurm_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    )
    assert submission_backend(spec) == "slurm"

    root = initialize_campaign(spec)
    calls = []

    class _Proc:
        returncode = 0
        stdout = "987654;dane\n"
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return _Proc()

    monkeypatch.setattr("flux_fiction.ensemble.campaign.subprocess.run", fake_run)
    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign.query_job_rows",
        lambda spec_arg, prefix, ids, include_inactive=True: [],
    )

    jobid = _submit_batch(root, spec, "b000001", "pdebug", dry_run=False)

    assert jobid == "987654"  # --parsable "<id>;<cluster>" is stripped to the id
    assert len(calls) == 1
    argv = calls[0]
    assert argv[0] == "sbatch"
    assert "-N1" in argv and "--parsable" in argv and "--exclusive" in argv
    assert "--partition=pdebug" in argv
    assert any(a.startswith("--time=") for a in argv)
    wrap = next(a for a in argv if a.startswith("--wrap="))
    assert "b000001" in wrap and "worker.sh" in wrap
    # Launched via srun so the task gets the session rootless podman needs,
    # but still one single-node job -- never a multi-node allocation and
    # never an enclosing personal flux instance.
    assert "srun -N1 -n1" in wrap
    # all cores of the exclusive node, not one per task
    assert "--cpu-bind=none" in wrap
    assert not any(a.startswith("-N8") for a in argv)
    assert "flux start" not in wrap


def test_submit_batch_passes_spack_runtime_and_pinning_env(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(
        _write_spec(
            tmp_path,
            trace=trace,
            config=config,
            scheduler=scheduler,
            campaign_extra="""
worker_runtime = "spack"
flux_launch = true
flux_launch_cores = 6
spack_fluxion_prefix = "/opt/fluxion"
spack_faketime_prefix = "/opt/faketime"
spack_gcc_runtime = "/opt/gcc-runtime"
spack_view = "/opt/view"
spack_ninja_prefix = "/opt/ninja"
meson_pythonpath = "/opt/meson"
host_jobtap_cc = "gcc-12"
host_tmpdir = "/tmp/ffe"
""",
        )
    )
    root = initialize_campaign(spec)
    captured = {}

    def fake_submit(**kwargs):
        captured.update(kwargs)
        return "f-test"

    monkeypatch.setattr("flux_fiction.ensemble.campaign._flux_python_submit_batch", fake_submit)

    assert _submit_batch(root, spec, "b000001", "pdebug", dry_run=False) == "f-test"

    env = captured["env"]
    assert env["FLUX_FICTION_WORKER_RUNTIME"] == "spack"
    assert env["FLUX_FICTION_FLUX_LAUNCH"] == "1"
    assert env["FLUX_FICTION_FLUX_LAUNCH_CORES"] == "6"
    assert env["SPACK_FLUXION_PREFIX"] == "/opt/fluxion"
    assert env["SPACK_FAKETIME_PREFIX"] == "/opt/faketime"
    assert env["SPACK_GCC_RUNTIME"] == "/opt/gcc-runtime"
    assert env["SPACK_VIEW"] == "/opt/view"
    assert env["SPACK_NINJA_PREFIX"] == "/opt/ninja"
    assert env["MESON_PYTHONPATH"] == "/opt/meson"
    assert env["HOST_JOBTAP_CC"] == "gcc-12"
    assert env["FLUX_FICTION_HOST_TMPDIR"] == "/tmp/ffe"
    assert captured["worker_script"].name == "worker.sh"


def test_slurm_state_mapping_matches_flux_row_schema():
    assert _slurm_state_result("RUNNING") == ("RUNNING", "-")
    assert _slurm_state_result("PENDING") == ("PENDING", "-")
    assert _slurm_state_result("COMPLETED") == ("INACTIVE", "COMPLETED")
    assert _slurm_state_result("TIMEOUT") == ("INACTIVE", "TIMEOUT")
    # sacct decorates cancellations with the cancelling uid
    assert _slurm_state_result("CANCELLED by 12345") == ("INACTIVE", "CANCELLED")


def _shake_rows(count: int = 200) -> list[dict[str, str]]:
    """Rows spanning the shapes that broke naive relative shaking: a 1-node
    job that cannot shrink, a wide job, a 1-second job and a full-day job."""
    rows = []
    for index in range(count):
        nodes = [1, 2, 16, 128][index % 4]
        elapsed = ["00:00:01", "00:05:00", "06:00:00", "1-00:00:00"][index % 4]
        rows.append(
            {
                "JobID": str(index + 1),
                "NNodes": str(nodes),
                "NCPUS": str(nodes * 4),
                "NGPUS": "0",
                "Timelimit": "1-00:00:00",
                "Submit": f"2026-01-01T{index // 60:02d}:{index % 60:02d}:00",
                "Elapsed": elapsed,
            }
        )
    return rows


def _duration_seconds(value: str) -> float:
    from flux_fiction.ensemble.trace import _hms_to_seconds

    return _hms_to_seconds(value)


def test_shake_attributes_each_touch_only_their_own_field():
    rows = _shake_rows()
    kwargs = dict(job_percentage=50, degree_seconds=60, relative_degree_percent=10)

    def changed(out, field):
        return sum(1 for a, b in zip(rows, out) if a[field] != b[field])

    only_time = apply_shake(rows, seed=5, attributes=["interarrival"], **kwargs)
    assert changed(only_time, "Submit") > 0
    assert changed(only_time, "Elapsed") == 0
    assert changed(only_time, "NNodes") == 0

    only_runtime = apply_shake(rows, seed=5, attributes=["runtime"], **kwargs)
    assert changed(only_runtime, "Elapsed") > 0
    assert changed(only_runtime, "Submit") == 0
    assert changed(only_runtime, "NNodes") == 0

    only_nodes = apply_shake(rows, seed=5, attributes=["nodes"], **kwargs)
    assert changed(only_nodes, "NNodes") > 0
    assert changed(only_nodes, "Submit") == 0
    assert changed(only_nodes, "Elapsed") == 0


def test_shake_attribute_streams_are_independent_and_deterministic():
    rows = _shake_rows()
    kwargs = dict(job_percentage=50, degree_seconds=60, relative_degree_percent=10)

    both = apply_shake(rows, seed=11, attributes=["interarrival", "runtime", "nodes"], **kwargs)
    assert both == apply_shake(
        rows, seed=11, attributes=["interarrival", "runtime", "nodes"], **kwargs
    )
    # Listing order must not matter; the set of attributes is what counts.
    assert both == apply_shake(
        rows, seed=11, attributes=["nodes", "runtime", "interarrival"], **kwargs
    )
    # Adding attributes must not disturb which jobs the others perturbed.
    only_time = apply_shake(rows, seed=11, attributes=["interarrival"], **kwargs)
    assert [r["Submit"] for r in only_time] == [r["Submit"] for r in both]


def test_shake_preserves_request_invariants():
    rows = _shake_rows()
    out = apply_shake(
        rows,
        seed=3,
        attributes=["runtime", "nodes"],
        job_percentage=100,
        degree_seconds=0,
        relative_degree_percent=25,
        max_nodes=64,
    )
    for row in out:
        nodes = int(row["NNodes"])
        # A node request is never zero, never exceeds the machine, and keeps
        # its CPU count consistent with the per-node ratio.
        assert 1 <= nodes <= 64
        assert int(row["NCPUS"]) == 4 * nodes
        # A job may not run longer than the wall time it asked for.
        assert _duration_seconds(row["Elapsed"]) <= _duration_seconds(row["Timelimit"])
    # The cap is real: 128-node rows must have been pulled down to it.
    assert any(int(r["NNodes"]) == 64 for r in out)


def test_shake_runtime_magnitude_is_relative_to_each_job():
    rows = _shake_rows()
    out = apply_shake(
        rows,
        seed=9,
        attributes=["runtime"],
        job_percentage=100,
        degree_seconds=0,
        relative_degree_percent=10,
    )
    for before, after in zip(rows, out):
        original = _duration_seconds(before["Elapsed"])
        shaken = _duration_seconds(after["Elapsed"])
        # Within +/-10% of its own runtime (plus a second of formatting slack),
        # so a day-long job is not perturbed by the same absolute amount as a
        # one-second job.
        assert abs(shaken - original) <= original * 0.10 + 1.0


def test_unsupported_shake_attribute_is_rejected(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec_path = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    spec_path.write_text(
        spec_path.read_text(encoding="utf-8").replace(
            "job_percentage = 50", 'attributes = ["bogus"]\njob_percentage = 50'
        ),
        encoding="utf-8",
    )
    with pytest.raises(EnsembleConfigError):
        load_campaign_spec(spec_path)


def test_distribution_axis_expands_grid_and_shares_the_control(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec_path = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    spec_path.write_text(
        spec_path.read_text(encoding="utf-8").replace(
            "rabbit_ceiling_percentages = [0, 10]",
            'rabbit_ceiling_percentages = [0, 10]\n'
            'rabbit_distributions = ["gaussian", "pareto"]',
        ),
        encoding="utf-8",
    )
    spec = load_campaign_spec(spec_path)
    tasks = generate_tasks(spec)

    # 2 duplicates x 2 queue x 1 match x (1 shared control + 1 active cell per
    # distribution) = 12. The control is NOT duplicated per distribution: with
    # no requests written, the draw shape cannot change the trace.
    assert len(tasks) == 12
    controls = [t for t in tasks if not t["rabbit_active"]]
    assert len(controls) == 4
    active = [t for t in tasks if t["rabbit_active"]]
    assert sorted({t["rabbit_distribution"] for t in active}) == ["gaussian", "pareto"]
    assert len({t["task_id"] for t in tasks}) == len(tasks)
    assert all("__d" in t["task_id"] for t in tasks)


def test_single_distribution_keeps_legacy_task_ids(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec_path = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    tasks = generate_tasks(load_campaign_spec(spec_path))
    # No distribution suffix when there is nothing to disambiguate, so ids and
    # seeds match campaigns generated before the axis existed.
    assert all("__d" not in t["task_id"] for t in tasks)
    assert all(t["rabbit_distribution"] == "uniform" for t in tasks)


def test_per_task_distribution_reaches_the_generated_trace(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec_path = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    spec_path.write_text(
        spec_path.read_text(encoding="utf-8")
        .replace("slice_count = 4", "slice_count = 4")
        .replace(
            "rabbit_ceiling_percentages = [0, 10]",
            'rabbit_ceiling_percentages = [0, 100]\n'
            'rabbit_distributions = ["gaussian", "pareto"]',
        )
        .replace('distribution = "uniform"', 'distribution = "uniform"\ntail_min_gib = 100'),
        encoding="utf-8",
    )
    spec = load_campaign_spec(spec_path)
    tasks = [t for t in generate_tasks(spec) if t["rabbit_active"]]

    values: dict[str, list[float]] = {}
    for task in tasks:
        out_dir = tmp_path / "mat" / task["task_id"]
        materialize_task_inputs(spec, task, out_dir)
        rows = list(csv.DictReader((out_dir / "trace.csv").open(encoding="utf-8")))
        drawn = [float(r["RabbitGiB"]) for r in rows if float(r["RabbitGiB"]) > 0]
        values.setdefault(task["rabbit_distribution"], []).extend(drawn)

    # The pareto floor is honoured, which is only possible if the task's own
    # distribution (not the spec-level default) drove the draw.
    assert min(values["pareto"]) >= 100
    assert set(values) == {"gaussian", "pareto"}


def test_unsupported_distribution_in_grid_is_rejected(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec_path = _write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler)
    spec_path.write_text(
        spec_path.read_text(encoding="utf-8").replace(
            "rabbit_ceiling_percentages = [0, 10]",
            'rabbit_ceiling_percentages = [0, 10]\nrabbit_distributions = ["bogus"]',
        ),
        encoding="utf-8",
    )
    with pytest.raises(EnsembleConfigError):
        load_campaign_spec(spec_path)


def _fail_batch(root: Path, batch_id: str, *, summaries: int = 0, flux_result: str = ""):
    bdir = root / "batches" / batch_id
    bdir.mkdir(parents=True, exist_ok=True)
    write_json(bdir / "batch_result.json", {
        "batch_id": batch_id, "state": "failed", "return_code": 1,
        "flux_result": flux_result, "failure_reason": "boom",
    })
    for i in range(summaries):
        child = bdir / "parallel" / "ts_manifest" / "runs" / f"{i:04d}_t" / "child"
        child.mkdir(parents=True, exist_ok=True)
        (child / "summary.json").write_text("{}", encoding="utf-8")


def test_retry_failed_requeues_only_total_losses(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    batches = read_jsonl(batches_path(root))
    dead, partial = batches[0]["batch_id"], batches[1]["batch_id"]

    _fail_batch(root, dead, summaries=0)
    # A batch killed at its walltime is ALSO recorded state=failed, but its
    # children finalized -- requeueing it would discard real metrics.
    _fail_batch(root, partial, summaries=3, flux_result="TIMEOUT")
    write_json(state_path(root), {"submitted": {dead: {"fluxid": "f1"},
                                                partial: {"fluxid": "f2"}},
                                  "submission_history": []})

    report = {r["batch_id"]: r for r in failed_batch_report(root)}
    assert report[dead]["total_loss"] is True
    assert report[partial]["total_loss"] is False

    preview = retry_failed_batches(root, dry_run=True)
    assert [p["batch_id"] for p in preview] == [dead]
    assert (root / "batches" / dead / "batch_result.json").exists(), "dry run must not mutate"

    done = retry_failed_batches(root)
    assert [d["batch_id"] for d in done] == [dead]
    # Requeued: terminal marker gone and submission record dropped, which is
    # exactly what makes _batch_state report "queued" again.
    assert not (root / "batches" / dead / "batch_result.json").exists()
    assert (root / "batches" / partial / "batch_result.json").exists()
    submitted = read_json(state_path(root))["submitted"]
    assert dead not in submitted and partial in submitted


def test_retry_failed_can_include_partial_and_target_one_batch(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    batches = read_jsonl(batches_path(root))
    a, b = batches[0]["batch_id"], batches[1]["batch_id"]
    _fail_batch(root, a, summaries=2)
    _fail_batch(root, b, summaries=2)
    write_json(state_path(root), {"submitted": {}, "submission_history": []})

    assert retry_failed_batches(root, dry_run=True) == []          # both partial
    only_a = retry_failed_batches(root, batch_ids=[a], include_partial=True)
    assert [x["batch_id"] for x in only_a] == [a]
    assert not (root / "batches" / a / "batch_result.json").exists()
    assert (root / "batches" / b / "batch_result.json").exists()


def test_requeued_batch_is_resubmitted_by_the_next_tick(tmp_path: Path, monkeypatch):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    dead = read_jsonl(batches_path(root))[0]["batch_id"]
    _fail_batch(root, dead, summaries=0)
    write_json(state_path(root), {"submitted": {dead: {"fluxid": "f1", "submitted_at": 0}},
                                  "submission_history": []})

    monkeypatch.setattr("flux_fiction.ensemble.campaign.query_job_rows", lambda *a, **k: [])
    submits: list[str] = []
    monkeypatch.setattr(
        "flux_fiction.ensemble.campaign._submit_batch",
        lambda root, spec, batch_id, queue, dry_run=False: submits.append(batch_id) or "fX",
    )
    retry_failed_batches(root)
    launcher_tick(root, spec)
    assert dead in submits


def _write_attempt(root: Path, batch_id: str, manifest: str, task_id: str, *,
                   state: str, jobs_completed: int, updated_at: str) -> Path:
    """Write one child status.json under batches/<b>/parallel/<manifest>/runs/..."""
    child = (root / "batches" / batch_id / "parallel" / manifest / "runs"
             / f"0001_{task_id}" / "child")
    child.mkdir(parents=True, exist_ok=True)
    write_json(child / "status.json", {
        "state": state,
        "jobs_completed": jobs_completed,
        "jobs_submitted": jobs_completed,
        "jobs_total": 100,
        "updated_at": updated_at,
    })
    return child / "status.json"


def test_requeued_task_progress_ignores_the_faketime_stamp(tmp_path: Path):
    """The live attempt must win even though the dead one looks newer.

    A child runs under libfaketime and stamps simulated time (here 2026-01),
    while the terminal record for a killed child is written by the parent
    runner in real time (2026-07). Ordering attempts on updated_at therefore
    always picks the dead attempt, which froze corona's reported progress at
    its pre-requeue counts.
    """
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    batch = read_jsonl(batches_path(root))[0]
    batch_id = batch["batch_id"]
    task_id = batch["task_ids"][0]

    _write_attempt(root, batch_id, "20260726_115313_manifest", task_id,
                   state="failed", jobs_completed=51, updated_at="2026-07-26T21:27:58Z")
    live = _write_attempt(root, batch_id, "20260726_213415_manifest", task_id,
                          state="running", jobs_completed=70, updated_at="2026-01-21T10:18:17Z")
    # Real mtimes agree with launch order regardless of what the files claim.
    import os
    os.utime(live, (2_000_000_000, 2_000_000_000))

    progress = _collect_task_progress(root, [batch])
    assert progress[task_id]["status"] == "running"
    assert progress[task_id]["jobs_completed"] == 70


def test_results_csv_prefers_the_newest_attempts_summary(tmp_path: Path):
    """Two attempts both wrote a parallel_summary; the later one is the result."""
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    batch = read_jsonl(batches_path(root))[0]
    batch_id, task_id = batch["batch_id"], batch["task_ids"][0]

    for manifest, completed in (("20260726_115313_manifest", 51),
                                ("20260726_213415_manifest", 100)):
        d = root / "batches" / batch_id / "parallel" / manifest
        d.mkdir(parents=True, exist_ok=True)
        write_json(d / "parallel_summary.json", {"runs": [
            {"name": task_id, "state": "succeeded", "jobs_completed": completed,
             "jobs_total": 100},
        ]})

    rows = {r["task_id"]: r for r in csv.DictReader(
        write_results_csv(root, spec=spec).open(encoding="utf-8"))}
    assert rows[task_id]["jobs_completed"] == "100"
