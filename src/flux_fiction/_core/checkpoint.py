"""
Logical checkpoint/restart of a simulation.

A checkpoint is taken only at a quiescent boundary: every event at or before
``current_time`` has run, the scheduler has settled, and every allocation it
made has been started and acknowledged. At that point the simulator's state
reduces to plain data:

- the logical clock and the epoch/time-step counters,
- one record per submitted job (Flux id, transitions, start time),
- the validator's accounting and the KVS samples.

The event list is not serialized. It only ever holds two kinds of event --
``submit`` for every trace job (inserted in trace order before the run starts)
and ``complete`` for every started job (appended in start order) -- so it is
rebuilt from the job records: unsubmitted jobs get their submit event back and
running jobs get their completion event back, submits before completions within
each timestamp, completions in their original start order. That reproduces the
uninterrupted run's equal-time ordering exactly.

The Flux side (KVS, job-manager state, allocations) is persisted separately by
the instance's own restart machinery; see ``flux_fiction.cli.run_ff``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any

import flux_fiction._core.models as models
from flux_fiction.api.status import utcnow_iso

logger = logging.getLogger(__name__)

CHECKPOINT_FORMAT = "flux-fiction-checkpoint"
CHECKPOINT_VERSION = 1

# Exit status of the simulation process (and of flux-fiction-run) when it
# stopped at a checkpoint rather than finishing. EX_TEMPFAIL: try again later.
CHECKPOINT_EXIT_CODE = 75

_SIM_COUNTERS = (
    "time_step",
    "current_epoch",
    "latest_mutating_epoch",
    "last_quiescent_epoch",
    "stale_quiescence_replies",
    "num_submits",
    "num_started",
    "num_complete",
    "start_seq",
)

_JOB_FIELDS = (
    "start_time",
    "start_seq",
    "queue_wait",
    "flux_observed_start",
    "real_submit",
    "real_start",
    "real_finish",
)


class CheckpointError(RuntimeError):
    pass


def file_sha256(path: str | os.PathLike[str] | None) -> str | None:
    if not path:
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def config_fingerprint(config: object) -> dict[str, Any]:
    """Inputs that must be identical for a restored run to be the same experiment."""
    resource_path = getattr(config, "resource_R", None) or getattr(config, "resource_file", None)
    return {
        "job_traces_sha256": file_sha256(getattr(config, "job_traces", None)),
        "config_json_sha256": file_sha256(getattr(config, "config_json", None)),
        "resource_sha256": file_sha256(resource_path),
        "raw_jobspec_sha256": file_sha256(getattr(config, "raw_jobspec_file", None)),
        "omit_core_resources": bool(getattr(config, "omit_core_resources", False)),
        "exclusive": bool(getattr(config, "exclusive", False)),
        "batch_job_starts": bool(getattr(config, "batch_job_starts", True)),
        "account_system_latency": bool(getattr(config, "account_system_latency", True)),
        "async_submit": bool(getattr(config, "async_submit", False)),
        "quiescent_accumulation_window": float(
            getattr(config, "quiescent_accumulation_window", 0.0) or 0.0
        ),
        "fluxion_variant": getattr(config, "fluxion_variant", None),
    }


def _job_record(job: models.Job) -> dict[str, Any]:
    record = {
        "trace_index": job.trace_index,
        "jobid": job.jobid,
        "state_transitions": dict(job.state_transitions),
    }
    for key in _JOB_FIELDS:
        record[key] = getattr(job, key, None)
    return record


def capture(simulation, *, config: object, reason: str, generation: int,
            exec_validator=None, backend_state: dict | None = None) -> dict[str, Any]:
    """Serialize a simulation that is stopped at a quiescent boundary."""
    if simulation._pending_submits:
        raise CheckpointError(
            "cannot checkpoint with {} submissions in flight".format(len(simulation._pending_submits))
        )
    pending_starts = getattr(simulation.adapter, "pending_start_jobids", lambda: [])()
    if pending_starts:
        raise CheckpointError(
            "cannot checkpoint with allocated but unstarted jobs: {}".format(sorted(pending_starts))
        )
    expect = simulation.pending_quiescent_expect
    if expect["submits"] or expect["finishes"]:
        raise CheckpointError("cannot checkpoint with unflushed quiescence expectations")

    jobs = []
    running = []
    pending = []
    for jobid, job in simulation.job_map.items():
        jobs.append(_job_record(job))
        transitions = job.state_transitions
        if "INACTIVE" in transitions:
            continue
        if "STARTED" in transitions:
            running.append(jobid)
        else:
            pending.append(jobid)
    jobs.sort(key=lambda r: r["trace_index"])

    fake_now = None
    if simulation.faketime_controller is not None:
        fake_now = float(simulation.faketime_controller.current_effective_time())

    next_event = simulation.event_list.min()
    state = {
        "format": CHECKPOINT_FORMAT,
        "version": CHECKPOINT_VERSION,
        "generation": int(generation),
        "created_at": utcnow_iso(),
        "reason": reason,
        "fingerprint": config_fingerprint(config),
        "jobs_total": int(simulation.jobs_total),
        "sim": {
            "current_time": float(simulation.current_time),
            **{key: int(getattr(simulation, key)) for key in _SIM_COUNTERS},
        },
        "next_event_time": None if next_event is None else float(next_event[0]),
        "faketime_effective": fake_now,
        "jobs": jobs,
        "running_jobids": running,
        "pending_jobids": pending,
        "kvs_samples": list(simulation.kvs_samples),
        "exec_validator": exec_validator.to_state() if exec_validator is not None else None,
        "backend": backend_state,
    }
    return state


def write_state(path: str | os.PathLike[str], state: dict[str, Any]) -> Path:
    """Write the state file atomically; a torn checkpoint must never look complete."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return path


def load_state(path: str | os.PathLike[str]) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        state = json.load(f)
    if state.get("format") != CHECKPOINT_FORMAT:
        raise CheckpointError(f"{path} is not a flux-fiction checkpoint")
    if state.get("version") != CHECKPOINT_VERSION:
        raise CheckpointError(
            "{} has checkpoint version {}; this build reads version {}".format(
                path, state.get("version"), CHECKPOINT_VERSION
            )
        )
    return state


def verify_fingerprint(state: dict[str, Any], config: object) -> None:
    saved = state.get("fingerprint") or {}
    current = config_fingerprint(config)
    mismatched = sorted(
        key for key in set(saved) | set(current) if saved.get(key) != current.get(key)
    )
    if mismatched:
        details = ", ".join(
            "{}: checkpoint={!r} now={!r}".format(key, saved.get(key), current.get(key))
            for key in mismatched
        )
        raise CheckpointError("checkpoint does not match this configuration ({})".format(details))


def restore(simulation, jobs: list[models.Job], state: dict[str, Any], *, exec_validator=None) -> None:
    """
    Overlay checkpointed state onto freshly prepared trace jobs and rebuild the
    event list. ``jobs`` must be the trace in reader order with jobspecs set,
    exactly as a fresh run would prepare it.
    """
    if int(state["jobs_total"]) != len(jobs):
        raise CheckpointError(
            "checkpoint has {} trace jobs, trace now has {}".format(state["jobs_total"], len(jobs))
        )
    by_index = {job.trace_index: job for job in jobs}

    for record in state["jobs"]:
        job = by_index.get(record["trace_index"])
        if job is None:
            raise CheckpointError("checkpoint job trace_index={} not in trace".format(record["trace_index"]))
        job._jobid = record["jobid"]
        job.state_transitions = dict(record["state_transitions"])
        for key in _JOB_FIELDS:
            setattr(job, key, record.get(key))
        simulation.job_map[job.jobid] = job

    sim = state["sim"]
    simulation.current_time = float(sim["current_time"])
    for key in _SIM_COUNTERS:
        setattr(simulation, key, int(sim[key]))
    simulation.jobs_total = len(jobs)
    simulation.kvs_samples = list(state.get("kvs_samples") or [])
    simulation.restored_time_step = simulation.time_step
    if exec_validator is not None and state.get("exec_validator"):
        exec_validator.load_state(state["exec_validator"])

    # Submits first, in trace order: that is the order a fresh run inserts them.
    for job in jobs:
        if "SUBMITTED" not in job.state_transitions:
            job.insert_apriori_events(simulation)
    # Then completions, in the order the jobs originally started.
    running = [
        job for job in jobs
        if "STARTED" in job.state_transitions and "INACTIVE" not in job.state_transitions
    ]
    running.sort(key=lambda job: job.start_seq)
    for job in running:
        simulation.schedule_completion(job)

    # Events at or before current_time already ran; keep the list's notion of
    # "now" so late insertions are still reported.
    simulation.event_list._current_time = simulation.current_time

    expected_running = set(state.get("running_jobids") or [])
    if {job.jobid for job in running} != expected_running:
        raise CheckpointError("running job set in checkpoint is inconsistent with job records")
    logger.info(
        "Restored checkpoint generation %s at sim_time=%s: %d submitted, %d running, %d complete",
        state.get("generation"),
        simulation.current_time,
        simulation.num_submits,
        len(running),
        simulation.num_complete,
    )
