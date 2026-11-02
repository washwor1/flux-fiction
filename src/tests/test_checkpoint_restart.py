from __future__ import annotations

import csv
import importlib.util
import json
import random
import sys
import types
from dataclasses import replace
from pathlib import Path

import pytest

try:
    _tqdm_missing = importlib.util.find_spec("tqdm") is None
except ValueError:
    _tqdm_missing = "tqdm" not in sys.modules

if _tqdm_missing:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._adapters.mock.adapter import MockAdapter
from flux_fiction._core import checkpoint
from flux_fiction.api import client
from flux_fiction.api.config import ExperimentConfig

# Simulator-owned columns only: REAL_* and FLUX_OBSERVED_START are wall/fake
# clock observations and differ between any two runs.
_COMPARED = ("SUBMIT", "START", "FINISH", "NODELIST")


def _write_trace(path: Path, njobs: int = 40, seed: int = 7) -> None:
    rng = random.Random(seed)
    rows = ["JobID,NNodes,NCPUS,Timelimit,Submit,Elapsed"]
    submit = 0
    for i in range(njobs):
        submit += rng.choice([0, 0, 30, 60, 120])
        nnodes = rng.choice([1, 1, 2, 3, 4, 6, 8])
        elapsed = rng.randint(60, 900)
        rows.append(
            "{},{},{},00:20:00,2019-01-01T{:02d}:{:02d}:{:02d},00:{:02d}:{:02d}".format(
                i + 1, nnodes, nnodes, submit // 3600, (submit // 60) % 60, submit % 60,
                elapsed // 60, elapsed % 60,
            )
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _config(tmp_path: Path, trace: Path, name: str, **extra) -> ExperimentConfig:
    out = tmp_path / name / "output"
    out.mkdir(parents=True)
    return ExperimentConfig(
        job_traces=str(trace),
        nnodes=8,
        ncpus=1,
        ngpus=0,
        backend="mock",
        quiet=True,
        output_dir=str(out) + "/",
        summary_file=str(tmp_path / name / "summary.json"),
        **extra,
    )


def _transitions(cfg: ExperimentConfig) -> dict[str, tuple]:
    with open(Path(cfg.output_dir) / "job_transitions.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    return {row["trace_idx"]: tuple(row[c] for c in _COMPARED) + (row["jobid"],) for row in rows}


def _segments(tmp_path, trace, name, cuts):
    """Run the trace as len(cuts)+1 segments, checkpointing after each cut."""
    ckpt_dir = tmp_path / name / "ckpt"
    restore = None
    for seg, cut in enumerate(list(cuts) + [None]):
        extra = {}
        if cut is not None:
            extra.update(
                checkpoint_file=str(ckpt_dir / f"state-{seg + 1}.json"),
                checkpoint_after_completions=cut,
            )
        if restore is not None:
            extra["restore_file"] = restore
        cfg = _config(tmp_path, trace, f"{name}-seg{seg}", **extra)
        result = client.run_experiment(cfg, adapter=MockAdapter())
        assert result.ok, result.message
        if cut is None:
            assert not result.checkpointed
            return cfg
        assert result.checkpointed, result.message
        restore = cfg.checkpoint_file


def test_restored_run_reproduces_uninterrupted_schedule(tmp_path: Path):
    trace = tmp_path / "trace.csv"
    _write_trace(trace)

    control = _config(tmp_path, trace, "control")
    assert client.run_experiment(control, adapter=MockAdapter()).ok

    once = _segments(tmp_path, trace, "once", cuts=[12])
    twice = _segments(tmp_path, trace, "twice", cuts=[9, 25])

    expected = _transitions(control)
    assert len(expected) == 40
    assert _transitions(once) == expected
    assert _transitions(twice) == expected

    control_summary = json.loads(Path(control.summary_file).read_text())
    for cfg in (once, twice):
        summary = json.loads(Path(cfg.summary_file).read_text())
        assert summary["jobs_completed"] == 40
        assert summary["makespan_seconds"] == control_summary["makespan_seconds"]


def test_checkpoint_records_running_and_pending_jobs(tmp_path: Path):
    trace = tmp_path / "trace.csv"
    _write_trace(trace)
    cfg = _config(
        tmp_path, trace, "seg0",
        checkpoint_file=str(tmp_path / "state.json"),
        checkpoint_after_completions=10,
    )
    result = client.run_experiment(cfg, adapter=MockAdapter())
    assert result.checkpointed

    state = checkpoint.load_state(cfg.checkpoint_file)
    assert state["sim"]["num_complete"] >= 10
    assert state["running_jobids"], "a mid-run cut should leave jobs running"
    assert state["pending_jobids"], "a loaded cut should leave jobs queued"
    by_id = {r["jobid"]: r for r in state["jobs"]}
    for jobid in state["running_jobids"]:
        assert "STARTED" in by_id[jobid]["state_transitions"]
        assert "INACTIVE" not in by_id[jobid]["state_transitions"]
    assert state["next_event_time"] > state["sim"]["current_time"]


def test_restore_rejects_a_different_trace(tmp_path: Path):
    trace = tmp_path / "trace.csv"
    _write_trace(trace)
    cfg = _config(
        tmp_path, trace, "seg0",
        checkpoint_file=str(tmp_path / "state.json"),
        checkpoint_after_completions=10,
    )
    assert client.run_experiment(cfg, adapter=MockAdapter()).checkpointed

    other = tmp_path / "other.csv"
    _write_trace(other, seed=8)
    resumed = _config(tmp_path, other, "seg1", restore_file=cfg.checkpoint_file)
    with pytest.raises(checkpoint.CheckpointError, match="job_traces_sha256"):
        client.run_experiment(resumed, adapter=MockAdapter())
