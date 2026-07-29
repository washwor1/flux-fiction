"""Asynchronous job submission must be observationally identical to blocking.

The whole safety argument for pipelining submissions is one barrier: every job
id is collected before the scheduler is told anything. These tests pin that
barrier down and check that the two paths produce the same job stream.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from flux_fiction._adapters.mock.adapter import MockAdapter
from flux_fiction._core import events, models
from flux_fiction._core.engine import Simulation


def _job(nnodes: int = 1, submit_time: float = 0.0) -> models.Job:
    return models.Job(
        nnodes=nnodes,
        ncpus=nnodes * 4,
        submit_time=submit_time,
        elapsed_time=60.0,
        timelimit=120.0,
    )


def _simulation(adapter, *, async_submit: bool) -> Simulation:
    sim = Simulation(
        adapter,
        events.EventList(),
        {},
        async_submit=async_submit,
        output_dir=None,
    )
    adapter.open(sim)
    adapter.install_resources(SimpleNamespace(nnodes=8, ncpus=32, ngpus=0))
    return sim


class _OrderSpy(MockAdapter):
    """Records the order of submit vs scheduler-facing calls."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[str] = []

    def submit_job(self, jobspec) -> int:
        self.calls.append("submit")
        return super().submit_job(jobspec)

    def submit_job_async(self, jobspec):
        self.calls.append("submit_start")
        return super().submit_job_async(jobspec)

    def submit_get_id(self, handle) -> int:
        self.calls.append("submit_resolve")
        return super().submit_get_id(handle)

    def accumulate_quiescent(self, payload):
        self.calls.append("accumulate")
        return super().accumulate_quiescent(payload)

    def query_quiescent(self, payload, return_cb):
        self.calls.append("quiescent")
        return super().query_quiescent(payload, return_cb)


def test_async_submit_defers_the_job_id_until_the_drain():
    adapter = _OrderSpy()
    sim = _simulation(adapter, async_submit=True)
    assert sim.async_submit is True

    jobs = [_job(submit_time=float(i)) for i in range(4)]
    for job in jobs:
        sim.submit_job(job)

    # Ids are not known yet, so nothing may have landed in the job map.
    assert sim.job_map == {}
    assert len(sim._pending_submits) == 4
    assert sim.num_submits == 4

    drained = sim._drain_pending_submits()
    assert drained == 4
    assert sim._pending_submits == []
    assert len(sim.job_map) == 4
    assert all(job.jobid in sim.job_map for job in jobs)


def test_quiescence_never_runs_while_a_submission_is_in_flight():
    adapter = _OrderSpy()
    sim = _simulation(adapter, async_submit=True)
    for i in range(3):
        sim.submit_job(_job(submit_time=float(i)))

    sim._send_quiescence_probe()

    # This is the invariant that makes the optimization safe: every id is
    # collected before the first scheduler-facing call.
    assert "submit_resolve" in adapter.calls
    last_resolve = max(i for i, c in enumerate(adapter.calls) if c == "submit_resolve")
    scheduler_calls = [
        i for i, c in enumerate(adapter.calls) if c in {"accumulate", "quiescent"}
    ]
    assert scheduler_calls, "expected the probe to reach the adapter"
    assert min(scheduler_calls) > last_resolve


def test_async_and_blocking_paths_produce_the_same_job_stream():
    streams = {}
    for mode in (False, True):
        adapter = _OrderSpy()
        sim = _simulation(adapter, async_submit=mode)
        for i in range(6):
            sim.submit_job(_job(nnodes=1 + (i % 3), submit_time=float(i)))
        sim._drain_pending_submits()
        streams[mode] = {
            "submit_order": list(adapter.submitted),
            "job_map": sorted(sim.job_map),
            "num_submits": sim.num_submits,
        }

    assert streams[False]["submit_order"] == streams[True]["submit_order"]
    assert streams[False]["job_map"] == streams[True]["job_map"]
    assert streams[False]["num_submits"] == streams[True]["num_submits"]


def test_async_submit_falls_back_when_the_adapter_cannot_pipeline():
    class _NoAsync(MockAdapter):
        def supports_async_submit(self) -> bool:
            return False

    adapter = _NoAsync()
    sim = _simulation(adapter, async_submit=True)
    # Requested but unsupported: the engine must quietly use the blocking path
    # rather than calling a method that raises NotImplementedError.
    assert sim.async_submit is False
    sim.submit_job(_job())
    assert len(sim.job_map) == 1
    assert sim._pending_submits == []


def test_job_resolve_submit_is_idempotent_and_returns_the_id():
    adapter = MockAdapter()
    adapter.open(SimpleNamespace(job_map={}, start_job=lambda *_: None))
    job = _job()
    job.submit_async(adapter)
    first = job.resolve_submit(adapter)
    assert first == job.jobid
    # A second drain must not re-submit or lose the id.
    assert job.resolve_submit(adapter) == first


def test_base_adapter_rejects_async_submit_by_default():
    from flux_fiction._adapters.base import Adapter

    class _Bare(Adapter):
        pass

    bare = _Bare()
    assert bare.supports_async_submit() is False
    with pytest.raises(NotImplementedError):
        bare.submit_job_async(json.dumps({}))
    with pytest.raises(NotImplementedError):
        bare.submit_get_id(object())


def test_unstarted_job_does_not_break_the_summary_walk():
    """An early finalize ends the run with jobs still queued.

    The summary walks every job in the map and reads queue_wait, which is only
    assigned when a job STARTS. A job that never started must still carry the
    attribute, or the walk raises AttributeError and the run dies before
    summary.json is written -- observed on all 18 tasks of the 2026-07-26
    calibration, which produced every per-job CSV and no summary.
    """
    queued = _job()
    assert queued.queue_wait is None

    started = _job()
    started.queue_wait = 12.5

    # Mirrors the summary block in engine.run().
    waits = [float(j.queue_wait) for j in (queued, started) if j.queue_wait is not None]
    assert waits == [12.5]
