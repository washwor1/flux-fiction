from __future__ import annotations

import importlib.util
import sys
import types

import pytest

try:
    _tqdm_missing = importlib.util.find_spec("tqdm") is None
except ValueError:
    _tqdm_missing = "tqdm" not in sys.modules

if _tqdm_missing:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._core import events
from flux_fiction._core import faketime as faketime_module
from flux_fiction._core import models
from flux_fiction._core.engine import Simulation
from flux_fiction._core.faketime import FakeTimeController, _parse_relative_offset


class ManualClock:
    def __init__(self, now: float):
        self.now = float(now)

    def __call__(self) -> float:
        return self.now


def read_offset(path) -> float:
    return _parse_relative_offset(path.read_text())


def test_faketime_controller_seeds_relative_offset(tmp_path):
    stamp = tmp_path / "faketime_stamp"
    real_clock = ManualClock(1000.0)
    fake_clock = ManualClock(100.0)

    controller = FakeTimeController(
        stamp,
        initial_epoch=100.0,
        real_time=real_clock,
        fake_time=fake_clock,
        seed=True,
    )

    assert controller.current_effective_time() == 100.0
    assert read_offset(stamp) == -900.0


def test_faketime_controller_jumps_forward_only(tmp_path):
    stamp = tmp_path / "faketime_stamp"
    stamp.write_text("-900.000000000s\n")
    fake_clock = ManualClock(100.05)
    controller = FakeTimeController(
        stamp,
        initial_epoch=100.0,
        fake_time=fake_clock,
        seed=False,
        near_event_threshold=0.5,
    )

    skipped = controller.advance_to(0.01)
    assert skipped.action == "overrun"
    assert read_offset(stamp) == -900.0

    jumped = controller.advance_to(1.0)
    assert jumped.action == "jumped"
    assert read_offset(stamp) == -899.05


def test_faketime_controller_can_adopt_existing_relative_offset(tmp_path):
    stamp = tmp_path / "faketime_stamp"
    stamp.write_text("+12.500000000s\n")
    fake_clock = ManualClock(1012.5)

    controller = FakeTimeController(stamp, fake_time=fake_clock, seed=False)

    assert controller.current_effective_time() == 1012.5


def test_shared_clock_uses_integer_targets_without_timestamp_file(tmp_path):
    stamp = tmp_path / "unused_stamp"
    updates = []
    fake_clock = ManualClock(100.0)

    controller = FakeTimeController(
        stamp,
        initial_epoch=100.25,
        fake_time=fake_clock,
        shared_clock=True,
        shared_clock_setter=updates.append,
        seed=True,
        near_event_threshold=0.0,
    )
    assert updates == [100_250_000_000]
    assert not stamp.exists()

    fake_clock.now = 100.5
    decision = controller.advance_to(1.0)
    assert decision.action == "jumped"
    assert updates[-1] == 101_250_000_000
    assert not stamp.exists()

    fake_clock.now = 101.5
    repeated = controller.advance_to(2.0)
    assert repeated.action == "jumped"
    assert updates[-1] == 102_250_000_000
    assert len(updates) == 3
    assert not stamp.exists()


def test_shared_clock_rejects_unavailable_library(tmp_path, monkeypatch):
    def unavailable():
        raise RuntimeError("shared state is inactive")

    monkeypatch.setattr(faketime_module, "_load_shared_clock", unavailable)
    with pytest.raises(RuntimeError, match="shared state is inactive"):
        FakeTimeController(tmp_path / "unused", shared_clock=True, seed=False)


def test_shared_clock_preserves_forward_only_policy(tmp_path):
    updates = []
    fake_clock = ManualClock(105.0)
    controller = FakeTimeController(
        tmp_path / "unused",
        initial_epoch=100.0,
        fake_time=fake_clock,
        shared_clock=True,
        shared_clock_setter=updates.append,
        seed=False,
        near_event_threshold=0.0,
    )

    decision = controller.advance_to(1.0)
    assert decision.action == "overrun"
    assert updates == []


def test_faketime_controller_waits_for_near_event(tmp_path):
    stamp = tmp_path / "faketime_stamp"
    stamp.write_text("+0.000000000s\n")
    fake_clock = ManualClock(100.5)
    controller = FakeTimeController(
        stamp,
        initial_epoch=100.0,
        fake_time=fake_clock,
        seed=False,
        near_event_threshold=1.0,
    )

    def tick():
        fake_clock.now = 101.0

    original_sleep = __import__("time").sleep
    try:
        __import__("time").sleep = lambda _delay: tick()
        waited = controller.advance_to(1.0)
    finally:
        __import__("time").sleep = original_sleep

    assert waited.action == "waited"
    assert read_offset(stamp) == 0.0


def test_simulation_advances_faketime_before_event_callbacks(tmp_path):
    calls = []

    class Controller:
        def advance_to(self, simulation_time):
            calls.append(("advance", simulation_time))

    class Adapter:
        def get_kvs_stats(self):
            return {}

        def accumulate_quiescent(self, payload):
            calls.append(("accumulate", payload))

        def query_quiescent(self, payload, return_cb):
            calls.append(("quiescent", payload))

    def event_cb():
        calls.append(("event", None))

    event_list = events.EventList()
    event_list.add_event(42.0, event_cb)
    simulation = Simulation(
        Adapter(),
        event_list,
        {},
        output_dir=str(tmp_path) + "/",
        faketime_controller=Controller(),
    )

    simulation.advance()

    assert calls[0] == ("advance", 42.0)
    assert calls[1] == ("event", None)


def test_simulation_keeps_logical_start_separate_from_observed_faketime(tmp_path):
    class Controller:
        def current_effective_time(self):
            return 123.456789

    class Adapter:
        def ack_start(self, jobid):
            pass

    job = models.Job(
        nnodes=1,
        ncpus=1,
        submit_time=100.0,
        elapsed_time=10.0,
        timelimit=20.0,
        gap=0.25,
    )
    job._jobid = "job1"
    job.record_state_transition("SUBMITTED", 100.0)

    simulation = Simulation(
        Adapter(),
        events.EventList(),
        {"job1": job},
        output_dir=str(tmp_path) + "/",
        faketime_controller=Controller(),
    )
    simulation.current_time = 200.0

    simulation.start_job("job1")

    assert job.start_time == 200.0
    assert job.state_transitions["STARTED"] == 200.0
    assert job.complete_time == pytest.approx(210.0)
    assert job.flux_observed_start == 123.456789
