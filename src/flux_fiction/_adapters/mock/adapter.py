from __future__ import annotations

import itertools
import json
import logging
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MockQuiescenceStep:
    flush_starts: bool = True
    delay_reactor_ticks: int = 0
    max_starts: int | None = None


@dataclass
class MockQuiescenceConfig:
    default_flush_starts: bool = True
    default_delay_reactor_ticks: int = 0
    default_max_starts: int | None = None
    script: tuple[MockQuiescenceStep, ...] = ()


@dataclass
class MockResourceConfig:
    jobspec_shape: dict[str, Any] = field(
        default_factory=lambda: {"intermediate_types": [], "intermediate_counts": {}}
    )
    leaf_resources: dict[str, Any] = field(default_factory=dict)
    rabbit_storage: dict[str, Any] = field(default_factory=dict)


@dataclass
class MockDiagnosticsConfig:
    include_kvs: bool = False
    include_queue_status: bool = False
    use_simulated_timestamps: bool = True


@dataclass
class MockScenario:
    scheduling_policy: str = "fcfs"
    max_starts_per_query: int | None = None
    quiescence: MockQuiescenceConfig = field(default_factory=MockQuiescenceConfig)
    resources: MockResourceConfig = field(default_factory=MockResourceConfig)
    diagnostics: MockDiagnosticsConfig = field(default_factory=MockDiagnosticsConfig)
    completion_status_by_jobid: dict[int, int] = field(default_factory=dict)
    completion_status_by_submit_index: dict[int, int] = field(default_factory=dict)


@dataclass
class _QueuedJob:
    jobid: int
    nnodes: int
    jobspec: Any
    submit_index: int
    trace_idx: int | None = None


class MockAdapter:
    """
    Deterministic backend for unit and component tests.

    Default behavior stays intentionally simple:
    - FCFS scheduling on node count
    - starts flush during quiescence checks
    - completions succeed with status 0

    Tests can override behavior with a ``MockScenario`` instead of monkeypatching
    adapter internals directly.
    """

    def __init__(self, *, scenario: MockScenario | None = None) -> None:
        self.simulation = None
        self.scenario = scenario or MockScenario()
        self._jobid_gen = itertools.count(1)
        self._free_nodes = 0
        self._total_nodes = 0
        self._resource_desc: dict[str, Any] = {}
        self._queue: deque[_QueuedJob] = deque()
        self._running: dict[int, _QueuedJob] = {}
        self._completed_jobids: set[int] = set()
        self._cancelled_jobids: set[int] = set()
        self._submit_order: list[int] = []
        self._completion_statuses: dict[int, int] = dict(
            self.scenario.completion_status_by_jobid
        )

        self.acked_starts: list[int] = []
        self.acked_completes: list[int] = []
        self.submitted: list[int] = []
        self.cancelled: list[int] = []
        self._nodelists: dict[int, list[int]] = {}

        self._deferred: deque[Callable[[], None]] = deque()
        self._reactor_running = False
        self.accumulated_quiescent_payloads: list[dict[str, Any]] = []
        self.quiescent_payloads: list[dict[str, Any]] = []
        self.applied_quiescence_steps: list[MockQuiescenceStep] = []
        self._quiescence_step_index = 0
        self._queued_quiescence_steps: deque[MockQuiescenceStep] = deque()

    def open(self, simulation) -> None:
        self.simulation = simulation

    def close(self) -> None:
        return

    def install_resources(self, cfg):
        """Register emulated resources with the resource manager."""
        self._resource_desc = self.describe_resources(cfg)
        self._free_nodes = self._resource_desc["nnodes"]
        self._total_nodes = self._free_nodes

    def describe_resources(self, cfg):
        return {
            "source_path": None,
            "nnodes": int(cfg.nnodes or 0),
            "cores_per_node": int(cfg.ncpus or 1),
            "gpus_per_node": int(cfg.ngpus or 0),
            "jobspec_shape": dict(self.scenario.resources.jobspec_shape),
            "leaf_resources": dict(self.scenario.resources.leaf_resources),
            "rabbit_storage": dict(self.scenario.resources.rabbit_storage),
        }

    def reload_scheduler(self, cfg):
        """Restart the scheduler and associated modules."""
        return

    def register_exec_service(self):
        """Register the applicable emulated exec service."""
        return

    def register_job_tracking(self):
        """Setup logging for job state changes."""
        return

    def arm_watchers(self):
        """Setup and configure any watchers."""
        return

    def start_reactor(self) -> None:
        self._reactor_running = True
        while self._reactor_running and self._deferred:
            cb = self._deferred.popleft()
            cb()

    def stop_reactor(self) -> None:
        self._reactor_running = False

    def get_kvs_stats(self) -> dict:
        return {"dbfile_size": 0, "object_count": 0}

    def supports_async_submit(self) -> bool:
        return True

    def submit_job_async(self, jobspec):
        # No real round-trip to pipeline: submit now and box the id so the
        # engine's asynchronous path is exercised end to end under test.
        return ("mock-submit", self.submit_job(jobspec))

    def submit_get_id(self, handle) -> int:
        tag, jobid = handle
        assert tag == "mock-submit"
        return jobid

    def submit_job(self, jobspec) -> int:
        """
        Accepts Flux jobspec-like input (dict or JSON string) and queues the job.
        """
        jobid = next(self._jobid_gen)
        self.submitted.append(jobid)

        jobspec_obj = _jobspec_object(jobspec)
        nnodes = _count_resource_type(jobspec_obj.get("resources", []), "node") or 1
        submit_index = len(self._submit_order) + 1
        self._submit_order.append(jobid)

        status = self.scenario.completion_status_by_submit_index.get(submit_index)
        if status is not None:
            self._completion_statuses[jobid] = int(status)

        self._queue.append(
            _QueuedJob(
                jobid=jobid,
                nnodes=nnodes,
                jobspec=jobspec_obj,
                submit_index=submit_index,
            )
        )
        return jobid

    def cancel_job(self, jobid):
        """Cancel a queued or running job."""
        for queued in list(self._queue):
            if queued.jobid == jobid:
                self._queue.remove(queued)
                self._cancelled_jobids.add(jobid)
                self.cancelled.append(jobid)
                return

        job = self._running.pop(jobid, None)
        if job is not None:
            self._free_nodes += job.nnodes
            self._cancelled_jobids.add(jobid)
            self.cancelled.append(jobid)

    def set_completion_status(self, jobid: int, status: int) -> None:
        self._completion_statuses[jobid] = int(status)

    def enqueue_quiescence_step(self, step: MockQuiescenceStep) -> None:
        self._queued_quiescence_steps.append(step)

    def _try_schedule(self, max_starts: int | None = None) -> list[int]:
        """
        FCFS: start as many queued jobs as fit in free nodes.
        """
        if self.scenario.scheduling_policy != "fcfs":
            raise ValueError(
                f"Unsupported mock scheduling policy {self.scenario.scheduling_policy!r}"
            )

        started: list[int] = []
        while self._queue and self._queue[0].nnodes <= self._free_nodes:
            if max_starts is not None and len(started) >= max_starts:
                break

            job = self._queue.popleft()
            self._free_nodes -= job.nnodes
            self._running[job.jobid] = job

            nodes = list(range(self._free_nodes, self._free_nodes + job.nnodes))
            self._nodelists[job.jobid] = nodes
            started.append(job.jobid)
            logger.debug("Started: %s", job.jobid)

        return started

    def query_quiescent(self, json_string_or_payload, return_cb):
        """
        In Flux, this asks jobtap whether the system is stable.
        In mock, behavior is controlled by the scenario's quiescence config.
        """
        if self.simulation is None:
            raise RuntimeError("MockAdapter not opened with a simulation")

        payload = _payload_object(json_string_or_payload)
        self.quiescent_payloads.append(payload)
        step = self._resolve_quiescence_step()
        self.applied_quiescence_steps.append(step)

        def _cont():
            if step.flush_starts:
                max_starts = step.max_starts
                if max_starts is None:
                    max_starts = self.scenario.max_starts_per_query
                if max_starts is None:
                    max_starts = self.scenario.quiescence.default_max_starts
                jobids = self._try_schedule(max_starts=max_starts)
                for jid in jobids:
                    self.simulation.start_job(jid)
            return_cb(None, None)

        for _ in range(max(0, int(step.delay_reactor_ticks))):
            self._deferred.append(lambda: None)
        self._deferred.append(_cont)

    def accumulate_quiescent(self, json_string_or_payload):
        payload = _payload_object(json_string_or_payload)
        self.accumulated_quiescent_payloads.append(payload)

    def get_eventlog(self, jobid):
        """Get a synthetic eventlog for a job."""
        if self.scenario.diagnostics.use_simulated_timestamps:
            eventlog = self._eventlog_from_simulation(jobid)
            if eventlog is not None:
                return eventlog

        return self._static_eventlog(jobid)

    def get_job_diagnostics(self, jobid):
        queued = next((job for job in self._queue if job.jobid == jobid), None)
        running = self._running.get(jobid)
        job = queued or running
        state = self._job_state(jobid)

        diag = {
            "id": jobid,
            "formatted_id": self.get_formatted_id(jobid),
            "state": state,
            "nnodes": job.nnodes if job else self._job_nnodes(jobid),
            "jobspec": job.jobspec if job else self._job_jobspec(jobid),
            "eventlog": self.get_eventlog(jobid),
        }

        if self.scenario.diagnostics.include_kvs:
            diag["kvs"] = {
                "jobspec": self._job_jobspec(jobid),
                "R": self._build_mock_r(jobid),
                "eventlog": diag["eventlog"].get("eventlog", ""),
            }
        if self.scenario.diagnostics.include_queue_status:
            diag["queue_status"] = {
                "queued": len(self._queue),
                "running": len(self._running),
                "free_nodes": self._free_nodes,
                "total_nodes": self._total_nodes,
            }
        return diag

    def get_scheduler_state(self, jobid):
        nodelist = self._nodelists.get(jobid, [])
        job = self._job_lookup(jobid)
        transitions = job.state_transitions if job is not None else {}

        return {
            "state": self._job_state(jobid),
            "flux_t_submit": transitions.get("SUBMITTED", ""),
            "flux_t_run": transitions.get("STARTED", ""),
            "flux_t_cleanup": transitions.get("COMPLETED", ""),
            "flux_expiration": "",
            "flux_duration": getattr(job, "elapsed_time", "") if job is not None else "",
            "flux_nodelist": ",".join(str(node) for node in nodelist),
            "annotations": "",
        }

    def check_jobspec_satisfiability(self, jobspec_json):
        try:
            jobspec_obj = json.loads(jobspec_json)
        except Exception as e:
            return {"error": repr(e)}

        needed = _count_resource_type(jobspec_obj.get("resources", []), "node")
        return {
            "satisfiable": needed <= self._total_nodes,
            "node_count": needed,
            "total_nodes": self._total_nodes,
        }

    def get_formatted_id(self, job_id):
        return f"ƒ{job_id}"

    def nodelist_lookup(self, jobid: int):
        return self._nodelists.get(jobid, []), "mock"

    def ack_start(self, jobid: int) -> None:
        self.acked_starts.append(jobid)

    def ack_complete(self, jobid: int) -> None:
        self.acked_completes.append(jobid)
        job = self._running.pop(jobid, None)
        logger.debug("Ended: %s", jobid)
        if job is not None:
            self._free_nodes += job.nnodes
        self._completed_jobids.add(jobid)

    def _resolve_quiescence_step(self) -> MockQuiescenceStep:
        if self._queued_quiescence_steps:
            return self._queued_quiescence_steps.popleft()
        script = self.scenario.quiescence.script
        if self._quiescence_step_index < len(script):
            step = script[self._quiescence_step_index]
            self._quiescence_step_index += 1
            return step
        return MockQuiescenceStep(
            flush_starts=self.scenario.quiescence.default_flush_starts,
            delay_reactor_ticks=self.scenario.quiescence.default_delay_reactor_ticks,
            max_starts=self.scenario.quiescence.default_max_starts,
        )

    def _job_lookup(self, jobid: int):
        if self.simulation is None:
            return None
        return self.simulation.job_map.get(jobid)

    def _job_jobspec(self, jobid: int):
        job = self._job_lookup(jobid)
        if job is not None:
            return getattr(job, "jobspec", None)
        queued = next((item for item in self._queue if item.jobid == jobid), None)
        if queued is not None:
            return queued.jobspec
        return None

    def _job_nnodes(self, jobid: int) -> int | None:
        job = self._job_lookup(jobid)
        if job is not None:
            return getattr(job, "nnodes", None)
        queued = next((item for item in self._queue if item.jobid == jobid), None)
        if queued is not None:
            return queued.nnodes
        return None

    def _job_state(self, jobid: int) -> str:
        if jobid in self._cancelled_jobids:
            return "cancelled"
        if jobid in self._running:
            return "running"
        if any(job.jobid == jobid for job in self._queue):
            return "queued"
        if jobid in self._completed_jobids:
            status = self._completion_statuses.get(jobid, 0)
            return "failed" if status else "complete"
        if jobid in self.submitted:
            return "submitted"
        return "unknown"

    def _build_mock_r(self, jobid: int) -> dict[str, Any]:
        nodes = self._nodelists.get(jobid, [])
        if not nodes:
            return {}

        resource_desc = self._resource_desc or self.describe_resources(
            _DummyConfig(self._total_nodes)
        )
        children: dict[str, str] = {}
        cores_per_node = int(resource_desc.get("cores_per_node") or 0)
        gpus_per_node = int(resource_desc.get("gpus_per_node") or 0)
        if cores_per_node > 0:
            children["core"] = _idset_from_count(cores_per_node)
        if gpus_per_node > 0:
            children["gpu"] = _idset_from_count(gpus_per_node)

        return {
            "execution": {
                "R_lite": [
                    {
                        "rank": _idset_from_values(nodes),
                        "children": children,
                    }
                ]
            }
        }

    def _eventlog_from_simulation(self, jobid: int) -> dict[str, Any] | None:
        job = self._job_lookup(jobid)
        if job is None:
            return None

        events: list[dict[str, Any]] = []
        submit_ts = job.state_transitions.get("SUBMITTED")
        start_ts = job.state_transitions.get("STARTED")
        finish_ts = job.state_transitions.get("COMPLETED")
        status = self._completion_statuses.get(jobid, 0)

        if submit_ts not in (None, ""):
            events.append({"timestamp": float(submit_ts), "name": "submit"})
            events.append({"timestamp": float(submit_ts), "name": "priority", "context": {"priority": 16}})
        if start_ts not in (None, ""):
            events.append({"timestamp": float(start_ts), "name": "alloc"})
            events.append({"timestamp": float(start_ts), "name": "start"})
        if finish_ts not in (None, ""):
            events.append({"timestamp": float(finish_ts), "name": "finish", "context": {"status": status}})
            events.append(
                {
                    "timestamp": float(finish_ts),
                    "name": "release",
                    "context": {"ranks": "all", "final": True},
                }
            )
        if jobid in self._cancelled_jobids:
            cancel_ts = finish_ts
            if cancel_ts in (None, ""):
                cancel_ts = start_ts if start_ts not in (None, "") else submit_ts
            if cancel_ts not in (None, ""):
                events.append({"timestamp": float(cancel_ts), "name": "cancel"})

        if not events:
            return None

        return {
            "id": jobid,
            "eventlog": "\n".join(json.dumps(event) for event in events) + "\n",
        }

    def _static_eventlog(self, jobid: int) -> dict[str, Any]:
        events = [
            {
                "timestamp": 1771624596.0322015,
                "name": "submit",
                "context": {"userid": 1000, "urgency": 16, "flags": 0, "version": 1},
            },
            {"timestamp": 1771624596.0427759, "name": "validate"},
            {"timestamp": 1771624596.0531645, "name": "depend"},
            {"timestamp": 1771624596.0532191, "name": "priority", "context": {"priority": 16}},
        ]
        queued = any(job.jobid == jobid for job in self._queue)
        status = self._completion_statuses.get(jobid, 0)
        if jobid in self._running:
            events.extend(
                [
                    {"timestamp": 1771624596.0546091, "name": "alloc"},
                    {"timestamp": 1771624596.239527, "name": "start"},
                ]
            )
        elif jobid in self._cancelled_jobids:
            events.append({"timestamp": 1771624596.239527, "name": "cancel"})
        elif not queued and jobid in self.submitted:
            events.extend(
                [
                    {"timestamp": 1771624596.0546091, "name": "alloc"},
                    {"timestamp": 1771624596.239527, "name": "start"},
                    {
                        "timestamp": 1771624596.2398844,
                        "name": "finish",
                        "context": {"status": status},
                    },
                    {
                        "timestamp": 1771624596.2399106,
                        "name": "release",
                        "context": {"ranks": "all", "final": True},
                    },
                    {"timestamp": 1771624596.2399344, "name": "free"},
                    {"timestamp": 1771624596.2399468, "name": "clean"},
                ]
            )
        return {
            "id": jobid,
            "eventlog": "\n".join(json.dumps(event) for event in events) + "\n",
        }


@dataclass
class _DummyConfig:
    nnodes: int
    ncpus: int = 1
    ngpus: int = 0


def _jobspec_object(jobspec) -> dict[str, Any]:
    if isinstance(jobspec, str):
        try:
            return json.loads(jobspec)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"submit_job expected JSON string or dict; got invalid JSON: {e}"
            ) from e
    if isinstance(jobspec, Mapping):
        return dict(jobspec)
    raise TypeError(f"submit_job expected dict or JSON string, got {type(jobspec)!r}")


def _count_resource_type(resources, target_type, multiplier=1):
    total = 0
    for resource in resources:
        if not isinstance(resource, Mapping):
            continue
        count = int(resource.get("count", 1))
        current_multiplier = multiplier * count
        if resource.get("type") == target_type:
            total += current_multiplier
        children = resource.get("with", [])
        if isinstance(children, list):
            total += _count_resource_type(children, target_type, current_multiplier)
    return total


def _payload_object(value) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {"raw": value}
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _idset_from_values(values: list[int]) -> str:
    if not values:
        return ""
    ordered = sorted(set(int(value) for value in values))
    ranges: list[str] = []
    start = ordered[0]
    previous = ordered[0]

    for value in ordered[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append(f"{start}-{previous}" if start != previous else str(start))
        start = value
        previous = value
    ranges.append(f"{start}-{previous}" if start != previous else str(start))
    return ",".join(ranges)


def _idset_from_count(count: int) -> str:
    if count <= 0:
        return ""
    if count == 1:
        return "0"
    return f"0-{count - 1}"
