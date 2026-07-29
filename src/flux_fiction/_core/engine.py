from __future__ import annotations

import flux_fiction._core.errors as errors
import flux_fiction._core.models as models
import flux_fiction._core.events as events
from flux_fiction._core.faketime import FakeTimeController

from flux_fiction._adapters.base import Adapter 
import flux_fiction._outputs.filesystem_output as filesystem_output
import flux_fiction._outputs.vis as output_vis
from flux_fiction.api.status import RunStatusWriter, utcnow_iso
from flux_fiction.telemetry import TelemetryClient

from flux_fiction._exec.simexec import SimpleExec  

# from flux_fiction.telemetry import get_tracer

from dataclasses import dataclass
import logging
from collections import defaultdict
import csv
import time
from pathlib import Path

import json
import os
from tqdm import tqdm

# dftracer imports for simulated-time job lifecycle tracing
try:
    from dftracer.python import dftracer, dft_fn as DFTracerFn
    _dft = DFTracerFn("simulation")
    _dft_available = True
except ImportError:
    _dft = None
    _dft_available = False

logger = logging.getLogger(__name__)

# How often (in time-steps) to stat the early-finalize sentinel. Cheap next to
# the status.json write that already happens every step, but throttled anyway.
EARLY_FINALIZE_CHECK_EVERY = 10

# tracer = get_tracer()

@dataclass(frozen=True)
class EngineResult:
    ok: bool
    message: str = ""


def _write_summary_file(path: str | None, payload: dict) -> None:
    if not path:
        return
    summary_path = Path(path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def run(
    config: object,
    adapter: Adapter,
    *,
    status: RunStatusWriter | None = None,
) -> EngineResult:
    """
    Core entrypoint. This will:
      - init Flux adapter
      - load resources
      - load traces
      - execute DES loop
      - produce artifacts/metrics
    """
    logger.log(1, f"[core] engine.run() got config: {config}")

    faketime_controller = None
    if config.faketime_timestamp_file:
        faketime_controller = FakeTimeController(
            config.faketime_timestamp_file,
            initial_epoch=config.faketime_initial_epoch,
            tolerance=config.faketime_tolerance,
            near_event_threshold=config.faketime_near_event_threshold,
            seed=config.faketime_seed,
        )

    #TODO describe_resources seems fragile. Look into what can break it and if it can work with heterogenenous resources. We could probably use flux for this instead of parsing the jgf/config
    resource_desc = adapter.describe_resources(config)
    resource_nnodes = int(resource_desc.get("nnodes") or config.nnodes)
    resource_cores_per_node = int(resource_desc.get("cores_per_node") or config.ncpus)
    resource_gpus_per_node = int(resource_desc.get("gpus_per_node") or config.ngpus or 0)

    #TODO understand how jobspec_shape is generated
    jobspec_shape = resource_desc.get("jobspec_shape", {})
    rabbit_storage = resource_desc.get("rabbit_storage", {})
    raw_jobspec_override = None
    if getattr(config, "raw_jobspec_file", None):
        with open(config.raw_jobspec_file, "r", encoding="utf-8") as f:
            raw_jobspec_override = json.load(f)
    node_exclusive_accounting = bool(
        config.exclusive
        or output_vis.match_policy_is_node_exclusive(config.config_json)
    )

    kvs_size_start = int(adapter.get_kvs_stats().get("dbfile_size", 0))

    exec_validator = SimpleExec(
        resource_nnodes,
        resource_cores_per_node,
        gpus_per_node=resource_gpus_per_node,
        exclusive=node_exclusive_accounting,
    )

    simulation = Simulation(
        adapter,
        events.EventList(),
        {},
        submit_job_hook=exec_validator.submit_job,
        start_job_hook=exec_validator.start_job,
        complete_job_hook=exec_validator.complete_job,
        batch_job_starts=config.batch_job_starts,
        account_system_latency=config.account_system_latency,
        jobtap_logging=config.jobtap_logging,
        async_submit=getattr(config, "async_submit", False),
        submit_novalidate=getattr(config, "submit_novalidate", False),
        output_dir = config.output_dir,
        faketime_controller=faketime_controller,
        quiescent_accumulation_window=config.quiescent_accumulation_window,
        otel_enabled=config.otel_enabled,
        otel_bridge_socket=config.otel_bridge_socket,
        otel_service_name=config.otel_service_name,
        status=status or RunStatusWriter(getattr(config, "status_file", None)),
        config_file=getattr(config, "config_file", None),
        source_config_file=getattr(config, "source_config_file", None),
        trace_file=config.job_traces,
    )

    run_span_id = simulation.telemetry.start_span(
        "engine.run_total",
        output_dir=config.output_dir,
        trace_file=getattr(config, "job_traces", None),
    )

    with simulation.telemetry.span("engine.adapter_setup"):
        adapter.open(simulation)
        adapter.install_resources(config)
        adapter.reload_scheduler(config)
        adapter.register_exec_service()
        adapter.arm_watchers()
        adapter.register_job_tracking()

    # adapter.configure_backend(config, simulation.start_job)

    reader = models.SacctReader(config.job_traces, require_gpus=(config.ngpus and config.ngpus > 0))

    with simulation.telemetry.span("engine.trace_load"):
        reader.validate_trace()
        jobs = list(reader.read_trace())
    simulation.jobs_total = len(jobs)
    simulation.write_status_snapshot(state="running")

    # exit(292)
    
    for idx, job in enumerate(jobs):
        job.trace_index = idx  
 
    with simulation.telemetry.span("engine.prepare_jobs", jobs_total=len(jobs)):
        for job in jobs:
            if raw_jobspec_override is not None:
                job.set_jobspec_override(raw_jobspec_override)
            else:
                job.set_jobspec_shape(jobspec_shape)
                job.set_rabbit_storage_shape(
                    rabbit_storage,
                    emit_dw=config.rabbit_storage_emit_dw,
                    name=config.rabbit_storage_name,
                )
                if config.exclusive:
                    job.set_exclusive(resource_cores_per_node, resource_gpus_per_node)
        for job in jobs:
            job.insert_apriori_events(simulation)
    pbar = tqdm(total=len(jobs), desc="Jobs completed", unit="job", leave=True)
    simulation.progress = pbar

    # with tracer.start_as_current_span("simulation.advance"):
    try:
        with simulation.telemetry.span("engine.prime_simulation"):
            simulation.advance()
    except Exception as e:
        message = _finalize_failure(simulation) or str(e)
        logger.error("Simulation failed before reactor start: %s", message)
        simulation.telemetry.end_span(run_span_id, state="failed", error=message)
        _close_progress(simulation)
        _close_adapter(adapter)
        simulation.telemetry.close()
        return EngineResult(ok=False, message=message)

    try:
        with simulation.telemetry.span("engine.reactor_run"):
            adapter.start_reactor()
    except Exception as e:
        message = (
            _finalize_failure(simulation)
            or f"Error during simulation time: {e}"
        )
        logger.error("Reactor encountered an exception: %s", message)
        simulation.telemetry.end_span(run_span_id, state="failed", error=message)
        _close_progress(simulation)
        _close_adapter(adapter)
        simulation.telemetry.close()
        return EngineResult(ok=False, message=message)

    if simulation.failed_reason:
        message = _finalize_failure(simulation)
        simulation.telemetry.end_span(run_span_id, state="failed", error=message)
        _close_progress(simulation)
        _close_adapter(adapter)
        simulation.telemetry.close()
        return EngineResult(ok=False, message=message)

    try:
        with simulation.telemetry.span("engine.adapter_close"):
            adapter.close()
    except Exception as e:
        logger.error(f"Error tearing down watchers {e}")
        simulation.telemetry.end_span(run_span_id, state="failed", error="Error tearing down watchers")
        simulation.telemetry.close()
        return EngineResult(ok=False, message="Error tearing down watchers")

    if simulation.progress is not None:
        simulation.progress.close()

    #TODO add a way to verify that the eventlog is done before grabbing it :)

    with simulation.telemetry.span("engine.postprocess"):
        run_id = f"nodes{resource_nnodes}_cpr{resource_cores_per_node}"
        kvs_outfile = f"{config.output_dir}kvs_growth_{run_id}.csv"
        simulation.dump_kvs_timeseries(kvs_outfile)
        logger.info(f"Wrote KVS time series to {kvs_outfile}")

        simulation.dump_eventlog()
        filesystem_output.dump_transitions_to_csv(simulation, f"{config.output_dir}job_transitions.csv", adapter)
        filesystem_output.write_per_node_chrome_trace(simulation, f"{config.output_dir}pernode.json", adapter)
        resource_summary = output_vis.summarize_and_plot_resources(
            simulation,
            adapter,
            resource_desc,
            config.output_dir,
            config_json=config.config_json,
            config_exclusive=config.exclusive,
            make_plots=getattr(config, "make_plots", True),
        )

    kvs_size_end = int(adapter.get_kvs_stats().get("dbfile_size", 0))
    completed = max(1, simulation.num_complete)  

    kvs_bytes_per_completed = (kvs_size_end - kvs_size_start) / float(completed)

    summary_makespan = float(resource_summary.get("makespan_hours", 0.0)) * 3600.0
    validator_makespan = float(exec_validator.makespan.end - exec_validator.makespan.beginning)
    makespan = max(1e-9, summary_makespan or validator_makespan)
    kvs_growth_bytes_per_sim_s = (kvs_size_end - kvs_size_start) / makespan

    print(f"KVS content-sqlite dbfile_size start: {kvs_size_start} bytes")
    print(f"KVS content-sqlite dbfile_size end:   {kvs_size_end} bytes")
    print(f"KVS bytes per completed job:          {kvs_bytes_per_completed:.2f} bytes/job")
    print(f"KVS growth rate:                      {kvs_growth_bytes_per_sim_s:.2f} bytes/s (sim time)")

    waits = []
    for job in simulation.job_map.values():
        if job.queue_wait is not None:
            waits.append(float(job.queue_wait))

    avg_wait = None
    if waits:
        avg_wait = sum(waits) / len(waits)
        print(f"Average queue wait time: {avg_wait:.6f} seconds (sim time) over {len(waits)} jobs")
    else:
        print("Average queue wait time: N/A (no jobs have queue_wait recorded)")

    max_wait = None
    if waits:
        max_wait = max(waits)
        print(f"Max queue wait time: {max_wait:.6f} seconds (sim time)")

    observed_start_lags = sorted(
        float(job.flux_observed_start) - float(job.state_transitions["STARTED"])
        for job in simulation.job_map.values()
        if job.flux_observed_start is not None
        and job.state_transitions.get("STARTED") is not None
    )
    observed_start_lag_avg = (
        sum(observed_start_lags) / len(observed_start_lags)
        if observed_start_lags else None
    )
    observed_start_lag_max = observed_start_lags[-1] if observed_start_lags else None
    observed_start_lag_p95 = (
        observed_start_lags[min(len(observed_start_lags) - 1, int(0.95 * len(observed_start_lags)))]
        if observed_start_lags else None
    )

    summary_payload = {
        "version": 1,
        "state": "succeeded",
        "generated_at": utcnow_iso(),
        "output_dir": config.output_dir,
        "config_file": getattr(config, "config_file", None),
        "source_config_file": getattr(config, "source_config_file", None),
        "trace_file": getattr(config, "job_traces", None),
        "jobs_total": int(simulation.jobs_total),
        "jobs_completed": int(simulation.num_complete),
        "makespan_seconds": float(makespan),
        "makespan_hours": float(makespan) / 3600.0,
        "avg_queue_wait_seconds": None if avg_wait is None else float(avg_wait),
        "max_queue_wait_seconds": None if max_wait is None else float(max_wait),
        "quiescence_epochs": int(simulation.current_epoch),
        "last_quiescent_epoch": int(simulation.last_quiescent_epoch),
        "stale_quiescence_replies": int(simulation.stale_quiescence_replies),
        "flux_observed_start_lag_samples": len(observed_start_lags),
        "flux_observed_start_lag_avg_seconds": observed_start_lag_avg,
        "flux_observed_start_lag_p95_seconds": observed_start_lag_p95,
        "flux_observed_start_lag_max_seconds": observed_start_lag_max,
        "kvs_size_start_bytes": int(kvs_size_start),
        "kvs_size_end_bytes": int(kvs_size_end),
        "kvs_bytes_per_completed_job": float(kvs_bytes_per_completed),
        "kvs_growth_bytes_per_sim_s": float(kvs_growth_bytes_per_sim_s),
        "resource_summary": resource_summary,
    }
    _write_summary_file(getattr(config, "summary_file", None), summary_payload)
    simulation.telemetry.end_span(run_span_id, state="succeeded", makespan_seconds=float(makespan))
    simulation.telemetry.close()

    return EngineResult(ok=True, message="Ran Successfully")


def _close_progress(simulation):
    if simulation.progress is not None:
        try:
            simulation.progress.close()
        except Exception:
            logger.exception("Error closing progress bar")


def _close_adapter(adapter):
    try:
        adapter.close()
    except Exception:
        logger.exception("Error tearing down adapter after failure")


def _finalize_failure(simulation):
    try:
        simulation.finalize_failure_report()
    except Exception:
        logger.exception("Error finalizing failure diagnostics")
    return simulation.failed_reason

class Simulation(object):
    '''
    Primary class for the emulator

    Contains functions needed to orchestrate the emulator 
    '''
    def __init__(
            self,
            adapter: Adapter,
            event_list: events.EventList,
            job_map: dict,
            submit_job_hook: callable=None,
            start_job_hook: callable=None,
            complete_job_hook: callable=None,
            progress=None,
            batch_job_starts: bool = True,
            account_system_latency: bool = True,
            jobtap_logging: bool = False,
            async_submit: bool = False,
            submit_novalidate: bool = False,
            output_dir: str = "./",
            faketime_controller: FakeTimeController | None = None,
            quiescent_accumulation_window: float = 0.0,
            otel_enabled: bool = False,
            otel_bridge_socket: str | None = None,
            otel_service_name: str = "flux-fiction",
            status: RunStatusWriter | None = None,
            config_file: str | None = None,
            source_config_file: str | None = None,
            trace_file: str | None = None,
    ):
        self.event_list = event_list
        self.job_map = job_map
        self.current_time = 0
        self.adapter = adapter
        self.num_submits = 0
        self.progress = progress
        self.num_complete = 0
        self.pending_inactivations = set()
        self.job_manager_quiescent = True
        self.submit_job_hook = submit_job_hook
        self.start_job_hook = start_job_hook
        self.complete_job_hook = complete_job_hook
        self.pending_continuation = False
        # Jobs whose submission is in flight but whose id has not been
        # collected yet. Drained before any quiescence traffic, so the
        # scheduler is never told about a job whose id we do not hold.
        self._pending_submits = []
        self.async_submit = bool(async_submit) and bool(
            getattr(adapter, "supports_async_submit", lambda: False)()
        )
        self.submit_novalidate = bool(submit_novalidate)
        if async_submit and not self.async_submit:
            logger.warning(
                "async_submit requested but %s does not support it; "
                "falling back to blocking submits",
                type(adapter).__name__,
            )
        self.step_expect = defaultdict(lambda: {"submits": 0, "finishes": 0})
        self.time_step = 0
        self.pending_start_msgs = {} 
        self.queue_wait = None
        self.kvs_samples = []          
        self.kvs_sample_every = 1      
        self.kvs_module_name = "content-sqlite"
        self.batch_job_starts = bool(batch_job_starts)
        self.account_system_latency = bool(account_system_latency)
        self.jobtap_logging = bool(jobtap_logging)
        self.output_dir = output_dir
        self.faketime_controller = faketime_controller
        self.failed_reason = None
        self.failure_report_path = None
        self.failure_diagnostics_reason = None
        self.final_quiescence_probe_sent = False
        self.otel_enabled = bool(otel_enabled and otel_bridge_socket)
        self.otel_bridge_socket = otel_bridge_socket
        self.otel_service_name = otel_service_name
        # A positive window coalesces distinct logical event buckets before
        # asking the scheduler for quiescence.  That is an optional throughput
        # tradeoff: it changes the scheduler-visible ordering and therefore
        # must not be the correctness default.
        self.quiescent_accumulation_window = float(quiescent_accumulation_window)
        self.pending_quiescent_expect = {"submits": 0, "finishes": 0}
        # Epochs make delayed quiescence replies harmless.  An epoch advances
        # once for each synchronous bucket that mutates scheduler-visible state.
        self.current_epoch = 0
        self.latest_mutating_epoch = 0
        self.last_quiescent_epoch = 0
        self.quiescence_inflight = False
        self.quiescence_epoch = None
        self.quiescence_bucket_time = None
        self.stale_quiescence_replies = 0
        self.num_started = 0
        self.jobs_total = 0
        self.early_finalize_requested = False
        self.early_finalize_reason = ""
        self._finalize_check_counter = 0
        self.status_writer = status or RunStatusWriter(None)
        self.status_context = {
            "run_dir": str(Path(output_dir).resolve().parent) if output_dir else None,
            "config_file": config_file,
            "source_config_file": source_config_file,
            "trace_file": trace_file,
        }
        self.telemetry = TelemetryClient(
            otel_bridge_socket if self.otel_enabled else None,
            otel_service_name,
            "python-engine",
        )

    def write_status_snapshot(self, *, state: str | None = None, failure_reason: str | None = None):
        if not self.status_writer.enabled:
            return
        self.status_writer.update(
            state=state,
            failure_reason=failure_reason,
            jobs_total=int(self.jobs_total),
            jobs_submitted=int(self.num_submits),
            jobs_running=max(0, int(self.num_started) - int(self.num_complete)),
            jobs_completed=int(self.num_complete),
            jobs_started=int(self.num_started),
            current_sim_time=float(self.current_time),
            time_step=int(self.time_step),
            **self.status_context,
        )

    def _next_event_time(self):
        next_item = self.event_list.min()
        if not next_item:
            return None
        return next_item[0]

    def _add_pending_quiescent_expect(self, expect):
        if not expect:
            return
        self.pending_quiescent_expect["submits"] += int(expect.get("submits", 0) or 0)
        self.pending_quiescent_expect["finishes"] += int(expect.get("finishes", 0) or 0)

    def _bump_epoch_for_expect(self, expect):
        if not expect:
            return False
        mutates = int(expect.get("submits", 0) or 0) + int(expect.get("finishes", 0) or 0)
        if mutates <= 0:
            return False
        self.current_epoch += 1
        self.latest_mutating_epoch = self.current_epoch
        return True

    def _flush_pending_quiescent_expect(self):
        expect = {
            "submits": int(self.pending_quiescent_expect["submits"]),
            "finishes": int(self.pending_quiescent_expect["finishes"]),
        }
        if expect["submits"] or expect["finishes"]:
            self.adapter.accumulate_quiescent(json.dumps({
                "epoch": self.current_epoch,
                "logical_time": self.current_time,
                "expect": expect,
            }))
        self.pending_quiescent_expect = {"submits": 0, "finishes": 0}
        return expect

    def _send_quiescence_probe(self, *, final=False):
        if self.quiescence_inflight:
            return False
        # Barrier: never probe, and never accumulate expectations, while a
        # submission is still in flight. _flush_pending_quiescent_expect() is
        # only reached from here, so this one call covers both.
        self._drain_pending_submits()
        expect = self._flush_pending_quiescent_expect()
        probe_epoch = self.current_epoch
        bucket_time = float(self.current_time)
        payload = {
            "epoch": probe_epoch,
            "logical_time": bucket_time,
            "expect": expect,
            "final": bool(final),
        }
        span_id = self.telemetry.start_span(
            "simulation.query_quiescent",
            sim_time=bucket_time,
            epoch=probe_epoch,
            expect_submits=int(expect["submits"]),
            expect_finishes=int(expect["finishes"]),
            time_step=self.time_step,
        )
        self.quiescence_inflight = True
        self.quiescence_epoch = probe_epoch
        self.quiescence_bucket_time = bucket_time
        try:
            self.adapter.query_quiescent(
                json.dumps(payload),
                lambda fut, _arg, epoch=probe_epoch, logical_time=bucket_time, span=span_id:
                    self.quiescent_cb(epoch, logical_time, span),
            )
        except Exception:
            self.quiescence_inflight = False
            self.quiescence_epoch = None
            self.quiescence_bucket_time = None
            self.telemetry.end_span(span_id, state="rpc_error")
            raise
        return True

    def _should_defer_quiescent(self, next_event_time):
        if next_event_time is None:
            return False
        gap = float(next_event_time) - float(self.current_time)
        return gap < self.quiescent_accumulation_window

    def sample_kvs_stats(self):
        """
        Record a KVS stat snapshot at current sim time.
        Stores: time, dbfile_size, object_count (if available)
        """
        try:
            st = self.adapter.get_kvs_stats()
            self.kvs_samples.append({
                "time": float(self.current_time),
                "dbfile_size": int(st.get("dbfile_size", 0)),
                "object_count": int(st.get("object_count", 0)),
            })
        except Exception as e:
            # Don't crash the sim for metrics
            logger.warning("KVS sample failed at time=%s: %s", self.current_time, e)
            self.kvs_samples.append({
                "time": float(self.current_time),
                "dbfile_size": "",
                "object_count": "",
            })

    def dump_kvs_timeseries(self, out_path: str):
        fieldnames = ["time", "dbfile_size", "object_count"]
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in self.kvs_samples:
                w.writerow(row)

    def dump_scheduler_state(self, label=""):
        """
        Query the backend adapter for the state of all known jobs and dump to
        CSV.
        """
        rows = []
        for jobid, job in self.job_map.items():
            try:
                snapshot = self.adapter.get_scheduler_state(jobid)

                rows.append({
                    "sim_time": f"{float(self.current_time):.6f}",
                    "label": label,
                    "jobid": jobid,
                    "trace_idx": getattr(job, "trace_index", ""),
                    "nnodes": job.nnodes,
                    "state": snapshot.get("state", "?"),
                    "sim_submit": f"{float(job.submit_time):.6f}",
                    "sim_start": f"{float(job.start_time):.6f}" if job.start_time is not None else "",
                    "sim_complete": f"{float(job.complete_time):.6f}" if job.start_time is not None else "",
                    "flux_t_submit": snapshot.get("flux_t_submit", ""),
                    "flux_t_run": snapshot.get("flux_t_run", ""),
                    "flux_t_cleanup": snapshot.get("flux_t_cleanup", ""),
                    "flux_expiration": snapshot.get("flux_expiration", ""),
                    "flux_duration": snapshot.get("flux_duration", ""),
                    "flux_nodelist": snapshot.get("flux_nodelist", ""),
                    "annotations": snapshot.get("annotations", ""),
                    "state_transitions": json.dumps({k: f"{v:.6f}" for k, v in job.state_transitions.items()}),
                })
            except Exception as e:
                rows.append({
                    "sim_time": f"{float(self.current_time):.6f}",
                    "label": label,
                    "jobid": jobid,
                    "trace_idx": getattr(job, "trace_index", ""),
                    "nnodes": job.nnodes,
                    "state": "ERROR",
                    "sim_submit": f"{float(job.submit_time):.6f}",
                    "sim_start": "",
                    "sim_complete": "",
                    "flux_t_submit": "",
                    "flux_t_run": "",
                    "flux_t_cleanup": "",
                    "flux_expiration": "",
                    "flux_duration": "",
                    "flux_nodelist": "",
                    "annotations": "",
                    "state_transitions": str(e),
                })
        
        if not rows:
            return
        
        outfile = f"{self.output_dir}scheduler_state_log.csv"
        fieldnames = list(rows[0].keys())
        
        write_header = not os.path.exists(outfile)
        with open(outfile, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                w.writeheader()
            for r in rows:
                w.writerow(r)
        
        logger.debug("Dumped %d job states at sim_time=%s label=%s", len(rows), self.current_time, label)

    def _output_path(self, filename):
        return os.path.join(self.output_dir or ".", filename)

    def _fail(self, message):
        if self.failed_reason:
            return

        self.failure_report_path = self._write_failure_report(message)
        if self.failure_report_path:
            self.failed_reason = (
                "{}\nFailure report: {}"
                .format(message, self.failure_report_path)
            )
        else:
            self.failed_reason = message

        logger.critical("%s", self.failed_reason)
        self.write_status_snapshot(state="failed", failure_reason=self.failed_reason)
        try:
            self.adapter.stop_reactor()
        except Exception:
            logger.debug("Could not stop reactor after failure", exc_info=True)

    def _write_failure_report(self, message):
        try:
            os.makedirs(self.output_dir or ".", exist_ok=True)
            path = self._output_path("simulation_failure.txt")
            with open(path, "w") as f:
                f.write(message)
                if not message.endswith("\n"):
                    f.write("\n")
            return path
        except Exception:
            logger.exception("Could not write simulation failure report")
            return None

    def _build_failure_header(self, reason):
        unfinished = sum(
            1
            for job in self.job_map.values()
            if "INACTIVE" not in job.state_transitions
        )
        return "\n".join([
            "Simulation stopped: {}".format(reason),
            "sim_time={} completed={} submitted={} unfinished={}".format(
                self.current_time,
                self.num_complete,
                self.num_submits,
                unfinished,
            ),
        ])

    def finalize_failure_report(self):
        if not self.failure_diagnostics_reason:
            return

        report = self._build_unfinished_jobs_report(self.failure_diagnostics_reason)
        path = self._write_failure_report(report)
        if path:
            self.failure_report_path = path
            self.failed_reason = "{}\nFailure report: {}".format(report, path)
        else:
            self.failed_reason = report
        self.failure_diagnostics_reason = None

    def _eventlog_summary(self, eventlog):
        if not isinstance(eventlog, dict):
            return "eventlog unavailable: {}".format(eventlog)

        raw = eventlog.get("eventlog") or ""
        if not raw.strip():
            return "eventlog empty"

        events = []
        for line in raw.strip().splitlines():
            try:
                parsed = json.loads(line)
            except Exception:
                events.append("unparseable: {}".format(line[:160]))
                continue

            name = (parsed.get("type") or parsed.get("name") or "?").lower()
            stamp = parsed.get("timestamp", "")
            context = parsed.get("context") or {}
            event = "{}@{}".format(name, stamp) if stamp != "" else name
            if context:
                event += " context={}".format(
                    json.dumps(context, sort_keys=True, default=str)
                )
            events.append(event)

        return " -> ".join(events)

    def _job_info_summary(self, job_info):
        if not job_info:
            return ""
        if not isinstance(job_info, dict):
            return str(job_info)

        interesting = [
            "state",
            "state_single",
            "result",
            "exception",
            "userid",
            "urgency",
            "priority",
            "queue",
            "nodelist",
            "t_submit",
            "t_run",
            "t_cleanup",
            "expiration",
            "duration",
            "annotations",
        ]
        summary = {
            key: job_info.get(key)
            for key in interesting
            if key in job_info and job_info.get(key) not in (None, "")
        }
        return json.dumps(summary or job_info, sort_keys=True, default=str)

    def _job_diagnostic_block(self, jobid, job):
        try:
            formatted_id = self.adapter.get_formatted_id(jobid)
        except Exception:
            formatted_id = jobid

        lines = [
            "--- job {} ---".format(jobid),
            "formatted_id={}".format(formatted_id),
            (
                "trace_idx={trace_idx} nnodes={nnodes} ncpus={ncpus} "
                "ngpus={ngpus} rabbit_storage_gib={rabbit_gib:.3f} "
                "rabbit_shares={rabbit_shares} rabbit_request_count={rabbit_request_count}"
            ).format(
                trace_idx=getattr(job, "trace_index", None),
                nnodes=job.nnodes,
                ncpus=job.ncpus,
                ngpus=job.ngpus,
                rabbit_gib=job.rabbit_storage_gib,
                rabbit_shares=job.rabbit_storage_share_count,
                rabbit_request_count=getattr(job, "rabbit_storage_request_count", 0),
            ),
            "state_transitions={}".format(
                json.dumps(job.state_transitions, sort_keys=True, default=str)
            ),
        ]

        diag = {}
        try:
            diag = self.adapter.get_job_diagnostics(jobid)
        except Exception as e:
            lines.append("diagnostics_error={}".format(repr(e)))

        formatted = diag.get("formatted_id") if isinstance(diag, dict) else None
        if formatted:
            lines[1] = "formatted_id={}".format(formatted)

        if isinstance(diag, dict):
            eventlog = diag.get("eventlog")
            if eventlog is None and isinstance(diag.get("kvs"), dict):
                eventlog = {"eventlog": diag["kvs"].get("eventlog", "")}
            if eventlog is not None:
                lines.append("eventlog={}".format(self._eventlog_summary(eventlog)))

            job_info = diag.get("job_info")
            if job_info:
                lines.append("job_info={}".format(self._job_info_summary(job_info)))

            for key in (
                "queue_list",
                "queue_status",
                "qmanager_stats",
                "qmanager_params",
                "resource_match_stats",
            ):
                if key in diag:
                    lines.append(
                        "{}={}".format(
                            key,
                            json.dumps(diag[key], sort_keys=True, default=str),
                        )
                    )

            for key in sorted(diag):
                if key.endswith("_error"):
                    lines.append("{}={}".format(key, diag[key]))

        attributes = job.jobspec.get("attributes", {})
        system_attrs = attributes.get("system", {})
        queue = system_attrs.get("queue") or system_attrs.get("queue-name")
        lines.append("jobspec_queue={}".format(queue or "<none>"))
        lines.append(
            "jobspec_attributes={}".format(
                json.dumps(attributes, indent=2, sort_keys=True, default=str)
            )
        )
        try:
            satisfiability = self.adapter.check_jobspec_satisfiability(
                json.dumps(job.jobspec, sort_keys=True, default=str)
            )
            lines.append(
                "jobspec_satisfiability={}".format(
                    json.dumps(satisfiability, sort_keys=True, default=str)
                )
            )
        except Exception as e:
            lines.append("jobspec_satisfiability_error={}".format(repr(e)))
        lines.append(
            "jobspec_resources={}".format(
                json.dumps(
                    job.jobspec.get("resources", []),
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
        )
        return "\n".join(lines)

    def _build_unfinished_jobs_report(self, reason, limit=10):
        unfinished = [
            (jobid, job)
            for jobid, job in self.job_map.items()
            if "INACTIVE" not in job.state_transitions
        ]

        lines = [
            "Simulation stopped: {}".format(reason),
            "sim_time={} completed={} submitted={} unfinished={}".format(
                self.current_time,
                self.num_complete,
                self.num_submits,
                len(unfinished),
            ),
            (
                "Flux accepted these jobs, but the emulator did not receive "
                "allocation/start callbacks before the scheduler became quiescent."
            ),
        ]

        for jobid, job in unfinished[:limit]:
            lines.append(self._job_diagnostic_block(jobid, job))

        if len(unfinished) > limit:
            lines.append(
                "... {} more unfinished jobs omitted from this report"
                .format(len(unfinished) - limit)
            )

        return "\n\n".join(lines)

    def add_event(self, time, callback):
        '''
        Adds an event to the emulator's event list

        Takes in a time that the event will occur and a callback function to be invoked at that time
        '''
        self.event_list.add_event(time, callback)


    
    def submit_job(self, job):
        job.record_state_transition("SUBMITTED", models.qtime(self.current_time))
        # Log job submission as simulated-time event
        if _dft_available:
            try:
                sim_submit_time_ns = int(models.qtime(job.submit_time) * 1e9)
                gap_ns = int(job.gap * 1e9) if hasattr(job, 'gap') else 0
                trace_idx = getattr(job, "trace_index", None)
                dftracer.get_instance().log_event(
                    name="job_submit",
                    cat="simulation",
                    start_time=sim_submit_time_ns,
                    duration=gap_ns,
                    int_args={"trace_idx": (0, -1 if trace_idx is None else int(trace_idx))},
                    string_args={"job_id": (0, str(getattr(job, "jobid", "") or ""))},
                )
            except Exception as e:
                logger.debug("Failed to log job_submit event: %s", e)
        with self.telemetry.span(
            "simulation.submit_job",
            trace_idx=getattr(job, "trace_index", None),
            nnodes=job.nnodes,
            rabbit_storage_gib=job.rabbit_storage_gib,
        ):
            try:
                if self.submit_job_hook:
                    self.submit_job_hook(self, job)
                logger.debug("Submitting a new job")
                if self.async_submit:
                    # Start it and move on; the id is collected in
                    # _drain_pending_submits() before any quiescence traffic.
                    job.submit_async(self.adapter)
                    self._pending_submits.append(job)
                else:
                    job.submit(self.adapter)
            except Exception as e:
                job.record_state_transition("SUBMIT_FAILED", models.qtime(self.current_time))
                self._fail(
                    "Submit failed for trace_idx={}: {}\nJobspec:\n{}"
                    .format(
                        getattr(job, "trace_index", None),
                        e,
                        json.dumps(job.jobspec, indent=2, sort_keys=True, default=str),
                    )
                )
                raise

        self.num_submits += 1
        self.final_quiescence_probe_sent = False
        if self.async_submit:
            # job.jobid is not known yet; job_map is populated at drain time.
            return
        self.job_map[job.jobid] = job
        logger.info("Submitted job {}".format(job.jobid))

    def _drain_pending_submits(self):
        """Collect the job id of every in-flight submission.

        This is the barrier that makes asynchronous submission safe: it runs
        before the scheduler is told what to expect and before any quiescence
        probe, so from the scheduler's point of view the set of jobs present at
        a given logical time is identical to the blocking path.
        """
        if not self._pending_submits:
            return 0
        pending, self._pending_submits = self._pending_submits, []
        for job in pending:
            try:
                job.resolve_submit(self.adapter)
            except Exception as e:
                job.record_state_transition("SUBMIT_FAILED", models.qtime(self.current_time))
                self._fail(
                    "Submit failed (async resolve) for trace_idx={}: {}\nJobspec:\n{}"
                    .format(
                        getattr(job, "trace_index", None),
                        e,
                        json.dumps(job.jobspec, indent=2, sort_keys=True, default=str),
                    )
                )
                raise
            self.job_map[job.jobid] = job
            logger.info("Submitted job {}".format(job.jobid))
        return len(pending)

    def start_job(self, jobid):
        job: models.Job = self.job_map[jobid]

        self.final_quiescence_probe_sent = False
        job.real_start = time.time()
        self.num_started += 1
        with self.telemetry.span(
            "simulation.start_job",
            jobid=jobid,
            trace_idx=getattr(job, "trace_index", None),
        ):
            if self.start_job_hook:
                self.start_job_hook(self, job)

            if self.account_system_latency and self.faketime_controller is not None:
                # Faketime is an observation channel for Flux, not simulator
                # truth.  Scheduler/emulator wall lag must not move STARTED or
                # the completion event derived from it.
                job.flux_observed_start = models.qtime(
                    self.faketime_controller.current_effective_time()
                )
                job.ack_start(self.adapter)
                start_time = models.qtime(self.current_time)
                job.mark_started(start_time)
            elif self.account_system_latency:
                start_time = models.qtime(self.current_time)
                job.mark_started(start_time)
                job.ack_start(self.adapter)
            else:
                start_time = models.qtime(self.current_time + job.gap)
                job.mark_started(start_time)
                job.ack_start(self.adapter)

            # Log job start as simulated-time event (after all branches have set start_time)
            if _dft_available:
                try:
                    sim_start_time_ns = int(models.qtime(job.start_time) * 1e9)
                    gap_ns = int(job.gap * 1e9) if hasattr(job, 'gap') else 0
                    # mhost: emit ONE event PER allocated node (not one event with
                    # a comma-joined node list) so a trace viewer can actually
                    # group/filter by individual simulated node — same pattern
                    # as per-MPI-rank tracing. Same start_time/duration on every
                    # per-node event; only mhost differs. Stored on the job so
                    # complete_job() reuses the same node list without a second
                    # RPC (pid/tid stay dummy/constant — this single Python
                    # process emulates every node, so mhost is the only field
                    # that actually distinguishes nodes in the trace).
                    nodes = []
                    _src = None
                    try:
                        nodes, _src = self.adapter.nodelist_lookup(jobid)
                    except Exception as e:
                        logger.warning("dftrace job_start nodelist_lookup failed jobid=%s: %r", jobid, e)
                        nodes = []
                    if not nodes:
                        logger.debug("dftrace job_start empty nodelist jobid=%s src=%s", jobid, _src)
                    job.allocated_nodes = nodes
                    node_ids = [str(n) for n in sorted(nodes)] if nodes else [""]
                    trace_idx = getattr(job, "trace_index", None)
                    for node_id in node_ids:
                        dftracer.get_instance().log_event(
                            name="job_start",
                            cat="simulation",
                            start_time=sim_start_time_ns,
                            duration=gap_ns,
                            int_args={"trace_idx": (0, -1 if trace_idx is None else int(trace_idx))},
                            string_args={"job_id": (0, str(jobid or "")), "mhost": (0, node_id)},
                        )
                except Exception as e:
                    logger.debug("Failed to log job_start event: %s", e)

            job.record_state_transition("STARTED", start_time)
            job.queue_wait = job.queue_wait_time()

            ct = models.qtime(job.complete_time)
            cb = models.make_tagged_cb("complete", job, lambda: self.complete_job(job), ct)
            self.event_list.add_event(ct, cb)
            self.step_expect[ct]["finishes"] += 1

    def complete_job(self, job):
        '''
        This is used to trigger the finish and release events for a job when the time to complete it is reached
        '''
        self.num_complete += 1
        self.final_quiescence_probe_sent = False
        t = models.qtime(self.current_time)
        # Log job completion as simulated-time event (spans from start to finish)
        if _dft_available:
            try:
                sim_start_time_ns = int(models.qtime(job.start_time) * 1e9)
                duration_ns = int(models.qtime(job.elapsed_time) * 1e9)
                nodes = getattr(job, "allocated_nodes", None) or []
                node_ids = [str(n) for n in sorted(nodes)] if nodes else [""]
                trace_idx = getattr(job, "trace_index", None)
                for node_id in node_ids:
                    dftracer.get_instance().log_event(
                        name="job_complete",
                        cat="simulation",
                        start_time=sim_start_time_ns,
                        duration=duration_ns,
                        int_args={"trace_idx": (0, -1 if trace_idx is None else int(trace_idx))},
                        string_args={"job_id": (0, str(getattr(job, "jobid", "") or "")), "mhost": (0, node_id)},
                    )
            except Exception as e:
                logger.debug("Failed to log job_complete event: %s", e)
        job.record_state_transition("COMPLETED", t)
        job.record_state_transition("INACTIVE", t)
        job.real_finish = time.time() 

        with self.telemetry.span(
            "simulation.complete_job",
            jobid=job.jobid,
            trace_idx=getattr(job, "trace_index", None),
        ):
            if self.complete_job_hook:
                self.complete_job_hook(self, job)
            job.complete(self.adapter)
            if self.progress is not None:
                self.progress.update(1)
            logger.info("Completed job {}".format(job.jobid))


    def record_job_state_transition(self, jobid, state):
        logger.log(9, "record_job_state_transition ignored (now simulator-owned): job=%s state=%s",
                jobid, state)

    
    def finalize_sentinel_path(self):
        """Sentinel the (unfaked) parent process touches when time is short."""
        if not self.output_dir:
            return None
        return Path(self.output_dir) / ".finalize_now"

    def _early_finalize_requested(self):
        """Has the parent asked us to wrap up before the allocation expires?

        The check is a file stat rather than a clock read because this process
        runs under libfaketime -- its own clocks report simulated time and
        cannot see the real wall approaching. The parent watcher is not
        preloaded, so it owns the real deadline and signals through the
        filesystem.
        """
        if self.early_finalize_requested:
            return True
        self._finalize_check_counter += 1
        if self._finalize_check_counter % EARLY_FINALIZE_CHECK_EVERY:
            return False
        sentinel = self.finalize_sentinel_path()
        if sentinel is not None and sentinel.exists():
            try:
                reason = sentinel.read_text(encoding="utf-8").strip()
            except OSError:
                reason = ""
            self.early_finalize_requested = True
            self.early_finalize_reason = reason or "walltime budget nearly exhausted"
            return True
        return False

    def advance(self, *args, **kwargs):
        if self.failed_reason:
            return
        while not self.failed_reason:
            # Wrap up early rather than being SIGKILLed at the wall with an
            # empty output directory: run the same post-sim analysis the normal
            # path runs, so a truncated run still yields complete artifacts for
            # however many jobs it got through.
            if self._early_finalize_requested():
                logger.warning(
                    "Early finalize requested (%s); stopping at time_step=%d with "
                    "%d/%d jobs complete and writing partial results",
                    self.early_finalize_reason,
                    self.time_step,
                    self.num_complete,
                    self.jobs_total,
                )
                self.write_status_snapshot(state="running")
                self.post_verification()
                self.adapter.stop_reactor()
                return
            events_at_time = []

            try:
                self.current_time, events_at_time = next(self.event_list)
            except StopIteration:
                if self.num_complete < self.num_submits:
                    if self.final_quiescence_probe_sent:
                        reason = "scheduler quiescent with submitted jobs still unfinished"
                        self.failure_diagnostics_reason = reason
                        self._fail(self._build_failure_header(reason))
                        return

                    logger.info("Event list empty but jobs in flight; probing jobtap for quiescence")
                    self.final_quiescence_probe_sent = True
                    try:
                        self._send_quiescence_probe(final=True)
                    except Exception as e:
                        self._fail(
                            "Final quiescence query failed at sim_time={}: {}"
                            .format(self.current_time, e)
                        )
                        raise
                    return
                else:
                    logger.info(f"completes {self.num_complete} submits {self.num_submits}")
                    logger.info("No more events in event list, running post-sim analysis")
                    self.write_status_snapshot(state="running")
                    self.post_verification()
                    logger.info("Ending simulation")
                    self.adapter.stop_reactor()
                    return
            logger.info("Fast-forwarding time to {}".format(self.current_time))
            if self.faketime_controller is not None:
                self.faketime_controller.advance_to(self.current_time)

            has_submits = any(getattr(cb, "_ev_kind", "") == "submit" for cb in events_at_time)
            if has_submits and self.faketime_controller is None:
                logger.info("Submit events detected at time %s; waiting 1s real time for scheduler clock to advance",
                            self.current_time)
            elif has_submits:
                logger.debug("Submit events detected at time %s; faketime is active, skipping pre-submit sleep",
                             self.current_time)

            with self.telemetry.span(
                "simulation.advance.bucket",
                sim_time=self.current_time,
                event_count=len(events_at_time),
                time_step=self.time_step,
            ):
                logger.debug("Doing events")
                for cb in events_at_time:
                    try:
                        cb()
                    except Exception as e:
                        kind = getattr(cb, "_ev_kind", "other")
                        trace_idx = getattr(cb, "_ev_trace_idx", None)
                        self._fail(
                            "Event callback failed at sim_time={} kind={} trace_idx={}: {}"
                            .format(self.current_time, kind, trace_idx, e)
                        )
                        raise

            if self.time_step == 0 and has_submits:
                logger.info(
                    "Initial submit bucket processed at time %s; allowing scheduler events to settle before quiescence probe",
                    self.current_time,
                )
                if self.faketime_controller is None:
                    time.sleep(0.5)

            logger.debug("Sampling KVS")
            if self.kvs_sample_every and (self.time_step % self.kvs_sample_every == 0):
                self.sample_kvs_stats()

            if self.time_step == 0:
                print("")
            self.time_step += 1
            self.write_status_snapshot(state="running")

            expect = self.step_expect.get(self.current_time, {"submits": 0, "finishes": 0})
            self._bump_epoch_for_expect(expect)
            self._add_pending_quiescent_expect(expect)
            if self.current_time in self.step_expect:
                del self.step_expect[self.current_time]

            next_event_time = self._next_event_time()
            if self._should_defer_quiescent(next_event_time):
                gap = float(next_event_time) - float(self.current_time)
                logger.debug(
                    "Deferring quiescent probe at sim_time=%s; next event arrives in %.6fs",
                    self.current_time,
                    gap,
                )
                continue

            logger.debug("Querying Quiescent")
            try:
                self._send_quiescence_probe()
            except Exception as e:
                self._fail(
                    "Quiescent query failed at sim_time={}: {}"
                    .format(self.current_time, e)
                )
                raise RuntimeError("Quiescent broke") from e
            return

    # def is_quiescent(self):
    #     '''
    #     Checks for some conditions that imply the system is not quiescent
    #     '''
    #     return self.job_manager_quiescent and len(self.pending_inactivations) == 0

    def quiescent_cb(self, reply_epoch, bucket_time, span_id=None):
        '''
        Calls upon the scheduler to see if it is idle
        '''
        logger.debug("Hit quiescent")
        logger.info("Quiescent confirmed by jobtap")
        self.quiescence_inflight = False
        self.quiescence_epoch = None
        self.quiescence_bucket_time = None
        stale = reply_epoch != self.current_epoch or bucket_time != float(self.current_time)
        self.telemetry.end_span(
            span_id,
            sim_time=self.current_time,
            reply_epoch=reply_epoch,
            current_epoch=self.current_epoch,
            stale=stale,
        )
        if stale:
            self.stale_quiescence_replies += 1
            logger.info(
                "Ignoring stale quiescence reply epoch=%s bucket=%s; current epoch=%s bucket=%s",
                reply_epoch,
                bucket_time,
                self.current_epoch,
                self.current_time,
            )
            if not self.failed_reason:
                self._send_quiescence_probe()
            return
        self.last_quiescent_epoch = reply_epoch
        self.job_manager_quiescent = True
        if self.failed_reason:
            return
        self.advance()


    def post_verification(self):
        '''
        This function looks to make sure all jobs have run to completion before program ends
        If they have not, it likely means an issue with the emulator
        As a result, job event log will be output in the logger for each job that didn't complete
        '''
        for jobid, job in self.job_map.items():
            if 'INACTIVE' not in job.state_transitions:
                logger.warning(
                    "Job {} had not reached the inactive state by simulation termination time.".format(jobid))
                
                eventlog = self.adapter.get_eventlog(jobid)
                logger.debug(f"Job ID: {self.adapter.get_formatted_id(eventlog['id'])}")
                lines = eventlog["eventlog"].strip().split("\n")
                for line in lines:
                    parsed = json.loads(line)
                    pretty_str = json.dumps(parsed, indent=4)
                    logger.debug(pretty_str)

    def dump_eventlog(self):
        """
        Print job eventlog to CSV.

        Many Flux eventlog entries have name="event" and the verb in "type".
        Prefer "type", with a fallback to "name".
        """
        fieldnames = [
            "jobid", "submit", "validate", "depend", "priority",
            "alloc", "start", "finish", "release", "free", "clean"
        ]

        with open(f"{self.output_dir}eventlog.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for jobid, job in self.job_map.items():
                eventlog = self.adapter.get_eventlog(jobid)

                row = {"jobid": jobid}
                for event in fieldnames[1:]:
                    row[event] = ""

                lines = (eventlog.get("eventlog") or "").strip().split("\n")
                for line in lines:
                    if not line.strip():
                        continue
                    parsed = json.loads(line)

                    # Prefer "type" (common), fall back to "name" (sometimes holds the verb)
                    evt = (parsed.get("type") or parsed.get("name") or "").lower()

                    if evt in row and not row[evt]:
                        row[evt] = parsed.get("timestamp", "")

                writer.writerow(row)
