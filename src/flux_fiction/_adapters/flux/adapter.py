# flux_fiction/_adapters/flux/adapter.py
from __future__ import annotations
import flux
import json
import logging
from dataclasses import asdict, is_dataclass
from collections.abc import Callable
from . import stats
from . import journal
from . import modules
from . import resources
from . import watchers

logger = logging.getLogger(__name__)


def _coerce_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "ok"):
            return True
        if lowered in ("0", "false", "no"):
            return False
    return None


def _infer_satisfiable(response, default=None):
    if isinstance(response, dict):
        for key in ("satisfiable", "feasible"):
            if key in response:
                value = _coerce_bool(response[key])
                if value is not None:
                    return value
        if "errnum" in response:
            try:
                return int(response["errnum"]) == 0
            except Exception:
                pass
    return default

class FluxAdapter:
    def __init__(self) -> None:
        self._handle: flux.Flux | None = None
        self._watchers = None
        self._services = None
        self.simulation = None
        self.consumer = None
        self._pending_start_msgs: dict[int, object] = {}
        self._pending_complete_msgs: dict[int, object] = {}

    def open(self, simulation) -> None:
        if self._handle is None:
            self._handle = flux.Flux()
        if simulation is None: 
            raise ValueError("valid Simulation object required to start FluxAdapter")
        self.simulation = simulation
        modules.reset_jobtap_plugin(
            self._handle,
            keep_timestep=False,
            batch_job_starts=self.simulation.batch_job_starts,
            log_enabled=self.simulation.jobtap_logging,
            otel_enabled=self.simulation.otel_enabled,
            otel_socket=self.simulation.otel_bridge_socket,
            otel_service_name=self.simulation.otel_service_name + "-jobtap",
        )

    def close(self) -> None:
        for job in self._pending_start_msgs:
            logger.warning(f"Job {job} was allocated but not started at the end of simulation")
            # self.cancel_job(job)
        for job in self._pending_complete_msgs:
            logger.warning(f"Job {job} was started but did not complete at the end of simulation")
            # self.cancel_job(job)
        if self._handle is None:
            return
        modules.reset_jobtap_plugin(
            self._handle,
            keep_timestep=False,
            batch_job_starts=True,
            log_enabled=False,
            otel_enabled=False,
            otel_socket=None,
            otel_service_name="flux-fiction-jobtap",
        )
        if self._watchers is not None:
            watchers.teardown_watchers(self._handle, self._watchers, self._services or set())
        self._watchers = None
        self._services = None

    #TODO See if I can use this with the exec module? Then again it might be good to have a standard interface from Flux to engine and then from engine to the exec system. 

    def install_resources(self, cfg):
        ''''''
        if cfg.resource_R:
            resources.insert_resource_R_from_json(self._handle, cfg.resource_R)
        elif cfg.resource_file:
            resource_obj = resources.load_json_file(cfg.resource_file)
            if resources.is_resource_r_json(resource_obj):
                resources.insert_resource_R_obj(
                    self._handle,
                    resource_obj,
                    source_path=cfg.resource_file,
                )
            else:
                resources.insert_resource_data(
                    self._handle,
                    cfg.nnodes,
                    cfg.ncpus,
                    gpus_per_rank=cfg.ngpus,
                    scheduling_obj=resource_obj,
                )
        else:
            resources.insert_resource_data(
                self._handle,
                cfg.nnodes,
                cfg.ncpus,
                gpus_per_rank=cfg.ngpus,
            )

    def describe_resources(self, cfg):
        return resources.describe_resource_config(cfg)
    
    def reload_scheduler(self, cfg):
        ''''''
        modules.reload_modules(self._handle, cfg.config_json)

        modules.load_missing_modules(self._handle)
    
    def register_exec_service(self):
        ''''''
        watchers.exec_hello(self._handle)

    def register_job_tracking(self):
        self.consumer = journal.setup_journal(self._handle)
    
    def arm_watchers(self):
        ''''''
        self._watchers, self._services = watchers.setup_watchers(self._handle, self.simulation.start_job, self._pending_start_msgs, self._pending_complete_msgs, batch_job_starts=self.simulation.batch_job_starts)
    
    def start_reactor(self):
        ''''''
        try: 
            self._handle.reactor_run(self._handle.get_reactor(), 0)
        except Exception as e:
            raise RuntimeError("Failed to run Flux reactor") from e


    def stop_reactor(self):
        ''''''
        self._handle.reactor_stop(self._handle.get_reactor())
            
    def get_kvs_stats(self) -> dict:
        return stats.get_kvs_stats(self._handle)
    
    def query_quiescent(self, json_string, return_cb):
        logger.debug("Querying quiescent")
        try:
            self._handle.rpc(
                "job-manager.emu-jobtap.quiescent",
                payload=json_string).then(safe_then(return_cb), arg=None)
        except Exception as e:
            raise RuntimeError("Failed to query quiescent") from e

    def accumulate_quiescent(self, json_string):
        logger.debug("Accumulating quiescent expectations")
        try:
            self._handle.rpc(
                "job-manager.emu-jobtap.accumulate",
                payload=json_string,
            ).get()
        except Exception as e:
            raise RuntimeError("Failed to accumulate quiescent expectations") from e
          
    
    def get_eventlog(self, jobid):
        return flux.job.job_kvs_lookup(self._handle, _lookup_jobid(jobid), keys=["eventlog"])

    def get_job_diagnostics(self, jobid):
        diag = {"id": jobid}
        lookup_jobid = _lookup_jobid(jobid)

        try:
            diag["formatted_id"] = self.get_formatted_id(jobid)
        except Exception as e:
            diag["formatted_id_error"] = repr(e)

        try:
            diag["eventlog"] = self.get_eventlog(jobid)
        except Exception as e:
            diag["eventlog_error"] = repr(e)

        try:
            from flux.job.list import get_job
            diag["job_info"] = _make_serializable(get_job(self._handle, lookup_jobid))
        except Exception as e:
            diag["job_info_error"] = repr(e)

        try:
            diag["kvs"] = _make_serializable(
                flux.job.job_kvs_lookup(
                    self._handle,
                    lookup_jobid,
                    keys=["jobspec", "R", "eventlog"],
                )
            )
        except Exception as e:
            diag["kvs_error"] = repr(e)

        try:
            diag.update(_make_serializable(modules.queue_status(self._handle)))
        except Exception as e:
            diag["queue_status_error"] = repr(e)

        for key, method in (
            ("qmanager_stats", "sched-fluxion-qmanager.stats-get"),
            ("qmanager_params", "sched-fluxion-qmanager.params"),
            ("resource_match_stats", "sched-fluxion-resource.stats-get"),
        ):
            try:
                diag[key] = _make_serializable(self._handle.rpc(method).get())
            except Exception as e:
                diag[f"{key}_error"] = repr(e)

        return diag

    def check_jobspec_satisfiability(self, jobspec_json):
        try:
            jobspec_obj = json.loads(jobspec_json)
        except Exception as e:
            return {"error": "invalid jobspec JSON: {}".format(repr(e))}

        attempts = []
        for method in (
            "sched-fluxion-resource.satisfiability",
            "feasibility.check",
        ):
            try:
                response = self._handle.rpc(
                    method,
                    {"jobspec": jobspec_obj},
                ).get()
                return {
                    "satisfiable": _infer_satisfiable(response, default=True),
                    "method": method,
                    "response": _make_serializable(response),
                }
            except Exception as e:
                attempts.append({"method": method, "error": repr(e)})

        try:
            response = self._handle.rpc(
                "sched-fluxion-resource.match",
                {
                    "cmd": "satisfiability",
                    "jobid": -1,
                    "jobspec": jobspec_json,
                },
            ).get()
            return {
                "satisfiable": _infer_satisfiable(response, default=True),
                "method": "sched-fluxion-resource.match",
                "response": _make_serializable(response),
                "attempts": attempts,
            }
        except Exception as e:
            attempts.append({
                "method": "sched-fluxion-resource.match",
                "error": repr(e),
            })

        return {"satisfiable": None, "attempts": attempts}
    
    def get_formatted_id(self, job_id):
        try:
            return flux.job.JobID(job_id).f58
        except Exception:
            return str(job_id)

    def nodelist_lookup(self, jobid) -> list[int]:
        if self._handle is not None:
            nodes = stats.flux_nodelist_by_id(self._handle, _lookup_jobid(jobid))
            return nodes, "flux_nodelist" if nodes else "missing"
        else:
            raise Exception("FluxAdapter.nodelist_lookup: flux handle is None")
        
    def submit_job(self, jobspec_json) -> int:
        return flux.job.submit(self._handle, jobspec_json)
                
    def cancel_job(self, jobid):
        return flux.job.RAW.cancel(self._handle, jobid, "Canceled by emulator")
    
    def ack_start(self,jobid):
        msg = self._pending_start_msgs.pop(jobid, None)
        self._pending_complete_msgs[jobid] = msg
        self._handle.respond(msg,payload={"id": jobid, "type": "start", "data": {}})

    def ack_complete(self, jobid):
        msg = self._pending_complete_msgs.pop(jobid, None)
        self._handle.respond(
            msg,
            payload={"id": jobid, "type": "finish", "data": {"status": 0}}
        )
        self._handle.respond(
            msg,
            payload={"id": jobid, "type": "release",
                     "data": {"ranks": "all", "final": True}}
        )
            

def safe_then(cb):
    def _wrapped(fut, arg):
        try:
            return cb(fut, arg)
        except Exception:
            logger.exception("Exception inside .then callback %s", cb.__name__)
            raise
    return _wrapped


def _make_serializable(obj):
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if is_dataclass(obj):
        return _make_serializable(asdict(obj))
    if isinstance(obj, dict):
        return {str(k): _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_make_serializable(v) for v in obj]
    if hasattr(obj, "__dict__"):
        return _make_serializable(vars(obj))
    return str(obj)


def _lookup_jobid(jobid):
    try:
        return int(jobid)
    except Exception:
        return int(flux.job.JobID(jobid))
