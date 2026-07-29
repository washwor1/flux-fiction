# flux_fiction/_adapters/base.py
from __future__ import annotations
from typing import Protocol, Any, Callable

class Adapter(Protocol):
    # def configure_backend(self, configuration: object, start_job_cb: function) -> None:
    #     '''Configure the backend to use given resource configuration. Requires reference to the Simulation\'s start job callback to properly handle interaction with the job manager.'''
    def open(self, simulation: object) -> None:
        '''Connect to the resource manager'''

    def close(self) -> None:
        '''Shutdown any persistent services on the backend (e.g., watchers, reactor) and disconnect from the resource manager'''
    
    def install_resources(self, cfg: object) -> None:
        '''Register emulated resources with the resource manager'''

    def describe_resources(self, cfg: object) -> dict:
        '''Return simulator-facing resource counts and jobspec shape'''

    def reload_scheduler(self, cfg: object) -> None:
        '''Restart the scheduler and associated modules'''

    def register_exec_service(self) -> None:
        '''Register the applicable emulated exec service'''

    def register_job_tracking(self) -> None:
        '''Setup logging for job state changes'''

    def arm_watchers(self) -> None:
        '''Setup and configure any watchers'''
    
    def start_reactor(self) -> None:
        '''Start the system reactor'''
    
    def stop_reactor(self) -> None:
        '''Stop the system reactor'''

    def get_kvs_stats(self) -> dict:
        '''Return the size of KVS (Flux specific)'''

    def query_quiescent(self, json_string: str, return_cb: Callable) -> None:
        '''Query whether Flux is quiescent'''
    
    def get_eventlog(self, jobid: int) -> dict[str, Any]:
        '''Get the eventlog for a job'''

    def get_job_diagnostics(self, jobid: int) -> dict[str, Any]:
        '''Return best-effort scheduler/job-manager diagnostics for a job'''

    def get_scheduler_state(self, jobid: int) -> dict[str, Any]:
        '''Return a best-effort scheduler-facing state snapshot for a job'''

    def check_jobspec_satisfiability(self, jobspec_json: str) -> dict[str, Any]:
        '''Return best-effort scheduler satisfiability diagnostics for a jobspec'''

    def get_formatted_id(self, job_id: int) -> str:
        '''Get the jobid in f58 format'''
    
    def nodelist_lookup(self, jobid: int) -> list[int]:
        '''Get the nodelist for a job'''

    def submit_job(self, jobspec_json: str) -> int:
        '''Submit a new job to the resource manager'''

    def supports_async_submit(self) -> bool:
        '''Whether submit_job_async/submit_get_id are usable on this adapter.

        Adapters that cannot pipeline submissions return False and the engine
        transparently falls back to the blocking path.
        '''
        return False

    def submit_job_async(self, jobspec_json: str):
        '''Start a submission and return a handle, without waiting for the id.

        Every handle MUST be resolved through submit_get_id before the caller
        relies on any job id, and before the scheduler is told what to expect.
        '''
        raise NotImplementedError("adapter does not support asynchronous submit")

    def submit_get_id(self, handle) -> int:
        '''Block until an asynchronous submission yields its job id.'''
        raise NotImplementedError("adapter does not support asynchronous submit")

    def cancel_job(self, jobid: int) -> None:
        '''Cancel the job with {jobid} job id'''
    
    def ack_complete(self, jobid: int) -> None:
        '''Send '''
    
    def ack_start(self,jobid: int) -> None:
        ''''''
