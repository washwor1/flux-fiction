# notes: I moved queue wait time into Job
from collections import namedtuple
from typing import Sequence
import math
import os
import re
from datetime import datetime, timedelta
import csv
from abc import ABC, abstractmethod
import logging
import time
import json
from flux_fiction._adapters.base import Adapter

logger = logging.getLogger(__name__)

TIME_QUANTUM = 1e-6

def qtime(t) -> float:
    return round(float(t) / TIME_QUANTUM) * TIME_QUANTUM

_event_seq_counter = 0

def make_tagged_cb(kind, job, fn, time_value):
    """Wrap a callback so we can log its kind and identity later."""
    global _event_seq_counter
    seq = _event_seq_counter
    _event_seq_counter += 1

    def cb():
        return fn()

    # attach debug metadata to the function object
    cb._ev_kind = kind
    cb._ev_time = time_value
    cb._ev_seq_add = seq
    cb._ev_jobid = getattr(job, "jobid", None) if job else None
    cb._ev_trace_idx = getattr(job, "trace_index", None) if job else None
    return cb

def create_resource(res_type, count, with_child=None):
    '''
    Creates a resource dictionary for a Job

    Note: 'count' variable must be of type int. Otherwise it will cause issues during scheduling.
    '''
    assert isinstance(count, int) and count > 0
    res = {"type": res_type, "count": count}
    if with_child:
        assert isinstance(with_child, Sequence) and not isinstance(with_child, str)
        res["with"] = list(with_child)
    return res


def create_slot(label, count, with_child):
    '''
    Helper function for creating the slot section of a jobspec for a Job
    '''
    slot = create_resource("slot", math.ceil(count), with_child or [])
    slot["label"] = label
    return slot

class Job(object):
    '''
    Class to track individual jobs within the emulator
    '''
    def __init__(
        self,
        nnodes,
        ncpus,
        submit_time,
        elapsed_time,
        timelimit,
        exitcode=0,
        ngpus=0,
        rabbit_storage_gib=0.0,
        gap=0.0,
        end_latency=0.0,
    ):
        self.nnodes = nnodes
        self.exclusive = False
        self.cores_per_node = None
        self.gpus_per_node = None
        self.ncpus = ncpus
        self.ngpus = int(ngpus or 0)
        self.rabbit_storage_gib = float(rabbit_storage_gib or 0.0)
        self.submit_time = submit_time
        self.elapsed_time = elapsed_time
        self.timelimit = timelimit
        self.exitcode = exitcode
        self.start_time = None
        self.state_transitions = {}
        self._jobid = None
        self._jobspec = None
        self._submit_future = None
        self.trace_index = None     # set from reader order (see below)
        self.real_submit = None     # time.time() at actual submit()
        self.real_start  = None     # time.time() when sim_exec.start processed
        self.real_finish = None     # time.time() when complete_job() runs
        self.jobspec_intermediate_types = []
        self.jobspec_intermediate_counts = {}
        self.rabbit_storage_resource_type = "ssd"
        self.rabbit_storage_parent_type = None
        self.rabbit_storage_nodes_per_parent = 0
        self.rabbit_storage_shares_per_parent = 0
        self.rabbit_storage_share_gib = 0.0
        self.rabbit_storage_parent_gib = 0.0
        self.rabbit_storage_share_count = 0
        self.rabbit_storage_request_count = 0
        self.rabbit_storage_emit_dw = False
        self.rabbit_storage_name = "rabbit"

        self.gap = float(gap or 0.0)
        self.end_latency = float(end_latency or 0.0)

    @property
    def jobspec(self):
        if self._jobspec is not None:
            return self._jobspec

        if self.exclusive:
            # request full node capacity
            total_cores = int(self.cores_per_node)
            total_gpus = int(self.gpus_per_node or 0)
        else:
            assert self.ncpus % self.nnodes == 0
            total_cores = math.ceil(self.ncpus / self.nnodes)
            total_gpus = 0
            if self.ngpus:
                if self.ngpus % self.nnodes != 0:
                    logger.warning(
                        "NGPUS ({}) not divisible by NNodes ({}); rounding up per-node request"
                        .format(self.ngpus, self.nnodes)
                    )
                total_gpus = math.ceil(self.ngpus / self.nnodes)

        branch_factor = 1
        for res_type in self.jobspec_intermediate_types:
            branch_factor *= int(self.jobspec_intermediate_counts.get(res_type, 1) or 1)

        core = create_resource("core", max(1, math.ceil(total_cores / branch_factor)))
        withs = [core]
        if total_gpus:
            gpu = create_resource("gpu", max(1, math.ceil(total_gpus / branch_factor)))
            withs.append(gpu)

        for res_type in reversed(self.jobspec_intermediate_types):
            count = int(self.jobspec_intermediate_counts.get(res_type, 1) or 1)
            withs = [create_resource(res_type, count, withs)]

        slot = create_slot("task", 1, withs)
        node_section = create_resource("node", self.nnodes, [slot]) if self.nnodes > 0 else slot
        if self.exclusive and self.nnodes > 0:
            node_section["exclusive"] = True

        resource_sections = self._resource_sections_with_rabbit_storage(node_section)
        attributes = {"system": {"duration": self.timelimit}}
        if self.rabbit_storage_emit_dw and self.rabbit_storage_gib > 0:
            capacity_gib = max(1, math.ceil(self.rabbit_storage_gib / max(1, self.nnodes)))
            attributes["system"]["dw"] = (
                "#DW jobdw type=xfs capacity={}GiB name={}"
                .format(capacity_gib, self.rabbit_storage_name)
            )
        jobspec = {
            "version": 1,
            "resources": resource_sections,
            "tasks": [{
                "command": ["command", "200"],
                "slot": "task",
                "count": {"per_slot": 1},
            }],
            "attributes": attributes,
        }
        self._jobspec = jobspec
        return self._jobspec


    def _resource_sections_with_rabbit_storage(self, node_section):
        force_top_slot = os.environ.get("FF_FORCE_TOP_LEVEL_RABBIT_SLOT", "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if self.rabbit_storage_request_count <= 0:
            if force_top_slot:
                return [create_slot("rabbit", 1, [node_section])]
            return [node_section]

        ssd_section = create_resource(
            self.rabbit_storage_resource_type,
            int(self.rabbit_storage_request_count),
        )
        ssd_section["exclusive"] = True
        bundle_count = 1
        if self.rabbit_storage_parent_type:
            nodes_per_parent = int(self.rabbit_storage_nodes_per_parent or self.nnodes or 1)
            storage_per_parent = float(
                self.rabbit_storage_parent_gib
                or (self.rabbit_storage_shares_per_parent * self.rabbit_storage_share_gib)
                or self.rabbit_storage_request_count
                or 1
            )
            bundle_count = max(
                1,
                math.ceil(self.nnodes / nodes_per_parent),
                math.ceil(self.rabbit_storage_request_count / storage_per_parent),
            )
            node_section = dict(node_section)
            node_section["count"] = max(1, math.ceil(self.nnodes / bundle_count))
            ssd_section["count"] = max(
                1,
                math.ceil(self.rabbit_storage_request_count / bundle_count),
            )

        return [create_slot("rabbit", bundle_count, [node_section, ssd_section])]


    def set_jobspec_shape(self, shape):
        shape = shape or {}
        self.jobspec_intermediate_types = [
            res_type for res_type in shape.get("intermediate_types", [])
            if res_type
        ]
        self.jobspec_intermediate_counts = {
            str(key): int(value)
            for key, value in shape.get("intermediate_counts", {}).items()
            if value
        }
        self._jobspec = None


    def set_rabbit_storage_shape(self, storage, *, emit_dw=False, name="rabbit"):
        storage = storage or {}
        self.rabbit_storage_resource_type = storage.get("resource_type") or "ssd"
        self.rabbit_storage_parent_type = storage.get("parent_type")
        self.rabbit_storage_nodes_per_parent = int(storage.get("nodes_per_parent") or 0)
        self.rabbit_storage_shares_per_parent = int(storage.get("shares_per_parent") or 0)
        self.rabbit_storage_share_gib = float(storage.get("share_gib") or 0.0)
        self.rabbit_storage_parent_gib = float(
            storage.get("max_parent_gib")
            or (
                float(storage.get("shares_per_parent") or 0)
                * float(storage.get("share_gib") or 0)
            )
            or 0.0
        )
        self.rabbit_storage_emit_dw = bool(emit_dw)
        self.rabbit_storage_name = str(name or "rabbit")
        if self.rabbit_storage_gib > 0 and self.rabbit_storage_share_gib > 0:
            self.rabbit_storage_share_count = math.ceil(
                self.rabbit_storage_gib / self.rabbit_storage_share_gib
            )
        else:
            self.rabbit_storage_share_count = 0
        self.rabbit_storage_request_count = (
            math.ceil(self.rabbit_storage_gib)
            if self.rabbit_storage_gib > 0
            else 0
        )
        self._jobspec = None


    def set_exclusive(self, cores_per_node, gpus_per_node=0):
        self.exclusive = True
        self.cores_per_node = int(cores_per_node)
        self.gpus_per_node = int(gpus_per_node or 0)
        self._jobspec = None


    def submit(self, adapter: Adapter):
        jobspec_json = json.dumps(self.jobspec)
        logger.log(9, jobspec_json)
        self.real_submit = time.time()
        try:
            self._jobid = adapter.submit_job(jobspec_json)
        except Exception as e:
            details = (
                "trace_idx={trace_idx} nnodes={nnodes} ncpus={ncpus} "
                "ngpus={ngpus} rabbit_storage_gib={rabbit_gib:.3f} "
                "rabbit_shares={rabbit_shares} rabbit_request_count={rabbit_request_count}"
            ).format(
                trace_idx=self.trace_index,
                nnodes=self.nnodes,
                ncpus=self.ncpus,
                ngpus=self.ngpus,
                rabbit_gib=self.rabbit_storage_gib,
                rabbit_shares=self.rabbit_storage_share_count,
                rabbit_request_count=self.rabbit_storage_request_count,
            )
            raise RuntimeError(
                "Job submit failed for {}: {}\nJobspec JSON:\n{}"
                .format(details, e, jobspec_json)
            ) from e
        logger.debug("Submitted job id %s", self._jobid)


    @property
    def jobid(self):
        return self._jobid


    @property
    def complete_time(self):
        if self.start_time is None:
            raise ValueError("Job has not started yet")
        return self.start_time + self.elapsed_time

    def start(self, adapter: Adapter, start_time):
        '''
        Records the time that the job was started by Flux and tells the job manager that the request is being handled
        '''
        self.mark_started(start_time)
        self.ack_start(adapter)


    def mark_started(self, start_time):
        self.start_time = qtime(start_time)


    def ack_start(self, adapter: Adapter):
        adapter.ack_start(self.jobid)


    def complete(self, adapter: Adapter):
        '''
        Emits the finish and release events when a job is complete
        '''
        adapter.ack_complete(self.jobid)

    def cancel(self, adapter: Adapter):
        '''
        Emits the cancel event for a job
        '''
        return adapter.cancel_job(self.jobid)


    def insert_apriori_events(self, simulation):
        '''
        Adds the submit times for every job into the event list

        This defines the order in which jobs are submitted to flux
        '''
        simulation.step_expect[self.submit_time]["submits"] += 1
        cb = make_tagged_cb("submit", self, lambda: simulation.submit_job(self), self.submit_time)
        simulation.add_event(self.submit_time, cb)

    def record_state_transition(self, state, time):
        '''
        Adds the time that a job state transition occurred to a dict "state_transitions"
        '''
        self.state_transitions[state] = time

    def queue_wait_time(self) -> float:
        """Sim-time queue wait: STARTED - SUBMITTED (no runtime)."""
        sub = self.state_transitions.get("SUBMITTED", None)
        sta = self.state_transitions.get("STARTED", None)
        if sub in (None, "") or sta in (None, ""):
            return 0.0
        return max(0.0, float(sta) - float(sub))


def datetime_to_epoch(dt):
    """Convert a datetime to a Unix epoch. Returns float to preserve
    sub-second precision when available."""
    return (dt - datetime(1970, 1, 1)).total_seconds()


re_dhms = re.compile(r"^\s*(\d+)[:-](\d+):(\d+):(\d+(?:\.\d+)?)\s*$")
re_hms = re.compile(r"^\s*(\d+):(\d+):(\d+(?:\.\d+)?)\s*$")

def walltime_str_to_timedelta(walltime_str):
    (days, hours, mins, secs) = (0, 0, 0, 0.0)
    match = re_dhms.search(walltime_str)
    if match:
        days = int(match.group(1))
        hours = int(match.group(2))
        mins = int(match.group(3))
        secs = float(match.group(4))
    else:
        match = re_hms.search(walltime_str)
        if match:
            hours = int(match.group(1))
            mins = int(match.group(2))
            secs = float(match.group(3))
    return timedelta(days=days, hours=hours, minutes=mins, seconds=secs)


class JobTraceReader(ABC):
    '''
    Class that is used to ingest job traces
    '''
    def __init__(self, tracefile):
        self.tracefile = tracefile

    @abstractmethod
    def validate_trace(self):
        pass

    @abstractmethod
    def read_trace(self):
        pass


def _parse_submit_time(row) -> float:
    """Extract submit time from a trace row with sub-second precision.

    Priority:
      1. t_submit column (raw epoch float) — exact, no parsing ambiguity.
      2. Submit string with microseconds  (%Y-%m-%dT%H:%M:%S.%f)
      3. Submit string without fractional seconds (legacy format)
    """
    raw = row.get("t_submit", "")
    if raw not in (None, ""):
        try:
            return qtime(float(raw))
        except (ValueError, TypeError):
            pass

    submit_str = row.get("Submit", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(submit_str, fmt)
            return qtime(datetime_to_epoch(dt))
        except ValueError:
            continue

    raise ValueError(f"Cannot parse submit time from row: Submit={submit_str!r}, t_submit={raw!r}")


def _parse_elapsed(row) -> float:
    """Extract elapsed time with sub-second precision.

    Priority:
      1. elapsed_s column (raw float seconds) — exact.
      2. Elapsed column parsed as HH:MM:SS[.fff] via walltime_str_to_timedelta.
    """
    raw = row.get("elapsed_s", "")
    if raw not in (None, ""):
        try:
            return float(raw)
        except (ValueError, TypeError):
            pass

    return walltime_str_to_timedelta(row["Elapsed"]).total_seconds()


def _parse_float_field(row, field_name, default=0.0) -> float:
    raw = row.get(field_name, "")
    if raw in (None, "", "0"):
        return float(default)
    try:
        return float(raw)
    except Exception:
        logger.warning("Invalid %s value '%s'; treating as %s", field_name, raw, default)
        return float(default)


_RABBIT_STORAGE_FIELDS = [
    "RabbitStorageGiB",
    "RabbitGiB",
    "RABBIT_STORAGE_GIB",
    "RABBIT_GIB",
    "SSD_GIB",
]

_LEGACY_RABBIT_STORAGE_FIELDS = [
    "RabbitStorageGB",
    "RabbitGB",
    "RABBIT_STORAGE_GB",
    "RABBIT_GB",
    "SSD_GB",
]

def _storage_value_to_gib(raw) -> float:
    if raw in (None, "", "0"):
        return 0.0
    text = str(raw).strip()
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*(GiB)?$", text, re.IGNORECASE)
    if not match:
        logger.warning(
            "Invalid Rabbit storage value '%s'; treating as 0. Rabbit storage must be expressed in GiB.",
            raw,
        )
        return 0.0

    return float(match.group(1))


def _parse_rabbit_storage_gib(row) -> float:
    lower_to_key = {str(key).lower(): key for key in row.keys()}
    for field in _RABBIT_STORAGE_FIELDS:
        key = lower_to_key.get(field.lower())
        if key is None:
            continue
        raw = row.get(key, "")
        return _storage_value_to_gib(raw)
    for field in _LEGACY_RABBIT_STORAGE_FIELDS:
        key = lower_to_key.get(field.lower())
        if key is None:
            continue
        logger.warning(
            "Rabbit storage field '%s' is no longer supported; use a GiB-named field such as RabbitGiB.",
            key,
        )
        return 0.0
    return 0.0


def job_from_slurm_row(row):
    '''
    generates a Job class from a sacct style job trace
    '''
    kwargs = {}
    if "ExitCode" in row and row["ExitCode"]:
        try:
            kwargs["exitcode"] = int(str(row["ExitCode"]).split(":")[0])
        except Exception:
            kwargs["exitcode"] = 0

    submit_time = _parse_submit_time(row)

    elapsed = _parse_elapsed(row)
    if elapsed <= 0:
        logger.warning("Elapsed time ({}) <= 0".format(elapsed))

    timelimit = walltime_str_to_timedelta(row["Timelimit"]).total_seconds()
    if elapsed > timelimit:
        logger.warning(
            "Elapsed time ({}) greater than Timelimit ({})".format(
                elapsed, timelimit)
        )

    nnodes = int(row["NNodes"])
    ncpus = int(row["NCPUS"])
    if nnodes > ncpus:
        logger.warning(
            "Number of Nodes ({}) greater than Number of CPUs ({}), setting NCPUS = NNodes".format(
                nnodes, ncpus
            )
        )
        ncpus = nnodes
    elif ncpus % nnodes != 0:
        old_ncpus = ncpus
        ncpus = math.ceil(ncpus / nnodes) * nnodes
        logger.warning(
            "Number of Nodes ({}) does not evenly divide the Number of CPUs ({}), setting NCPUS to an integer multiple of the number of nodes ({})".format(
                nnodes, old_ncpus, ncpus
            )
        )

    ngpus = 0
    if "NGPUS" in row and row["NGPUS"] not in (None, "", "0"):
        try:
            ngpus = int(row["NGPUS"])
        except Exception:
            logger.warning("Invalid NGPUS value '{}'; treating as 0".format(row["NGPUS"]))
            ngpus = 0

    rabbit_storage_gib = _parse_rabbit_storage_gib(row)

    submit_latency_s = _parse_float_field(row, "submit_latency_s", default=0.0)
    sched_latency_s = _parse_float_field(row, "sched_latency_s", default=0.0)
    launch_latency_s = _parse_float_field(row, "launch_latency_s", default=0.0)

    if sched_latency_s > 0.1:
        sched_latency_s = 0.05

    gap = submit_latency_s + sched_latency_s + launch_latency_s
    # print(f"{submit_time}, submit: {submit_latency_s}, sched {sched_latency_s}, launch: {launch_latency_s}, total gap: {gap}")

    finish_to_clean_latency_s = _parse_float_field(row, "finish_to_clean_latency_s", default=0.0)

    return Job(
        nnodes,
        ncpus,
        submit_time,
        elapsed,
        timelimit,
        None,
        ngpus,
        rabbit_storage_gib=rabbit_storage_gib,
        gap=gap,
        end_latency=finish_to_clean_latency_s,
        **kwargs,
    )


class SacctReader(JobTraceReader):
    required_fields_base = ["Elapsed", "Timelimit", "Submit", "NNodes", "NCPUS"]

    def __init__(self, tracefile, require_gpus=False):
        super(SacctReader, self).__init__(tracefile)
        self.require_gpus = require_gpus
        self.determine_delimiter()

    def determine_delimiter(self):
        """
        sacct outputs data with '|' as the delimiter by default, but ',' is a more
        common delimiter in general. This is a simple heuristic to figure out if
        the job trace is straight from sacct or has had some post-processing
        done that converts the delimiter to a comma.
        """
        with open(self.tracefile) as infile:
            first_line = infile.readline()
        self.delim = '|' if '|' in first_line else ','

    def validate_trace(self):
        with open(self.tracefile) as infile:
            reader = csv.reader(infile, delimiter=self.delim, skipinitialspace=True)
            header_fields = set(next(reader))
        required_fields = list(SacctReader.required_fields_base)
        if self.require_gpus:
            required_fields.append("NGPUS")
        for req_field in required_fields:
            if req_field not in header_fields:
                raise ValueError("Job file is missing '{}'".format(req_field))

    def read_trace(self):
        """
        You can obtain the necessary information from the sacct command using the -o flag.
        For example: sacct -o nnodes,ncpus,timelimit,state,submit,elapsed,exitcode[,ngpus]
        """
        with open(self.tracefile) as infile:
            lines = [line for line in infile.readlines()
                     if not line.startswith('#')]
            reader = csv.DictReader(lines, delimiter=self.delim, skipinitialspace=True)
            jobs = [job_from_slurm_row(row) for row in reader]
        return jobs

Makespan = namedtuple('Makespan', ['beginning', 'end'])
