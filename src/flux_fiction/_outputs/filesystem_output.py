from typing import Sequence, Union, List
import re
import json 
import csv
from flux_fiction._adapters.base import Adapter

def dump_transitions_to_csv(simulation, filename, adapter: Adapter):
    """
    Writes job transitions CSV and adds NODELIST (comma-separated ints).
    Nodelist is resolved via Flux job-list.list-id.
    """
    if filename is None: 
        raise RuntimeError("dump_transitions_to_csv: You didn't pass a filename for the job transition CSV")
    def f(x):
        return "" if x in (None, "") else f"{float(x):.6f}"

    rows = []
    for jobid, job in simulation.job_map.items():
        nodes, _ = adapter.nodelist_lookup(jobid)
        rows.append({
            "trace_idx": job.trace_index,
            "jobid": jobid,
            "nnodes": job.nnodes,
            "SUBMIT": f(job.state_transitions.get("SUBMITTED", "")),
            "START":  f(job.state_transitions.get("STARTED", "")),
            "FINISH": f(job.state_transitions.get("COMPLETED", "")),
            "REAL_SUBMIT": f(job.real_submit),
            "REAL_START":  f(job.real_start),
            "REAL_FINISH": f(job.real_finish),
            "FLUX_OBSERVED_START": f(job.flux_observed_start),
            "NODELIST": ",".join(str(n) for n in nodes),
        })

    rows.sort(key=lambda r: (
        r["trace_idx"] if r["trace_idx"] is not None else 10**9,
        float(r["REAL_SUBMIT"]) if r["REAL_SUBMIT"] else float("inf"))
    )

    fieldnames = ["trace_idx", "jobid", "nnodes",
                  "SUBMIT", "START", "FINISH",
                  "REAL_SUBMIT", "REAL_START", "REAL_FINISH",
                  "FLUX_OBSERVED_START",
                  "NODELIST"]
    with open(filename, "w", newline="") as csvfile:
        w = csv.DictWriter(csvfile, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def _sec_to_us(x: Union[str, float, int]) -> int:
    if x in ("", None):
        return 0
    return int(float(x) * 1_000_000.0)

def write_per_node_chrome_trace(simulation, out_path, adapter: Adapter, pid: int = 1):
    """
    Generates a Chrome/Perfetto trace with one lane per node:
      - tid = node index
      - name = job{trace_idx}:{jobid}
      - ts/dur from START..FINISH (sim time only; no submit/real times)

    Open the JSON in https://ui.perfetto.dev to see an occupancy chart.
    """
    if out_path is None: 
        raise RuntimeError("write_per_node_chrome_trace: You didn't pass a filename for the perfetto traces")
    
    events = []
    threads_emitted = set()

    # Process label
    events.append({
        "name": "process_name", "ph": "M", "pid": pid,
        "args": {"name": "Cluster"}
    })
    
    # Build slices
    for jobid, job in simulation.job_map.items():
        start_s = job.state_transitions.get("STARTED", None)
        finish_s = job.state_transitions.get("COMPLETED", None)
        if start_s in (None, "") or finish_s in (None, ""):
            continue
        ts_us = _sec_to_us(start_s)
        dur_us = _sec_to_us(finish_s) - ts_us
        if dur_us <= 0:
            continue

        nodes, src = adapter.nodelist_lookup(jobid)
        if not nodes:
            continue

        for n in nodes:
            if n not in threads_emitted:
                events.append({
                    "name": "thread_name", "ph": "M", "pid": pid, "tid": int(n),
                    "args": {"name": f"node {n}"}
                })
                events.append({
                    "name": "thread_sort_index", "ph": "M", "pid": pid, "tid": int(n),
                    "args": {"sort_index": int(n)}
                })
                threads_emitted.add(n)

            events.append({
                "name": f"job{getattr(job, 'trace_index', '')}:{jobid}",
                "cat": "occupancy",
                "ph": "X",
                "pid": pid,
                "tid": int(n),
                "ts": ts_us,
                "dur": dur_us,
                "args": {"jobid": str(jobid), "trace_idx": getattr(job, "trace_index", None),
                         "nnodes": getattr(job, "nnodes", None), "_source": src}
            })

    trace_obj = {"traceEvents": events, "displayTimeUnit": "ms"}
    with open(out_path, "w") as f:
        json.dump(trace_obj, f)
