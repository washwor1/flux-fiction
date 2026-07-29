#!/usr/bin/env python
from __future__ import print_function

import argparse
import errno
import glob
import json
import math
import os
import signal
import tempfile
import time

import flux
import flux.kvs
import yaml

from flux_fiction._adapters.flux import modules


class MatchTimeout(RuntimeError):
    pass


def _alarm_handler(signum, frame):
    raise MatchTimeout("match RPC timed out")


def _read_json(path):
    with open(path, "r") as f:
        return json.load(f)


def _write_json(path, payload):
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def _write_faketime_offset(path, offset_seconds):
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=".{}.".format(os.path.basename(path)),
        suffix=".tmp",
        dir=parent or None,
        text=True,
    )
    try:
        with os.fdopen(fd, "w") as f:
            f.write("{:+.9f}s\n".format(float(offset_seconds)))
        try:
            os.replace(tmp_name, path)
        except OSError as exc:
            if exc.errno != errno.EBUSY:
                raise
            with open(path, "w") as f:
                f.write("{:+.9f}s\n".format(float(offset_seconds)))
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def _graph_nodes(resource_obj):
    scheduling = resource_obj.get("scheduling")
    if isinstance(scheduling, dict):
        graph = scheduling.get("graph")
        if isinstance(graph, dict):
            return graph.get("nodes", [])
    graph = resource_obj.get("graph")
    if isinstance(graph, dict):
        return graph.get("nodes", [])
    return []


def _normalize_legacy_storage_status(resource_obj):
    """Match Flux Fiction's resource.R normalization for legacy Rabbit graphs."""
    nodes = _graph_nodes(resource_obj)
    ssd_nodes = [
        node
        for node in nodes
        if isinstance(node.get("metadata"), dict)
        and node["metadata"].get("type") == "ssd"
    ]
    statuses = [node["metadata"].get("status") for node in ssd_nodes]
    changed = 0
    if statuses and all(status == 1 for status in statuses):
        for node in ssd_nodes:
            node["metadata"]["status"] = 0
            changed += 1
    return changed


def _stats(handle, method):
    try:
        return handle.rpc(method).get()
    except Exception as exc:
        return {"error": repr(exc)}


def _load_resource_and_scheduler(handle, resource_r, scheduler_json):
    resource_obj = _read_json(resource_r)
    normalized_ssd_vertices = _normalize_legacy_storage_status(resource_obj)
    flux.kvs.put(handle, "resource.R", resource_obj)
    flux.kvs.commit(handle)
    modules.reload_modules(handle, scheduler_json)
    return normalized_ssd_vertices


def _percentile(sorted_values, pct):
    if not sorted_values:
        return None
    idx = int(round((len(sorted_values) - 1) * pct))
    idx = max(0, min(len(sorted_values) - 1, idx))
    return sorted_values[idx]


def _summarize_times(values):
    if not values:
        return {
            "count": 0,
            "min_seconds": None,
            "max_seconds": None,
            "avg_seconds": None,
            "p50_seconds": None,
            "p95_seconds": None,
            "p99_seconds": None,
        }
    ordered = sorted(values)
    return {
        "count": len(values),
        "min_seconds": ordered[0],
        "max_seconds": ordered[-1],
        "avg_seconds": sum(values) / float(len(values)),
        "p50_seconds": _percentile(ordered, 0.50),
        "p95_seconds": _percentile(ordered, 0.95),
        "p99_seconds": _percentile(ordered, 0.99),
    }


def _response_summary(resp):
    if not isinstance(resp, dict):
        return {"response_type": type(resp).__name__}
    summary = {}
    for key in ("jobid", "status", "type", "at", "overhead", "errnum", "error"):
        if key in resp:
            summary[key] = resp[key]
    if "match" in resp and isinstance(resp["match"], dict):
        summary["match_keys"] = sorted(resp["match"].keys())
    if not summary:
        summary["keys"] = sorted(resp.keys())
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Measure direct sched-fluxion-resource match cost in a nested Flux instance."
    )
    parser.add_argument("--resource-r", required=True)
    parser.add_argument("--scheduler-json", required=True)
    parser.add_argument("--jobspec-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--per-match-timeout", type=float, default=120.0)
    parser.add_argument(
        "--cancel-each",
        action="store_true",
        help="Cancel each direct allocation/reservation immediately after measuring it.",
    )
    parser.add_argument(
        "--faketime-stamp-file",
        default=None,
        help="Optional libfaketime timestamp file to rewrite before matches.",
    )
    parser.add_argument(
        "--faketime-jump-seconds",
        type=float,
        default=0.0,
        help="If --faketime-stamp-file is set, advance this many fake seconds before each match.",
    )
    parser.add_argument(
        "--faketime-base-offset-seconds",
        type=float,
        default=0.0,
        help="Initial relative libfaketime offset used with --faketime-stamp-file.",
    )
    args = parser.parse_args()

    jobspecs = sorted(glob.glob(os.path.join(args.jobspec_dir, "job_*.json")))
    if args.limit and args.limit > 0:
        jobspecs = jobspecs[: args.limit]
    if not jobspecs:
        raise SystemExit("no jobspecs found in {}".format(args.jobspec_dir))

    handle = flux.Flux()
    normalized_ssd_vertices = _load_resource_and_scheduler(
        handle,
        args.resource_r,
        args.scheduler_json,
    )
    print(
        "loaded scheduler label={} normalized_ssd_vertices={}".format(
            args.label,
            normalized_ssd_vertices,
        ),
        flush=True,
    )

    before_stats = _stats(handle, "sched-fluxion-resource.stats-get")
    handle.rpc("sched-fluxion-resource.stats-clear").get()
    print("stats reset label={}".format(args.label), flush=True)

    per_match = []
    response_counts = {}
    errors = []
    faketime_offsets = []
    if args.faketime_stamp_file:
        _write_faketime_offset(
            args.faketime_stamp_file,
            args.faketime_base_offset_seconds,
        )
    t_start = time.perf_counter()
    if args.per_match_timeout and args.per_match_timeout > 0:
        signal.signal(signal.SIGALRM, _alarm_handler)
    for idx, path in enumerate(jobspecs, start=1):
        with open(path, "r") as f:
            jobspec_obj = yaml.safe_load(f)
        jobspec_str = yaml.dump(jobspec_obj)
        jobid = int(handle.rpc("sched-fluxion-resource.next_jobid").get()["jobid"])
        faketime_offset = None
        if args.faketime_stamp_file:
            faketime_offset = (
                float(args.faketime_base_offset_seconds)
                + float(args.faketime_jump_seconds) * float(idx - 1)
            )
            _write_faketime_offset(args.faketime_stamp_file, faketime_offset)
            faketime_offsets.append(faketime_offset)
        payload = {
            "cmd": "allocate_orelse_reserve",
            "jobid": jobid,
            "jobspec": jobspec_str,
        }
        print(
            "match_begin label={} idx={} jobid={} jobspec={}".format(
                args.label,
                idx,
                jobid,
                path,
            ),
            flush=True,
        )
        match_start = time.perf_counter()
        try:
            if args.per_match_timeout and args.per_match_timeout > 0:
                signal.alarm(int(math.ceil(args.per_match_timeout)))
            resp = handle.rpc("sched-fluxion-resource.match", payload).get()
            elapsed = time.perf_counter() - match_start
            signal.alarm(0)
            status = str(resp.get("status", resp.get("type", "unknown")))
            response_counts[status] = response_counts.get(status, 0) + 1
            record = {
                "idx": idx,
                "jobid": jobid,
                "jobspec": path,
                "wall_seconds": elapsed,
                "response": _response_summary(resp),
            }
            if faketime_offset is not None:
                record["faketime_offset_seconds"] = faketime_offset
            per_match.append(record)
            print(
                "match_end label={} idx={} jobid={} wall_seconds={:.9f} status={}".format(
                    args.label,
                    idx,
                    jobid,
                    elapsed,
                    status,
                ),
                flush=True,
            )
            if args.cancel_each:
                handle.rpc("sched-fluxion-resource.cancel", {"jobid": jobid}).get()
        except Exception as exc:
            elapsed = time.perf_counter() - match_start
            signal.alarm(0)
            errors.append({
                "idx": idx,
                "jobid": jobid,
                "jobspec": path,
                "wall_seconds": elapsed,
                "error": repr(exc),
            })
            if faketime_offset is not None:
                errors[-1]["faketime_offset_seconds"] = faketime_offset
            print(
                "match_error label={} idx={} jobid={} wall_seconds={:.9f} error={}".format(
                    args.label,
                    idx,
                    jobid,
                    elapsed,
                    repr(exc),
                ),
                flush=True,
            )
            break
    total_wall = time.perf_counter() - t_start

    after_stats = _stats(handle, "sched-fluxion-resource.stats-get")
    params = _stats(handle, "sched-fluxion-resource.params")
    qmanager_stats = _stats(handle, "sched-fluxion-qmanager.stats-get")
    qmanager_params = _stats(handle, "sched-fluxion-qmanager.params")

    wall_values = [item["wall_seconds"] for item in per_match]
    payload = {
        "version": 1,
        "label": args.label,
        "resource_r": os.path.abspath(args.resource_r),
        "scheduler_json": os.path.abspath(args.scheduler_json),
        "jobspec_dir": os.path.abspath(args.jobspec_dir),
        "jobspecs_requested": len(jobspecs),
        "jobs_measured": len(per_match),
        "cancel_each": bool(args.cancel_each),
        "normalized_ssd_vertices": normalized_ssd_vertices,
        "total_wall_seconds": total_wall,
        "matches_per_second_wall": (
            len(per_match) / total_wall if total_wall > 0 else None
        ),
        "wall_time_summary": _summarize_times(wall_values),
        "response_counts": response_counts,
        "errors": errors,
        "faketime": {
            "stamp_file": os.path.abspath(args.faketime_stamp_file)
            if args.faketime_stamp_file
            else None,
            "jump_seconds": float(args.faketime_jump_seconds),
            "base_offset_seconds": float(args.faketime_base_offset_seconds),
            "offsets_recorded": len(faketime_offsets),
            "first_offset_seconds": faketime_offsets[0] if faketime_offsets else None,
            "last_offset_seconds": faketime_offsets[-1] if faketime_offsets else None,
        },
        "before_resource_stats": before_stats,
        "resource_stats": after_stats,
        "resource_params": params,
        "qmanager_stats": qmanager_stats,
        "qmanager_params": qmanager_params,
        "per_match": per_match,
    }
    _write_json(args.out, payload)
    print(json.dumps({
        "label": args.label,
        "jobs_measured": len(per_match),
        "errors": len(errors),
        "total_wall_seconds": total_wall,
        "matches_per_second_wall": payload["matches_per_second_wall"],
        "wall_time_summary": payload["wall_time_summary"],
    }, indent=2, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
