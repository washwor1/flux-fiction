#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from flux_fiction._adapters.flux import resources
from flux_fiction._core import models
from flux_fiction.ensemble.trace import (
    apply_rabbit_requests,
    apply_shake,
    load_normalized_trace,
    write_trace_csv,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_toml(path: Path, values: dict[str, object]) -> None:
    lines = ["[flux_fiction]"]
    for key, value in values.items():
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, (int, float)):
            rendered = repr(value)
        else:
            rendered = json.dumps(str(value))
        lines.append(f"{key} = {rendered}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class _ResourceCfg:
    def __init__(self, resource_r: Path, ncpus: int, ngpus: int):
        self.resource_R = str(resource_r)
        self.resource_file = None
        self.nnodes = 0
        self.ncpus = ncpus
        self.ngpus = ngpus


def _rabbit_capacity_gib(desc: dict) -> float:
    rabbit = desc["rabbit_storage"]
    return float(rabbit["parent_count"]) * float(rabbit["shares_per_parent"]) * float(
        rabbit["share_gib"]
    )


def _scheduler_config(*, queue_policy: str, match_policy: str, match_format: str) -> dict:
    return {
        "sched-fluxion-qmanager": {
            "queue-policy": queue_policy,
            "queue-params": {
                "max-queue-depth": 1000000,
                "queue-depth": 2048,
            },
            "policy-params": {
                "max-reservation-depth": 100000,
                "reservation-depth": 128,
            },
        },
        "sched-fluxion-resource": {
            "match-policy": match_policy,
            "match-format": match_format,
            "load-format": "rv1exec",
            "reserve-vtx-vec": 200000,
            "prune-filters": "ALL:core,ALL:node,ALL:gpu",
        },
        "queues": {
            "pbatch": {
                "requires": ["pbatch"],
            },
        },
        "policy": {
            "jobspec": {
                "defaults": {
                    "system": {
                        "queue": "pbatch",
                    },
                },
            },
        },
    }


def _flux_config_toml(*, queue_policy: str, match_policy: str, match_format: str) -> str:
    # Same module settings as the JSON config, written in Flux's native TOML
    # layout for nested system `flux start --config-path` probes.
    return f"""[sched-fluxion-qmanager]
queue-policy = "{queue_policy}"

[sched-fluxion-qmanager.queue-params]
max-queue-depth = 1000000
queue-depth = 2048

[sched-fluxion-qmanager.policy-params]
max-reservation-depth = 100000
reservation-depth = 128

[sched-fluxion-resource]
match-policy = "{match_policy}"
match-format = "{match_format}"
load-format = "rv1exec"
reserve-vtx-vec = 200000
prune-filters = "ALL:core,ALL:node,ALL:gpu"

[sched-fluxion-feasibility]
load-format = "rv1exec"
reserve-vtx-vec = 200000
prune-filters = "ALL:core,ALL:node,ALL:gpu"
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--base-trace", required=True, type=Path)
    ap.add_argument("--resource-r", required=True, type=Path)
    ap.add_argument("--jobs", type=int, default=200)
    ap.add_argument("--jobspecs", type=int, default=200)
    ap.add_argument("--slice-start", type=int, default=0)
    ap.add_argument("--cpus-per-node", type=int, default=96)
    ap.add_argument("--gpus-per-node", type=int, default=4)
    ap.add_argument("--shake-seed", type=int, default=91001)
    ap.add_argument("--rabbit-seed", type=int, default=12345)
    ap.add_argument("--queue-policy", default="conservative")
    ap.add_argument("--match-policy", default="lonodex")
    args = ap.parse_args()

    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out / "artifacts").mkdir(exist_ok=True)
    (out / "configs").mkdir(exist_ok=True)
    (out / "jobspecs").mkdir(exist_ok=True)
    (out / "system-conf").mkdir(exist_ok=True)

    desc = resources.describe_resource_config(
        _ResourceCfg(args.resource_r.resolve(), args.cpus_per_node, args.gpus_per_node)
    )
    capacity = _rabbit_capacity_gib(desc)

    rows = load_normalized_trace(
        args.base_trace,
        trace_format="tuolumne",
        slice_start=args.slice_start,
        slice_count=args.jobs,
        cpus_per_node=args.cpus_per_node,
        rabbit_field="RabbitGiB",
    )
    rows = apply_shake(
        rows,
        seed=args.shake_seed,
        attributes=["interarrival", "runtime", "nodes"],
        job_percentage=10,
        degree_seconds=60,
        relative_degree_percent=10,
    )
    rows = apply_rabbit_requests(
        rows,
        seed=args.rabbit_seed,
        job_percentage=100,
        capacity_gib=capacity,
        ceiling_percent=100,
        distribution="gaussian",
        field_name="RabbitGiB",
        mean_fraction=0.5,
        sigma_fraction=1.0 / 6.0,
        tail_min_gib=100.0,
    )
    trace_path = out / "queueb_gaussian_rabbit.csv"
    write_trace_csv(trace_path, rows, rabbit_field="RabbitGiB")

    for match_format in ("rv1_shorthand", "rv1_nosched"):
        scheduler = _scheduler_config(
            queue_policy=args.queue_policy,
            match_policy=args.match_policy,
            match_format=match_format,
        )
        scheduler_json = out / "configs" / f"scheduler_{match_format}.json"
        _write_json(scheduler_json, scheduler)
        _write_toml(
            out / "configs" / f"flux_fiction_{match_format}.toml",
            {
                "job_traces": str(trace_path),
                "resource_R": str(args.resource_r.resolve()),
                "config_json": str(scheduler_json),
                "ncpus": args.cpus_per_node,
                "ngpus": args.gpus_per_node,
                "backend": "flux",
                "quiet": True,
                "batch_job_starts": False,
                "account_system_latency": True,
                "jobtap_logging": False,
                "log_level": 40,
                "async_submit": True,
                "submit_novalidate": True,
                "make_plots": False,
            },
        )
        conf_dir = out / "system-conf" / match_format
        conf_dir.mkdir(parents=True, exist_ok=True)
        (conf_dir / "sched-fluxion.toml").write_text(
            _flux_config_toml(
                queue_policy=args.queue_policy,
                match_policy=args.match_policy,
                match_format=match_format,
            ),
            encoding="utf-8",
        )

    reader = models.SacctReader(str(trace_path), require_gpus=True)
    jobs = list(reader.read_trace())
    shape = desc.get("jobspec_shape", {})
    rabbit_storage = desc.get("rabbit_storage", {})
    for idx, job in enumerate(jobs[: max(0, args.jobspecs)], start=1):
        job.trace_index = idx - 1
        job.set_jobspec_shape(shape)
        job.set_rabbit_storage_shape(rabbit_storage)
        jobspec = dict(job.jobspec)
        # Direct vanilla probes match resources only; make the command valid in
        # case a caller submits the jobspec through the job manager.
        jobspec["tasks"] = [dict(task) for task in jobspec["tasks"]]
        jobspec["tasks"][0]["command"] = ["/bin/true"]
        _write_json(out / "jobspecs" / f"job_{idx:05d}.json", jobspec)

    commands = []
    for path in sorted((out / "jobspecs").glob("job_*.json")):
        commands.append(f"match allocate_orelse_reserve {path}")
    commands.append("stat")
    commands.append("quit")
    (out / "resource_query_commands.in").write_text("\n".join(commands) + "\n", encoding="utf-8")

    manifest = {
        "base_trace": str(args.base_trace.resolve()),
        "resource_R": str(args.resource_r.resolve()),
        "trace": str(trace_path),
        "jobs": len(rows),
        "jobspecs": min(len(jobs), max(0, args.jobspecs)),
        "queue_policy": args.queue_policy,
        "match_policy": args.match_policy,
        "match_formats": ["rv1_shorthand", "rv1_nosched"],
        "rabbit_capacity_gib": capacity,
        "rabbit_request_gib_min": min(float(row["RabbitGiB"]) for row in rows),
        "rabbit_request_gib_max": max(float(row["RabbitGiB"]) for row in rows),
    }
    _write_json(out / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
