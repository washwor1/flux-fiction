#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from flux_fiction.parallel import (
    ParallelValidationError,
    load_parallel_manifest,
    resolve_parallel_plan,
    run_parallel_plan,
)
from flux_fiction.parallel import flux_launch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and plan a parallel Flux Fiction manifest."
    )
    parser.add_argument("manifest", help="Path to the parallel manifest TOML file.")
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=None,
        help="Override manifest max_concurrent.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Override manifest fail_fast to stop queueing new runs after the first failure.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Override manifest output_root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the manifest and print the derived launch plan without executing.",
    )
    parser.add_argument(
        "--summary-interval",
        type=float,
        default=None,
        help="Override manifest summary interval in seconds.",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Optional shard index for external wrapper usage.",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=None,
        help="Optional shard count for external wrapper usage.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the resolved dry-run plan as JSON.",
    )
    parser.add_argument(
        "--show-makespan-extremes",
        action="store_true",
        help="Include the shortest and longest successful makespans in the final console summary.",
    )
    return parser


def _print_text_plan(plan) -> None:
    print(f"Manifest:          {plan.manifest_path}")
    print(f"Output root:       {plan.output_root}")
    print(f"Max concurrent:    {plan.max_concurrent}")
    if flux_launch.enabled():
        cores = flux_launch.cores_per_replica(plan.max_concurrent)
        managed = bool(os.environ.get("FLUX_URI")) and plan.max_concurrent > 1
        print(
            f"Flux launch:       enabled ({'managed, ' if managed else 'UNMANAGED, '}"
            f"cores/replica={cores})"
        )
    print(f"Fail fast:         {plan.fail_fast}")
    print(f"Progress mode:     {plan.progress_mode}")
    print(f"Summary interval:  {plan.summary_interval}")
    if plan.shard_count is not None:
        print(f"Shard:             {plan.shard_index}/{plan.shard_count}")
    print(f"Planned runs:      {len(plan.runs)}")
    print("")
    for run in plan.runs:
        print(f"[{run.ordinal:04d}] {run.name}")
        print(f"  config_file:       {run.config_file}")
        if run.job_traces:
            print(f"  job_traces:        {run.job_traces}")
        if run.config_json:
            print(f"  config_json:       {run.config_json}")
        if run.resource_file:
            print(f"  resource_file:     {run.resource_file}")
        if run.resource_R:
            print(f"  resource_R:        {run.resource_R}")
        if run.tag:
            print(f"  tag:               {run.tag}")
        print(f"  no_faketime:       {run.no_faketime}")
        print(f"  broker_log_level:  {run.broker_log_level}")
        if run.config_overrides:
            print(f"  config_overrides:  {run.config_overrides}")
        print(f"  run_root:          {run.run_root}")
        print(f"  child_run_dir:     {run.child_run_dir}")
        print(f"  stampfile:         {run.stampfile}")
        if run.metadata:
            print(f"  metadata:          {run.metadata}")
        print("")


def main(argv: list[str] | None = None) -> int:
    # Flux-native placement needs an enclosing instance; this replaces the
    # process with `flux start -- <this runner>` when the mode is on and no
    # FLUX_URI exists, and returns immediately otherwise.
    if argv is None:
        flux_launch.maybe_reexec_under_flux_start()
    args = build_parser().parse_args(argv)

    try:
        manifest = load_parallel_manifest(args.manifest)
        plan = resolve_parallel_plan(
            manifest,
            max_concurrent=args.max_concurrent,
            fail_fast=True if args.fail_fast else None,
            output_root=args.output_root,
            summary_interval=args.summary_interval,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
        )
    except (FileNotFoundError, ParallelValidationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(plan.to_json())
    else:
        _print_text_plan(plan)

    if args.dry_run:
        print("Dry run requested; not launching child runs.")
        return 0

    result = run_parallel_plan(plan, show_makespan_extremes=args.show_makespan_extremes)
    print("")
    print(f"Parallel status:   {result.status_path}")
    print(f"Parallel summary:  {result.summary_path}")
    print(f"Manifest snapshot: {result.manifest_snapshot_path}")
    print(f"Return code:       {result.return_code}")
    return result.return_code


if __name__ == "__main__":
    raise SystemExit(main())
