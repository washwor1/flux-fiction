#!/usr/bin/env python3
"""Generate the level-2 DFTracer Gaussian backfill A/B matrix inputs.

The runtime wrapper supplies one Fluxion install prefix per scheduler variant.
The generated manifests deliberately have six runs each: three queue policies
times the normal and node-only jobspec forms.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


POLICIES = ("easy", "hybrid", "conservative")
JOBSPEC_SHAPES = (("cores", False), ("no-cores", True))
VARIANTS = ("baseline", "skip-empty-dfu-expansion")


def toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return json.dumps(str(value))


def toml_inline_table(values: dict[str, object]) -> str:
    """Render the small, scalar-only metadata dictionary as TOML."""
    return "{ " + ", ".join(
        f"{key} = {toml_value(value)}" for key, value in sorted(values.items())
    ) + " }"


def write_toml(path: Path, values: dict[str, object]) -> None:
    path.write_text(
        "[flux_fiction]\n"
        + "".join(f"{key} = {toml_value(value)}\n" for key, value in values.items()),
        encoding="utf-8",
    )


def scheduler_config(policy: str) -> dict[str, object]:
    return {
        # Keep ingest disabled after Flux Fiction reloads the scheduler.  The
        # node-only jobspec arm intentionally violates jobspec-v1's slot/core
        # shape, so it must reach Fluxion unchanged.
        "ingest": {
            "frobnicator": {"disable": True, "plugins": []},
            "validator": {"disable": True, "plugins": []},
        },
        "sched-fluxion-qmanager": {
            "queue-policy": policy,
            "queue-params": {"queue-depth": 1024},
            "policy-params": {"reservation-depth": 4},
        },
        "sched-fluxion-resource": {
            "match-policy": "lonodex",
            "prune-filters": "ALL:core",
            "match-format": "rv1_nosched",
        },
    }


def write_manifest(path: Path, variant: str, config_dir: Path, runtime_root: Path) -> None:
    lines = [
        "version = 1",
        "",
        "[parallel]",
        "max_concurrent = 6",
        "fail_fast = false",
        "progress_mode = \"summary\"",
        "summary_interval = 30.0",
        f"output_root = {json.dumps(str(runtime_root / 'runs' / variant))}",
    ]
    for policy in POLICIES:
        for shape, omit_cores in JOBSPEC_SHAPES:
            name = f"{policy}-{shape}"
            config = config_dir / f"{variant}-{name}.toml"
            lines.extend(
                [
                    "",
                    "[[run]]",
                    f"name = {json.dumps(name)}",
                    f"config_file = {json.dumps(str(config))}",
                    "broker_log_level = 3",
                    "metadata = "
                    + toml_inline_table(
                        {
                            "scheduler_variant": variant,
                            "queue_policy": policy,
                            "reservation_depth": 4,
                            "jobspec_shape": shape,
                            "omit_core_resources": omit_cores,
                            "dataset": "gaussian",
                        }
                    ),
                ]
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, type=Path, help="host output directory")
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=None,
        help="path visible inside the container (defaults to --out)",
    )
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--nnodes", type=int, default=128)
    parser.add_argument("--ncpus", type=int, default=96)
    args = parser.parse_args()

    out = args.out.resolve()
    runtime_root = (args.runtime_root or out).resolve()
    config_dir = out / "matrix" / "configs"
    manifest_dir = out / "matrix" / "manifests"
    config_dir.mkdir(parents=True, exist_ok=False)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    runtime_config_dir = runtime_root / "matrix" / "configs"
    for policy in POLICIES:
        scheduler_path = config_dir / f"scheduler-{policy}.json"
        scheduler_path.write_text(
            json.dumps(scheduler_config(policy), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for variant in VARIANTS:
            for shape, omit_cores in JOBSPEC_SHAPES:
                config_path = config_dir / f"{variant}-{policy}-{shape}.toml"
                write_toml(
                    config_path,
                    {
                        "job_traces": str(args.trace.resolve()),
                        "nnodes": args.nnodes,
                        "nsockets": 1,
                        "config_json": str(runtime_config_dir / scheduler_path.name),
                        "ncpus": args.ncpus,
                        "ngpus": 0,
                        "backend": "flux",
                        "quiet": True,
                        "batch_job_starts": False,
                        "account_system_latency": True,
                        "jobtap_logging": False,
                        "async_submit": True,
                        "submit_novalidate": True,
                        "make_plots": False,
                        "omit_core_resources": omit_cores,
                    },
                )

    for variant in VARIANTS:
        write_manifest(
            manifest_dir / f"{variant}.toml",
            variant,
            runtime_config_dir,
            runtime_root,
        )

    matrix = [
        {
            "scheduler_variant": variant,
            "queue_policy": policy,
            "reservation_depth": 4,
            "jobspec_shape": shape,
            "omit_core_resources": omit_cores,
            "config": str(config_dir / f"{variant}-{policy}-{shape}.toml"),
        }
        for variant in VARIANTS
        for policy in POLICIES
        for shape, omit_cores in JOBSPEC_SHAPES
    ]
    (out / "matrix.json").write_text(
        json.dumps(
            {
                "dataset": str(args.trace.resolve()),
                "resources": {"nnodes": args.nnodes, "ncpus": args.ncpus, "ngpus": 0},
                "match_policy": "lonodex",
                "match_format": "rv1_nosched",
                "reservation_depth": 4,
                "runs": matrix,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"generated {len(matrix)} configs under {out / 'matrix'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
