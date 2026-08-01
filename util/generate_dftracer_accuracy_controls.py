#!/usr/bin/env python3
"""Generate focused Salishan Gaussian accuracy controls.

The matrix varies only the two factors under investigation:

* Fluxion base stack: current (v0.53-derived) or reconstructed v0.48
* Flux Fiction's account_system_latency setting: true or false

Submission remains asynchronous.  Each generated manifest contains one run so
the pdebug wrapper can pair one current and one v0.48 run without exceeding the
two-simulation concurrency demonstrated to be accuracy-safe.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


POLICIES = {
    "easy": {
        "trace_dir": "experiment_tuolumne_easy_gaussian_random",
        "scheduler": "scheduler_easy.json",
    },
    "conservative": {
        "trace_dir": "experiment_tuolumne_conservative_gaussian_random",
        "scheduler": "scheduler_conservative.json",
    },
    "hybrid": {
        "trace_dir": "experiment_tuolumne_hybrid_res_depth_4_gaussian_random",
        "scheduler": "scheduler_hybrid.json",
    },
}
RELEASES = ("v053", "v048")
LATENCY_SETTINGS = (("latency-on", True), ("latency-off", False))


def toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return json.dumps(str(value))


def write_config(path: Path, values: dict[str, object]) -> None:
    path.write_text(
        "[flux_fiction]\n"
        + "".join(f"{key} = {toml_value(value)}\n" for key, value in values.items()),
        encoding="utf-8",
    )


def write_manifest(
    path: Path,
    *,
    release: str,
    policy: str,
    latency_name: str,
    account_system_latency: bool,
    config_path: Path,
    output_root: Path,
) -> None:
    run_name = f"{release}-{policy}-{latency_name}"
    metadata = {
        "account_system_latency": account_system_latency,
        "async_submit": True,
        "dataset": "salishan-policy-specific-gaussian",
        "jobspec_shape": "cores",
        "policy": policy,
        "scheduler_base": release,
    }
    metadata_text = "{ " + ", ".join(
        f"{key} = {toml_value(value)}" for key, value in sorted(metadata.items())
    ) + " }"
    path.write_text(
        "\n".join(
            [
                "version = 1",
                "",
                "[parallel]",
                "max_concurrent = 1",
                "fail_fast = true",
                'progress_mode = "summary"',
                "summary_interval = 30.0",
                f"output_root = {toml_value(output_root)}",
                "",
                "[[run]]",
                f"name = {toml_value(run_name)}",
                f"config_file = {toml_value(config_path)}",
                "broker_log_level = 3",
                f"metadata = {metadata_text}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--workspace-root", required=True, type=Path)
    args = parser.parse_args()

    # Preserve the user-facing /g/g14 mount name.  Path.resolve() canonicalizes
    # it to /usr/WS1 on this host, but the containers bind the former path.
    out = args.out.absolute()
    workspace = args.workspace_root.absolute()
    configs = out / "matrix" / "configs"
    manifests = out / "matrix" / "manifests"
    configs.mkdir(parents=True, exist_ok=False)
    manifests.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, object]] = []
    for policy, policy_data in POLICIES.items():
        trace = (
            workspace
            / "salishan_runs"
            / str(policy_data["trace_dir"])
            / "processed"
            / "input_with_latencies_patched.csv"
        )
        scheduler = (
            workspace
            / "flux-fiction-accuracy-tests"
            / "baselines"
            / str(policy_data["scheduler"])
        )
        if not trace.is_file():
            raise FileNotFoundError(trace)
        if not scheduler.is_file():
            raise FileNotFoundError(scheduler)

        for latency_name, account_system_latency in LATENCY_SETTINGS:
            for release in RELEASES:
                stem = f"{release}-{policy}-{latency_name}"
                config = configs / f"{stem}.toml"
                manifest = manifests / f"{stem}.toml"
                write_config(
                    config,
                    {
                        "job_traces": trace,
                        "config_json": scheduler,
                        "nnodes": 128,
                        "nsockets": 1,
                        "ncpus": 96,
                        "ngpus": 0,
                        "backend": "flux",
                        "quiet": True,
                        "batch_job_starts": False,
                        "account_system_latency": account_system_latency,
                        "jobtap_logging": False,
                        # Intentionally retained from the original DFTracer
                        # matrix at the user's request.
                        "async_submit": True,
                        "submit_novalidate": True,
                        "make_plots": False,
                        "omit_core_resources": False,
                    },
                )
                write_manifest(
                    manifest,
                    release=release,
                    policy=policy,
                    latency_name=latency_name,
                    account_system_latency=account_system_latency,
                    config_path=config,
                    output_root=out / "runs" / stem,
                )
                records.append(
                    {
                        "name": stem,
                        "scheduler_base": release,
                        "policy": policy,
                        "account_system_latency": account_system_latency,
                        "async_submit": True,
                        "trace": str(trace),
                        "scheduler_config": str(scheduler),
                        "config": str(config),
                        "manifest": str(manifest),
                    }
                )

    (out / "matrix.json").write_text(
        json.dumps(
            {
                "dftracer_annotation_level": 2,
                "cmake_build_type": "Release",
                "jobspec_shape": "cores",
                "async_submit": True,
                "factors": ["scheduler_base", "account_system_latency"],
                "runs": records,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"generated {len(records)} focused controls under {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
