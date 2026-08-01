#!/usr/bin/env python3
"""Analyze the focused DFTracer Salishan accuracy controls."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import sys


POLICY_REFERENCE_DIR = {
    "easy": "experiment_tuolumne_easy_gaussian_random",
    "conservative": "experiment_tuolumne_conservative_gaussian_random",
    "hybrid": "experiment_tuolumne_hybrid_res_depth_4_gaussian_random",
}


def latest_status(root: Path, name: str) -> Path | None:
    candidates = list((root / "runs" / name).glob("*/parallel_status.json"))
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def finite_or_none(value: float) -> float | None:
    return value if math.isfinite(value) else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path("/g/g14/ashworth12/workspace/ff-podman"),
    )
    args = parser.parse_args()

    root = args.root.absolute()
    workspace = args.workspace_root.absolute()
    suite_scripts = workspace / "flux-fiction-accuracy-tests" / "scripts"
    sys.path.insert(0, str(suite_scripts))
    from accuracy_common import (  # type: ignore[import-not-found]
        load_transitions,
        makespan,
        notebook_utilization,
        utilization_comparison,
    )

    matrix = json.loads((root / "matrix.json").read_text(encoding="utf-8"))
    rows: list[dict[str, object]] = []
    for spec in matrix["runs"]:
        name = str(spec["name"])
        policy = str(spec["policy"])
        release = str(spec["scheduler_base"])
        latency = bool(spec["account_system_latency"])
        status_path = latest_status(root, name)
        parallel_state = "missing"
        child_dir: Path | None = None
        wall_seconds: float | None = None
        failure_reason: str | None = None
        if status_path is not None:
            status = json.loads(status_path.read_text(encoding="utf-8"))
            run = status.get("runs", [{}])[0]
            parallel_state = str(run.get("state", status.get("state", "unknown")))
            if run.get("child_run_dir"):
                child_dir = Path(run["child_run_dir"])
            failure_reason = run.get("failure_reason")
            if run.get("started_at") and run.get("finished_at"):
                from datetime import datetime

                started = datetime.fromisoformat(str(run["started_at"]).replace("Z", "+00:00"))
                finished = datetime.fromisoformat(str(run["finished_at"]).replace("Z", "+00:00"))
                wall_seconds = max(0.0, (finished - started).total_seconds())

        simulated_path = (child_dir / "output" / "job_transitions.csv") if child_dir else Path("/missing")
        reference_path = (
            workspace
            / "salishan_runs"
            / POLICY_REFERENCE_DIR[policy]
            / "processed"
            / "job_transitions.csv"
        )
        simulated = load_transitions(simulated_path) if simulated_path.is_file() else []
        reference = load_transitions(reference_path)
        sim_util = notebook_utilization(simulated, capacity_nodes=128.0)
        real_util = notebook_utilization(reference, capacity_nodes=128.0)
        util_error = abs(sim_util - real_util)
        comparison = utilization_comparison(simulated, reference, capacity_nodes=128.0)
        sim_makespan = makespan(simulated)
        real_makespan = makespan(reference)
        makespan_error_pct = (
            100.0 * (sim_makespan - real_makespan) / real_makespan
            if math.isfinite(sim_makespan) and real_makespan
            else float("nan")
        )

        pair_name = f"{policy}-{'latency-on' if latency else 'latency-off'}"
        validation_path = root / "pairs" / pair_name / "trace_validation.json"
        trace_events = run_match_events = None
        trace_path = None
        if validation_path.is_file():
            validation = json.loads(validation_path.read_text(encoding="utf-8"))
            trace_record = next(
                item for item in validation["traces"] if item["release"] == release
            )
            trace_events = trace_record["events"]
            run_match_events = trace_record["run_match_events_with_jobid"]
            trace_path = trace_record["trace"]

        row: dict[str, object] = {
            "name": name,
            "policy": policy,
            "scheduler_base": release,
            "account_system_latency": latency,
            "async_submit": True,
            "state": parallel_state,
            "failure_reason": failure_reason,
            "wall_seconds": wall_seconds,
            "simulated_jobs": len(simulated),
            "real_jobs": len(reference),
            "sim_notebook_utilization_pct": finite_or_none(sim_util),
            "real_notebook_utilization_pct": finite_or_none(real_util),
            "notebook_utilization_error_pct_points": finite_or_none(util_error),
            "notebook_signed_utilization_error_pct_points": finite_or_none(sim_util - real_util),
            "utilization_curve_mae_pct_points": finite_or_none(
                comparison["utilization_mae_pct_points"]
            ),
            "simulated_makespan_seconds": finite_or_none(sim_makespan),
            "real_makespan_seconds": finite_or_none(real_makespan),
            "makespan_error_pct": finite_or_none(makespan_error_pct),
            "accuracy_pass_1pp": (
                parallel_state == "succeeded"
                and len(simulated) == 500
                and math.isfinite(util_error)
                and util_error <= 1.0
            ),
            "trace_events": trace_events,
            "run_match_events_with_jobid": run_match_events,
            "trace_path": trace_path,
            "simulated_transition_path": str(simulated_path),
            "real_transition_path": str(reference_path),
        }
        rows.append(row)

    report_dir = root / "reports"
    figure_dir = report_dir / "figures"
    report_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    factor_effects = []
    by_key = {
        (row["policy"], row["scheduler_base"], row["account_system_latency"]): row
        for row in rows
    }
    for policy in POLICY_REFERENCE_DIR:
        for release in ("v053", "v048"):
            on = by_key[(policy, release, True)]["notebook_utilization_error_pct_points"]
            off = by_key[(policy, release, False)]["notebook_utilization_error_pct_points"]
            factor_effects.append(
                {
                    "factor": "turn_account_system_latency_off",
                    "policy": policy,
                    "scheduler_base": release,
                    "error_before_pp": on,
                    "error_after_pp": off,
                    "error_change_pp": (off - on) if on is not None and off is not None else None,
                }
            )
        for latency in (True, False):
            current = by_key[(policy, "v053", latency)]["notebook_utilization_error_pct_points"]
            historical = by_key[(policy, "v048", latency)]["notebook_utilization_error_pct_points"]
            factor_effects.append(
                {
                    "factor": "switch_v053_to_v048",
                    "policy": policy,
                    "account_system_latency": latency,
                    "error_before_pp": current,
                    "error_after_pp": historical,
                    "error_change_pp": (
                        historical - current
                        if current is not None and historical is not None
                        else None
                    ),
                }
            )

    payload = {
        "root": str(root),
        "complete": all(row["state"] == "succeeded" for row in rows),
        "all_traces_valid": all(
            isinstance(row["run_match_events_with_jobid"], int)
            and row["run_match_events_with_jobid"] > 0
            for row in rows
        ),
        "runs": rows,
        "factor_effects": factor_effects,
    }
    (report_dir / "accuracy_controls.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    scalar_fields = [
        key
        for key in rows[0]
        if not isinstance(rows[0][key], (dict, list))
    ]
    with (report_dir / "accuracy_controls.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=scalar_fields)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in scalar_fields} for row in rows)

    if all(row["notebook_utilization_error_pct_points"] is not None for row in rows):
        import pandas as pd
        from plotnine import (
            aes,
            facet_wrap,
            geom_col,
            geom_hline,
            ggplot,
            labs,
            element_text,
            position_dodge,
            scale_fill_manual,
            theme,
            theme_bw,
        )

        frame = pd.DataFrame(rows)
        frame["latency"] = frame["account_system_latency"].map(
            {True: "account latency on", False: "account latency off"}
        )
        plot = (
            ggplot(
                frame,
                aes(
                    x="latency",
                    y="notebook_utilization_error_pct_points",
                    fill="scheduler_base",
                ),
            )
            + geom_col(position=position_dodge(width=0.8), width=0.72)
            + geom_hline(yintercept=1.0, linetype="dashed", color="#9b2226")
            + facet_wrap("~policy", nrow=1)
            + scale_fill_manual(values={"v053": "#4472C4", "v048": "#ED7D31"})
            + labs(
                title="Salishan Gaussian accuracy controls",
                x="Flux Fiction setting",
                y="Notebook-equivalent utilization error (percentage points)",
                fill="Fluxion base",
            )
            + theme_bw()
            + theme(
                figure_size=(12, 4.5),
                axis_text_x=element_text(rotation=15, ha="right"),
            )
        )
        plot.save(figure_dir / "utilization_error_by_factor.png", dpi=160, verbose=False)

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["complete"] and payload["all_traces_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
