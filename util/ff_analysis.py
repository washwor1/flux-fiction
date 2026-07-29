"""Shared loading, derived columns and plot theme for the analysis notebooks.

Both notebooks import this so they stay thin and so a rerun picks up whatever
the campaign has produced since. `load(refresh=True)` re-runs the harvester
across every host first; `load()` reads the last harvest.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from plotnine import (aes, element_blank, element_line, element_rect, element_text,
                      theme, theme_minimal)

REPO = Path(__file__).resolve().parent.parent
SHARED = Path("/usr/WS1/ashworth12/ff-podman/ensemble-shared-rabbit-threshold")
PY = os.environ.get(
    "FF_PYTHON",
    "/collab/usr/gapps/python/toss_4_x86_64_ib/anaconda3-2025.3.1/bin/python3")

# Categorical hues in fixed order, from the validated reference palette. Slots
# 1-3 clear the all-pairs gates (worst CVD dE 9.2, normal-vision 24.0); the 4th
# is adjacent-only, so it is used on bars and facets, never on a scatter.
PAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
INK, INK2, GRID = "#10141a", "#4d566a", "#e4e7ee"

# A truncated run is NOT a shorter run of the same thing: makespan and
# utilisation are only comparable between runs that simulated the same number of
# jobs. These tiers keep that distinction explicit everywhere.
TIERS = [
    ("complete (10k jobs)", lambda s: s >= 10000),
    ("5000+ jobs", lambda s: (s >= 5000) & (s < 10000)),
    ("1000-4999 jobs", lambda s: (s >= 1000) & (s < 5000)),
    ("under 1000 jobs", lambda s: (s > 0) & (s < 1000)),
]


def theme_ff(base=10):
    """Recessive grid, hairline axes, no chartjunk."""
    return theme_minimal(base_size=base) + theme(
        figure_size=(9, 5),
        panel_grid_minor=element_blank(),
        panel_grid_major=element_line(color=GRID, size=0.5),
        panel_background=element_rect(fill="white", color="none"),
        plot_background=element_rect(fill="white", color="none"),
        axis_text=element_text(color=INK2, size=base - 1),
        axis_title=element_text(color=INK, size=base),
        plot_title=element_text(color=INK, size=base + 4, weight="bold", ha="left"),
        plot_subtitle=element_text(color=INK2, size=base, ha="left"),
        plot_caption=element_text(color=INK2, size=base - 2, ha="left"),
        strip_text=element_text(color=INK, size=base, weight="bold"),
        legend_title=element_text(color=INK, size=base),
        legend_text=element_text(color=INK2, size=base - 1),
    )


def refresh(only=None, force=False) -> str:
    """Re-run the harvester across every host. Safe to call repeatedly."""
    cmd = [PY, str(REPO / "util" / "ff_harvest.py")]
    if only:
        cmd += ["--only", only]
    if force:
        cmd += ["--force"]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=5400)
    return (out.stdout or "") + (out.stderr or "")[-800:]


def load(refresh_first: bool = False, force: bool = False):
    """-> (runs, trajectories). Every derived column the notebooks use is here."""
    if refresh_first:
        print(refresh(force=force))
    runs = pd.read_csv(SHARED / "metrics_all.csv")
    try:
        traj = pd.read_csv(SHARED / "trajectories_all.csv")
    except (OSError, ValueError):
        traj = pd.DataFrame(columns=["host", "task_id", "elapsed_h", "jobs_completed"])

    runs["finalized"] = runs["finalized"].astype(str).str.lower().eq("true")
    for c in ("rabbit_job_pct", "rabbit_ceiling_pct"):
        runs[c] = pd.to_numeric(runs[c], errors="coerce")
    runs["jobs_completed"] = pd.to_numeric(runs["jobs_completed"], errors="coerce").fillna(0)
    runs["policy"] = runs["queue_policy"] + " / " + runs["match_policy"]
    runs["config"] = (runs["queue_policy"] + "/" + runs["match_policy"]
                      + "/rj" + runs["rabbit_job_pct"].astype("Int64").astype(str)
                      + "/rc" + runs["rabbit_ceiling_pct"].astype("Int64").astype(str)
                      + "/" + runs["distribution"].astype(str))
    runs["complete"] = runs["jobs_completed"] >= 10000
    runs["rabbit_active"] = runs["rabbit_job_pct"] > 0

    tier = pd.Series("no data / still running", index=runs.index)
    for name, test in reversed(TIERS):
        tier[test(runs["jobs_completed"])] = name
    runs["tier"] = pd.Categorical(
        tier, categories=[t[0] for t in TIERS] + ["no data / still running"], ordered=True)

    # Throughput normalised for truncation: jobs actually simulated per hour of
    # SIMULATED makespan. Comparable across runs of different length in a way
    # that raw makespan is not.
    runs["jobs_per_makespan_h"] = runs["jobs_completed"] / (runs["makespan_s"] / 3600.0)
    # Share of the run's wall clock that Fluxion spent matching.
    runs["match_s_per_job"] = runs["match_total_s"] / runs["jobs_completed"].replace(0, np.nan)
    runs["match_fail_ratio"] = runs["match_fail_n"] / runs["match_ok_n"].replace(0, np.nan)
    return runs, traj


def tier_table(runs: pd.DataFrame) -> pd.DataFrame:
    """Per-config coverage: how many of a config's shakes reached each level."""
    g = runs.groupby("config")
    out = pd.DataFrame({
        "shakes": g.size(),
        "finalized": g["finalized"].sum(),
        "complete_10k": g["jobs_completed"].apply(lambda s: int((s >= 10000).sum())),
        "ge_5000": g["jobs_completed"].apply(lambda s: int((s >= 5000).sum())),
        "ge_1000": g["jobs_completed"].apply(lambda s: int((s >= 1000).sum())),
        "any_data": g["jobs_completed"].apply(lambda s: int((s > 0).sum())),
    })
    for c in ("queue_policy", "match_policy", "rabbit_job_pct",
              "rabbit_ceiling_pct", "distribution"):
        out[c] = g[c].first()
    return out.reset_index()


def coverage_summary(tiers: pd.DataFrame) -> pd.DataFrame:
    """The headline counts: how many configs cleared each bar."""
    n = len(tiers)
    rows = [
        ("all shakes complete (10k)", int((tiers.complete_10k == tiers.shakes).sum())),
        ("5+ shakes complete (10k)", int((tiers.complete_10k >= 5).sum())),
        ("1+ shake complete (10k)", int((tiers.complete_10k >= 1).sum())),
        ("all shakes reached 5000", int((tiers.ge_5000 == tiers.shakes).sum())),
        ("1+ shake reached 5000", int((tiers.ge_5000 >= 1).sum())),
        ("1+ shake reached 1000", int((tiers.ge_1000 >= 1).sum())),
        ("no shake produced data", int((tiers.any_data == 0).sum())),
    ]
    df = pd.DataFrame(rows, columns=["criterion", "configs"])
    df["pct_of_configs"] = (100 * df.configs / n).round(1)
    return df


def rho(df: pd.DataFrame, x: str, y: str, by: str | None = None) -> pd.DataFrame:
    """Spearman correlation, overall and optionally within each group.

    Reported in the notebooks rather than hardcoded, so a claim in a title stays
    tied to the data as the campaign grows. Worth splitting by policy: several of
    these relationships reverse sign within groups (Simpson's paradox), and the
    pooled number then says the opposite of every subgroup.
    """
    rows = [{"group": "all", "rho": df[[x, y]].dropna()[x].corr(
        df[[x, y]].dropna()[y], method="spearman"), "n": len(df[[x, y]].dropna())}]
    if by:
        for g, sub in df.groupby(by):
            s = sub[[x, y]].dropna()
            if len(s) >= 10:
                rows.append({"group": str(g),
                             "rho": s[x].corr(s[y], method="spearman"), "n": len(s)})
    out = pd.DataFrame(rows)
    out["rho"] = out["rho"].round(3)
    return out
