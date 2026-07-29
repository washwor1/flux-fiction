#!/usr/bin/env python3
"""Harvest one row per simulation run into a single tidy table, across clusters.

    util/ff_harvest.py                      # every host, incremental, then merge
    util/ff_harvest.py --force              # ignore the cache, recompute all
    util/ff_harvest.py --only dane
    util/ff_harvest.py --out ~/metrics.csv

Designed to be re-run as the campaign produces more data. Nothing is enumerated
by hand: runs are discovered by globbing each campaign root, so a rerun picks up
whatever has finalized since. Two things make a rerun cheap:

  * an incremental cache -- a run whose status/summary files have not changed
    since the last harvest is copied from the previous table rather than
    re-parsed, and the expensive part (per-job records) is only ever read once
    per run;
  * a process pool -- per-job parsing is CPU-bound and embarrassingly parallel.

Metrics come in two tiers. Grid factors and live jobs_completed are always
available from the child's status.json. Everything derived from per-job records
-- bounded slowdown, Jain fairness, queue depth -- needs job_transitions.csv,
which is only written when a run finalizes. An early finalize counts: a run cut
short by its walltime writes both files, so truncated runs still carry metrics.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import glob
import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
PY = os.environ.get(
    "FF_PYTHON",
    "/collab/usr/gapps/python/toss_4_x86_64_ib/anaconda3-2025.3.1/bin/python3",
)
DEFAULT_HOSTS = REPO / "test-inputs" / "hosts-rabbit-threshold.toml"
BOUNDED_THRESHOLD = 10.0        # seconds -- the standard bound for slowdown
MAX_WORKERS = int(os.environ.get("FF_HARVEST_WORKERS", "20"))

COLUMNS = [
    "host", "task_id", "batch_id", "queue_policy", "match_policy",
    "rabbit_job_pct", "rabbit_ceiling_pct", "distribution", "shake_seed",
    "state", "jobs_completed", "jobs_total", "pct_complete", "finalized",
    "makespan_s", "avg_queue_wait_s", "max_queue_wait_s",
    "util_node_pct", "util_core_pct", "util_gpu_pct", "util_ssd_pct",
    "match_ok_n", "match_ok_avg_s", "match_ok_max_s",
    "match_fail_n", "match_fail_avg_s", "match_total_s",
    "graph_v", "graph_load_s", "quiescence_epochs", "kvs_end_gb",
    "start_lag_avg_s", "run_started_at",
    "n_jobs", "wait_mean_s", "wait_p95_s", "run_mean_s",
    "bsld_mean", "bsld_median", "bsld_p95", "bsld_max", "fairness_jain",
    "queue_depth_mean", "queue_depth_max",
    "nodes_mean", "nodes_max", "frac_single_node",
    "wait_mean_single_s", "wait_mean_multi_s",
    "bsld_mean_single", "bsld_mean_multi",
    "fingerprint",
]


# ---------------------------------------------------------------- helpers
def read_json(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def per_job_metrics(path: str) -> dict:
    """Bounded slowdown, fairness and queue depth from the per-job event table.

    Vectorised: these files run to tens of thousands of rows and there are
    thousands of them.
    """
    try:
        df = pd.read_csv(path, usecols=["nnodes", "SUBMIT", "START", "FINISH"],
                         engine="c", on_bad_lines="skip")
    except (OSError, ValueError, KeyError):
        return {}
    df = df.dropna(subset=["SUBMIT", "START", "FINISH"])
    if df.empty:
        return {}
    submit = df["SUBMIT"].to_numpy(float)
    start = df["START"].to_numpy(float)
    finish = df["FINISH"].to_numpy(float)
    nodes = df["nnodes"].to_numpy(float)
    wait, run = start - submit, finish - start
    keep = (wait >= 0) & (run >= 0)
    if not keep.any():
        return {}
    submit, start, wait, run, nodes = (
        submit[keep], start[keep], wait[keep], run[keep], nodes[keep])
    n = wait.size

    bsld = np.maximum(1.0, (wait + run) / np.maximum(run, BOUNDED_THRESHOLD))
    # Jain's fairness index: 1.0 = every job delayed proportionally the same,
    # 1/n = one job absorbs all of the delay.
    jain = float(bsld.sum() ** 2 / (n * np.square(bsld).sum()))

    # Instantaneous queue depth: +1 on submit, -1 on start, time-weighted.
    t = np.concatenate([submit, start])
    delta = np.concatenate([np.ones(n), -np.ones(n)])
    order = np.argsort(t, kind="stable")
    t, delta = t[order], delta[order]
    depth = np.cumsum(delta)
    span = t[-1] - t[0]
    qd_mean = float((depth[:-1] * np.diff(t)).sum() / span) if span > 0 else 0.0

    single = nodes <= 1
    multi = ~single
    f = lambda v: float(v) if np.isfinite(v) else None
    return {
        "n_jobs": n,
        "wait_mean_s": f(wait.mean()), "wait_p95_s": f(np.percentile(wait, 95)),
        "run_mean_s": f(run.mean()),
        "bsld_mean": f(bsld.mean()), "bsld_median": f(np.median(bsld)),
        "bsld_p95": f(np.percentile(bsld, 95)), "bsld_max": f(bsld.max()),
        "fairness_jain": jain,
        "queue_depth_mean": qd_mean, "queue_depth_max": int(depth.max()),
        "nodes_mean": f(nodes.mean()), "nodes_max": int(nodes.max()),
        "frac_single_node": f(single.mean()),
        "wait_mean_single_s": f(wait[single].mean()) if single.any() else None,
        "wait_mean_multi_s": f(wait[multi].mean()) if multi.any() else None,
        "bsld_mean_single": f(bsld[single].mean()) if single.any() else None,
        "bsld_mean_multi": f(bsld[multi].mean()) if multi.any() else None,
    }


def manifest_epoch(task_dir: str) -> float | None:
    """Epoch seconds for an attempt, from its `<YYYYmmdd_HHMMSS>_manifest` dir."""
    try:
        stamp = task_dir.split("/parallel/")[1].split("/")[0].replace("_manifest", "")
        # timegm, not mktime: the stamp is UTC, and mktime would read it as local
        # time (and the DST offset differs from time.timezone half the year).
        return calendar.timegm(time.strptime(stamp, "%Y%m%d_%H%M%S"))
    except (IndexError, ValueError):
        return None


def harvest_one(args) -> dict | None:
    """One run -> one row. Runs in a worker process."""
    host, task, task_dir = args
    child = os.path.join(task_dir, "child")
    status = read_json(os.path.join(child, "status.json"))
    summary_path = os.path.join(child, "summary.json")
    summary = read_json(summary_path)
    if not status and not summary:
        return None

    parts = task.split("__")
    pick = lambda pre: next((x[len(pre):] for x in parts if x.startswith(pre)), "")
    done = summary.get("jobs_completed", status.get("jobs_completed"))
    total = summary.get("jobs_total", status.get("jobs_total"))
    row = {
        "host": host, "task_id": task,
        "batch_id": task_dir.split("/batches/")[1].split("/")[0],
        "queue_policy": pick("q"), "match_policy": pick("m"),
        "rabbit_job_pct": pick("rj"), "rabbit_ceiling_pct": pick("rc"),
        "distribution": parts[-1][1:] if parts[-1].startswith("d") else "",
        "shake_seed": "s9" + pick("s9") if pick("s9") else "",
        "state": summary.get("state") or status.get("state") or "",
        "jobs_completed": done, "jobs_total": total,
        "pct_complete": (100.0 * done / total) if (done and total) else None,
        "finalized": bool(summary),
        "fingerprint": "%.0f:%.0f" % (_mtime(summary_path),
                                      _mtime(os.path.join(child, "status.json"))),
        # The manifest directory is named for real wall clock at launch, so it
        # dates this attempt without trusting anything the child wrote.
        "run_started_at": manifest_epoch(task_dir),
    }
    if summary:
        rs = summary.get("resource_summary") or {}
        rs = rs.get("resources", rs)
        sm = (summary.get("scheduler_metrics") or {}).get("resource_match_stats") or {}
        match = sm.get("match") or {}
        ok, bad = match.get("succeeded") or {}, match.get("failed") or {}
        ok_stats, bad_stats = ok.get("stats") or {}, bad.get("stats") or {}
        get = lambda d, k: (d.get(k) or {}).get("utilization_pct")
        total_match = ((ok.get("njobs") or 0) * (ok_stats.get("avg") or 0)
                       + (bad.get("njobs") or 0) * (bad_stats.get("avg") or 0))
        row.update({
            "makespan_s": summary.get("makespan_seconds"),
            "avg_queue_wait_s": summary.get("avg_queue_wait_seconds"),
            "max_queue_wait_s": summary.get("max_queue_wait_seconds"),
            "util_node_pct": get(rs, "node"), "util_core_pct": get(rs, "core"),
            "util_gpu_pct": get(rs, "gpu"), "util_ssd_pct": get(rs, "ssd"),
            "match_ok_n": ok.get("njobs"), "match_ok_avg_s": ok_stats.get("avg"),
            "match_ok_max_s": ok_stats.get("max"),
            "match_fail_n": bad.get("njobs"), "match_fail_avg_s": bad_stats.get("avg"),
            "match_total_s": total_match or None,
            "graph_v": sm.get("V"), "graph_load_s": sm.get("load-time"),
            "quiescence_epochs": summary.get("quiescence_epochs"),
            "kvs_end_gb": (summary.get("kvs_size_end_bytes") or 0) / 1e9 or None,
            "start_lag_avg_s": summary.get("flux_observed_start_lag_avg_seconds"),
        })
        row.update(per_job_metrics(os.path.join(child, "output", "job_transitions.csv")))
    return row


def discover(root: str) -> dict[str, str]:
    """task_id -> newest attempt directory.

    A requeued batch has one manifest dir per attempt. Order them by the manifest
    NAME, which is real wall clock at launch; the child's own updated_at cannot be
    used because it runs under libfaketime and stamps simulated time.
    """
    best: dict[str, tuple] = {}
    for child in glob.iglob(root + "/batches/*/parallel/*_manifest/runs/*/child"):
        task_dir = os.path.dirname(child)
        name = os.path.basename(task_dir)
        task = name.split("_", 1)[1] if "_" in name else name
        manifest = task_dir.split("/parallel/")[1].split("/")[0]
        key = (manifest, _mtime(child))
        if task not in best or key > best[task][0]:
            best[task] = (key, task_dir)
    return {t: d for t, (_, d) in best.items()}


# ---------------------------------------------------------------- per host
def harvest_host(root: str, host: str, out_path: str, force: bool) -> int:
    tasks = discover(root)
    cached: dict[str, dict] = {}
    if not force and os.path.exists(out_path):
        try:
            old = pd.read_csv(out_path, dtype=str, keep_default_na=False)
            cached = {r["task_id"]: r for r in old.to_dict("records")}
        except Exception:
            cached = {}

    todo, reused = [], []
    for task, task_dir in tasks.items():
        child = os.path.join(task_dir, "child")
        fp = "%.0f:%.0f" % (_mtime(os.path.join(child, "summary.json")),
                            _mtime(os.path.join(child, "status.json")))
        prev = cached.get(task)
        if prev is not None and prev.get("fingerprint") == fp:
            reused.append(prev)
        else:
            todo.append((host, task, task_dir))

    rows = []
    if todo:
        workers = max(1, min(MAX_WORKERS, (os.cpu_count() or 4)))
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for row in pool.map(harvest_one, todo, chunksize=8):
                if row:
                    rows.append(row)
    allrows = reused + rows
    df = pd.DataFrame(allrows)
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[COLUMNS].sort_values(["host", "task_id"])
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    traj = trajectories(root, host, df)
    traj_path = str(Path(out_path).with_name("trajectories.csv"))
    traj.to_csv(traj_path, index=False)
    sys.stderr.write("%s: %d run(s) (%d recomputed, %d cached), %d trajectory point(s) -> %s\n"
                     % (host, len(df), len(rows), len(reused), len(traj), out_path))
    return len(df)


def trajectories(root: str, host: str, df: pd.DataFrame, per_task: int = 150) -> pd.DataFrame:
    """Wall-clock progress curves, downsampled, for runs that reached a finalize.

    The launcher appends one row per task per tick to progress.csv, so this is
    the only record of how a run got to its final count rather than just what
    that count was. Restricted to finalized runs and thinned to `per_task`
    points, because the raw file is hundreds of megabytes campaign-wide.
    """
    want = set(df.loc[df["finalized"].astype(str).str.lower() == "true", "task_id"])
    path = os.path.join(root, "progress.csv")
    if not want:
        return pd.DataFrame(columns=["host", "task_id", "wall_epoch", "jobs_completed"])
    try:
        cols = ["wall_epoch", "task_id", "jobs_completed"]
        raw = pd.read_csv(path, usecols=cols, on_bad_lines="skip")
    except (OSError, ValueError, KeyError):
        return pd.DataFrame(columns=["host", "task_id", "wall_epoch", "jobs_completed"])
    raw = raw[raw["task_id"].isin(want)].dropna()
    if raw.empty:
        return pd.DataFrame(columns=["host", "task_id", "wall_epoch", "jobs_completed"])
    raw = raw.sort_values(["task_id", "wall_epoch"])
    # Clip to the current attempt. A count reset is NOT a reliable boundary: until
    # the manifest-selection bug was fixed on 2026-07-27 the launcher reported the
    # DEAD attempt's frozen count for requeued tasks, so a series can step UP
    # across the boundary rather than down. The attempt's own start time is exact.
    starts = (df.set_index("task_id")["run_started_at"]
              .apply(pd.to_numeric, errors="coerce").dropna())
    raw = raw.join(starts.rename("t0"), on="task_id")
    raw = raw[raw["t0"].isna() | (raw["wall_epoch"] >= raw["t0"] - 60)]
    raw = raw.drop(columns=["t0"])
    # Even inside the attempt window the series can start with the DEAD attempt's
    # frozen count (the stale-manifest reporting bug), then drop to the live
    # attempt's real count. Cut at the last downward step so only the live
    # attempt survives.
    drops = raw.groupby("task_id", sort=False)["jobs_completed"].diff().lt(0)
    raw = raw[drops.groupby(raw["task_id"]).cumsum()
              == drops.groupby(raw["task_id"]).transform("sum")]

    # The launcher appends a row per task per tick forever, so a finished run
    # keeps emitting its frozen final count. Cut each series at its last real
    # advance, or every curve grows a long flat tail past the run's own end.
    adv = raw.groupby("task_id", sort=False)["jobs_completed"].diff().fillna(1) > 0
    raw = raw[adv.groupby(raw["task_id"]).transform(
        lambda s: s[::-1].cummax()[::-1].astype(bool))]
    out = []
    for task, g in raw.groupby("task_id", sort=False):
        if len(g) > per_task:
            g = g.iloc[np.linspace(0, len(g) - 1, per_task).astype(int)]
        g = g.copy()
        g["elapsed_h"] = (g["wall_epoch"] - g["wall_epoch"].iloc[0]) / 3600.0
        out.append(g[["task_id", "wall_epoch", "elapsed_h", "jobs_completed"]])
    res = pd.concat(out, ignore_index=True)
    res.insert(0, "host", host)
    return res


# ---------------------------------------------------------------- all hosts
def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hosts", type=Path, default=DEFAULT_HOSTS)
    p.add_argument("--only", action="append", metavar="HOST")
    p.add_argument("--force", action="store_true", help="ignore the cache")
    p.add_argument("--out", type=Path, help="merged CSV (default <shared>/metrics_all.csv)")
    p.add_argument("--timeout", type=int, default=3600)
    p.add_argument("--harvest-local", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--host", help=argparse.SUPPRESS)
    p.add_argument("--root", help=argparse.SUPPRESS)
    p.add_argument("--outfile", help=argparse.SUPPRESS)
    args = p.parse_args(argv)

    if args.harvest_local:
        return 0 if harvest_host(args.root, args.host, args.outfile, args.force) >= 0 else 1

    sys.path.insert(0, str(REPO / "src"))
    from flux_fiction.ensemble import campaign, hosts as hosts_mod

    config = hosts_mod.load_host_config(args.hosts)
    shared = Path(config.shared_root).expanduser()
    here = hosts_mod.detect_host(config)
    script = str(Path(__file__).resolve())
    started = time.time()

    for name in sorted(config.hosts):
        if args.only and name not in args.only:
            continue
        assignment = campaign.read_json(shared / "hosts" / name / "assignment.json")
        root = assignment.get("campaign_root")
        if not root:
            sys.stderr.write("%s: no campaign root published yet, skipping\n" % name)
            continue
        out = str(shared / "hosts" / name / "metrics.csv")
        cmd = [PY, script, "--harvest-local", "--host", name, "--root", root,
               "--outfile", out] + (["--force"] if args.force else [])
        argv_ = cmd if name == here else [
            "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
            name, " ".join(cmd)]
        try:
            done = subprocess.run(argv_, capture_output=True, text=True,
                                  timeout=args.timeout)
        except subprocess.TimeoutExpired:
            sys.stderr.write("%s: harvest timed out\n" % name)
            continue
        sys.stderr.write((done.stderr or "").strip()[-400:] + "\n")

    # Reading a directory is what invalidates the NFS dentry cache; without it a
    # file another cluster wrote seconds ago can still read as absent here.
    frames = []
    for name in sorted(config.hosts):
        path = shared / "hosts" / name / "metrics.csv"
        for _ in range(4):
            try:
                frames.append(pd.read_csv(path))
                break
            except (OSError, ValueError):
                try:
                    os.listdir(path.parent)
                except OSError:
                    pass
                time.sleep(0.4)
    if not frames:
        sys.stderr.write("nothing harvested\n")
        return 1
    merged = pd.concat(frames, ignore_index=True)
    out_path = args.out or (shared / "metrics_all.csv")
    merged.to_csv(out_path, index=False)

    tframes = []
    for name in sorted(config.hosts):
        try:
            tframes.append(pd.read_csv(shared / "hosts" / name / "trajectories.csv"))
        except (OSError, ValueError):
            continue
    if tframes:
        tall = pd.concat(tframes, ignore_index=True)
        tall.to_csv(Path(out_path).with_name("trajectories_all.csv"), index=False)
    fin = int(merged["finalized"].astype(str).str.lower().eq("true").sum())
    print("%d run(s) from %d host(s), %d finalized -> %s   [%.0fs]"
          % (len(merged), len(frames), fin, out_path, time.time() - started))
    return 0


if __name__ == "__main__":
    sys.exit(main())
