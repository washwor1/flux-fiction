#!/usr/bin/env python3
"""One-command status report for a multi-host Flux Fiction campaign.

Run it from any cluster:

    util/ff_report.py                      # refresh every host, then report
    util/ff_report.py --no-refresh         # report on what is already published
    util/ff_report.py --by rabbit_job_percentage
    util/ff_report.py --csv ~/snapshot.csv

Why a script rather than the bare CLI: the pieces live in three places that no
single host can reach.

  * Each host's campaign root is on ITS OWN lustre -- tuolumne mounts lustre5,
    dane and corona mount lustre1, and neither sees the other. So the refresh
    for a remote host has to happen over ssh, on that host.
  * results.csv is only regenerated automatically when a campaign drains
    (campaign.py, `if batches_drained`), while the publish step copies whatever
    file is already there on every tick. Mid-campaign the published table can
    therefore be an arbitrarily old snapshot -- corona spent 2026-07-27 serving
    a row that said `failed, 5173` for a run that was alive at 7,271. Refreshing
    means calling write_results_csv explicitly.
  * Only /usr/WS1 (the shared_root) is mounted everywhere, so that is where the
    per-host tables are collected and merged.

--refresh-local is the internal half: it runs on one host, rewrites that host's
results.csv from its own lustre, and publishes it to the shared root. The
orchestrating half calls it locally for this host and over ssh for the others.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# The default python3 on dane and corona is 3.6.8 with no tomllib and no tomli,
# so the campaign spec will not load under it. Always use the LC anaconda build.
PY = os.environ.get(
    "FF_PYTHON",
    "/collab/usr/gapps/python/toss_4_x86_64_ib/anaconda3-2025.3.1/bin/python3",
)
DEFAULT_HOSTS = REPO / "test-inputs" / "hosts-rabbit-threshold.toml"


def _import_campaign():
    sys.path.insert(0, str(REPO / "src"))
    from flux_fiction.ensemble import campaign, hosts  # noqa: E402

    return campaign, hosts


# --------------------------------------------------------------------------
# half one: runs ON a host, against that host's own lustre
# --------------------------------------------------------------------------
def refresh_local(hosts_file: Path, host_name: str | None) -> int:
    campaign, hosts_mod = _import_campaign()
    config = hosts_mod.load_host_config(hosts_file)
    host_name = host_name or hosts_mod.detect_host(config)
    if not host_name:
        print(
            f"ERROR: {socket.gethostname()} matches no host in {hosts_file}",
            file=sys.stderr,
        )
        return 2

    # The shared context records where each host put its campaign root, so we
    # do not have to re-derive it from the spec (and cannot get it wrong if the
    # spec is edited later).
    shared = Path(config.shared_root).expanduser()
    assignment = campaign.read_json(shared / "hosts" / host_name / "assignment.json")
    root = Path(assignment.get("campaign_root") or "")
    if not root.is_dir():
        print(f"{host_name}: no campaign root yet ({root})", file=sys.stderr)
        return 1

    path = campaign.write_results_csv(root)
    published = campaign.publish_host_results(root, config, host_name)
    rows = max(0, sum(1 for _ in path.open(encoding="utf-8")) - 1)

    wall = collect_walltime(root, config.hosts[host_name])
    (shared / "hosts" / host_name / "walltime.json").write_text(
        json.dumps(wall, indent=1) + "\n", encoding="utf-8")
    print(f"{host_name}: refreshed {rows} task row(s), {wall['n_running']} batch(es) "
          f"running -> {published or path}")
    return 0


def _parse_iso(s: str) -> float | None:
    """Epoch seconds from an ISO-8601 Z stamp, with or without fractional seconds."""
    if not s:
        return None
    s = s.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return (datetime.datetime.strptime(s, fmt)
                    .replace(tzinfo=datetime.timezone.utc).timestamp())
        except ValueError:
            continue
    return None


def collect_walltime(root: Path, host_settings) -> dict:
    """How long each live batch has been running, and where that pace lands it.

    Read from each batch's newest parallel_status.json rather than the RJMS, so
    one code path covers both the flux and slurm hosts. Those timestamps are
    written by the parent runner, which is NOT preloaded with libfaketime, so
    unlike anything the child writes they are real wall clock.
    """
    now = time.time()
    limit_h = (getattr(host_settings, "walltime_minutes", 0) or 0) / 60.0
    elapsed, projected, over = [], [], 0
    for batch_dir in sorted((root / "batches").glob("b*")):
        manifests = sorted((batch_dir / "parallel").glob("*_manifest"))
        if not manifests:
            continue
        status_file = manifests[-1] / "parallel_status.json"
        try:
            if now - status_file.stat().st_mtime > 1800:
                continue                      # not being written: finished or dead
            payload = json.loads(status_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if payload.get("state") != "running":
            continue
        started = _parse_iso(str(payload.get("started_at") or ""))
        if started is None:
            continue
        hours = (now - started) / 3600.0
        elapsed.append(hours)
        runs = payload.get("runs") or []
        done = sum(int(r.get("jobs_completed") or 0) for r in runs)
        total = sum(int(r.get("jobs_total") or 0) for r in runs)
        if total and done and hours > 0:
            proj = hours * total / done
            projected.append(proj)
            if limit_h and proj > limit_h:
                over += 1
    med = lambda v: statistics.median(v) if v else None
    return {"n_running": len(elapsed), "walltime_limit_h": limit_h or None,
            "elapsed_mean_h": statistics.mean(elapsed) if elapsed else None,
            "elapsed_median_h": med(elapsed),
            "elapsed_max_h": max(elapsed) if elapsed else None,
            "projected_median_h": med(projected),
            "n_projected_over_limit": over, "n_projected": len(projected)}


# --------------------------------------------------------------------------
# half two: orchestrates, then reports from the shared root
# --------------------------------------------------------------------------
def refresh_all(hosts_file: Path, only: list[str] | None, timeout: int) -> None:
    campaign, hosts_mod = _import_campaign()
    config = hosts_mod.load_host_config(hosts_file)
    here = hosts_mod.detect_host(config)
    script = Path(__file__).resolve()

    for name in sorted(config.hosts):
        if only and name not in only:
            continue
        cmd = [
            str(script), "--refresh-local", "--host", name, "--hosts", str(hosts_file),
        ]
        if name == here:
            argv = [PY] + cmd
        else:
            # Quoting is safe here: every element is a path we control.
            argv = [
                "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
                name, " ".join([PY] + cmd),
            ]
        try:
            done = subprocess.run(
                argv, capture_output=True, text=True, timeout=timeout
            )
        except subprocess.TimeoutExpired:
            print(f"{name}: refresh timed out after {timeout}s (skipped)", file=sys.stderr)
            continue
        out = (done.stdout or "").strip()
        err = (done.stderr or "").strip()
        if out:
            print(out)
        if done.returncode != 0 and err:
            print(f"{name}: {err.splitlines()[-1]}", file=sys.stderr)


def revalidate(shared: Path, names) -> None:
    """Force the NFS client to re-look-up files another cluster just wrote.

    The shared root is NFS with close-to-open consistency: a file a remote host
    finished writing a second ago can still be missing from this client's cached
    directory entry, so an immediate open() fails even though the data is there.
    Reading the directory is what invalidates that cache. Without this the report
    silently dropped dane's and corona's freshly published files while showing
    tuolumne's, which was written locally.
    """
    for name in names:
        for _ in range(4):
            try:
                os.listdir(shared / "hosts" / name)
                break
            except OSError:
                time.sleep(0.4)


def _read_json_shared(path: Path) -> dict:
    """Read a small JSON a remote host may have written moments ago."""
    for attempt in range(4):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            try:
                os.listdir(path.parent)
            except OSError:
                pass
            if attempt < 3:
                time.sleep(0.4)
    return {}


def _f(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_complete(row: dict) -> float | None:
    done, total = _f(row.get("jobs_completed", "")), _f(row.get("jobs_total", ""))
    if done is None or not total:
        return None
    return 100.0 * done / total


def _fmt(value: float | None, width: int = 7, places: int = 1) -> str:
    return " " * width if value is None else f"{value:{width}.{places}f}"


def report(hosts_file: Path, group_by: list[str], csv_out: Path | None) -> int:
    campaign, hosts_mod = _import_campaign()
    config = hosts_mod.load_host_config(hosts_file)
    revalidate(Path(config.shared_root).expanduser(), sorted(config.hosts))
    out_path, per_host = campaign.merge_host_results(config)

    with out_path.open(newline="", encoding="utf-8") as f:
        rows = [dict(r) for r in csv.DictReader(f)]
    if not rows:
        print("Nothing published yet. Run without --no-refresh, or wait for a tick.")
        return 1

    print(f"Merged {len(rows)} task row(s) -> {out_path}\n")

    # --- per host -------------------------------------------------------
    shared = Path(config.shared_root).expanduser()
    walls = {name: _read_json_shared(shared / "hosts" / name / "walltime.json")
             for name in per_host}
    print(f"{'HOST':10s} {'TASKS':>6s} {'DONE':>6s} {'RUN':>6s} {'FAIL':>6s} "
          f"{'TIMEOUT':>8s} {'QUEUED':>7s} {'SIM JOBS':>12s} {'MEAN %':>7s} "
          f"{'ELAPSED':>8s} {'MAX':>6s} {'PROJ':>7s} {'LIMIT':>6s} {'OVER':>9s}")
    for name in sorted(per_host):
        hr = [r for r in rows if r["host"] == name]
        if not hr:
            print(f"{name:10s} {0:6d}   (nothing published yet)")
            continue
        states = [r.get("state", "") for r in hr]
        pcts = [p for p in (_pct_complete(r) for r in hr) if p is not None]
        jobs = sum(_f(r.get("jobs_completed", "")) or 0 for r in hr)
        w = walls.get(name) or {}
        over = (f"{w['n_projected_over_limit']}/{w['n_projected']}"
                if w.get("n_projected") else "")
        print(
            f"{name:10s} {len(hr):6d} {states.count('succeeded'):6d} "
            f"{states.count('running'):6d} {states.count('failed'):6d} "
            f"{states.count('timeout'):8d} {states.count('queued'):7d} "
            f"{int(jobs):12,d} {_fmt(statistics.mean(pcts) if pcts else None)} "
            f"{_fmt(w.get('elapsed_mean_h'), 8)} {_fmt(w.get('elapsed_max_h'), 6)} "
            f"{_fmt(w.get('projected_median_h'), 7, 0)} "
            f"{_fmt(w.get('walltime_limit_h'), 6, 0)} {over:>9s}"
        )
    print("\nELAPSED/MAX = hours the running batches have been going (mean, worst). "
          "PROJ = hours\na batch needs to reach 100% at its own observed pace; "
          "OVER = how many exceed LIMIT.")

    # --- grid rollup ----------------------------------------------------
    print()
    header = " ".join(f"{k[:18]:>18s}" for k in group_by)
    print(f"{header} {'N':>4s} {'MEAN %':>7s} {'MIN %':>7s} {'MAX %':>7s} "
          f"{'MAKESPAN(s)':>12s} {'AVG WAIT(s)':>12s}")
    buckets: dict[tuple, list[dict]] = {}
    for r in rows:
        buckets.setdefault(tuple(r.get(k, "") for k in group_by), []).append(r)

    def sort_key(item):
        # Numeric grid axes (rabbit percentages) must not sort as strings.
        return tuple((_f(v) if _f(v) is not None else float("inf"), str(v))
                     for v in item[0])

    for key, group in sorted(buckets.items(), key=sort_key):
        pcts = [p for p in (_pct_complete(r) for r in group) if p is not None]
        mk = [v for v in (_f(r.get("makespan_seconds", "")) for r in group) if v]
        wait = [v for v in (_f(r.get("avg_queue_wait_seconds", "")) for r in group) if v]
        label = " ".join(f"{str(k)[:18]:>18s}" for k in key)
        print(
            f"{label} {len(group):4d} "
            f"{_fmt(statistics.mean(pcts) if pcts else None)} "
            f"{_fmt(min(pcts) if pcts else None)} "
            f"{_fmt(max(pcts) if pcts else None)} "
            f"{_fmt(statistics.median(mk) if mk else None, 12, 0)} "
            f"{_fmt(statistics.median(wait) if wait else None, 12, 0)}"
        )

    done = sum(1 for r in rows if r.get("state") == "succeeded")
    if not done:
        print("\nNo task has finished yet, so MAKESPAN and AVG WAIT are blank; "
              "MEAN % is jobs_completed/jobs_total, the live proxy.")

    if csv_out:
        csv_out = csv_out.expanduser()
        csv_out.write_bytes(out_path.read_bytes())
        print(f"\nCopied merged table -> {csv_out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hosts", type=Path, default=DEFAULT_HOSTS,
                   help=f"host config TOML (default {DEFAULT_HOSTS})")
    p.add_argument("--no-refresh", action="store_true",
                   help="skip the per-host refresh; merge what is already published")
    p.add_argument("--only", action="append", metavar="HOST",
                   help="refresh only this host (repeatable)")
    p.add_argument("--by", action="append", metavar="COLUMN",
                   help="group the rollup by this results.csv column (repeatable; "
                        "default queue_policy + match_policy)")
    p.add_argument("--csv", type=Path, metavar="PATH",
                   help="also copy the merged table here")
    p.add_argument("--timeout", type=int, default=900,
                   help="seconds to allow each host's refresh (default 900)")
    p.add_argument("--refresh-local", action="store_true",
                   help=argparse.SUPPRESS)
    p.add_argument("--host", help=argparse.SUPPRESS)
    args = p.parse_args(argv)

    if args.refresh_local:
        return refresh_local(args.hosts, args.host)

    if not args.no_refresh:
        refresh_all(args.hosts, args.only, args.timeout)
        print()
    return report(args.hosts, args.by or ["queue_policy", "match_policy"], args.csv)


if __name__ == "__main__":
    sys.exit(main())
