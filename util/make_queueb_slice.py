#!/usr/bin/env python3
"""Derive a usable simulation trace from the raw Tuolumne export.

The raw export (``tuo_data_chrono_clean.csv``) is sorted by ``@timestamp``,
which is the job's END time, and it mixes every queue together. Three problems
follow from that, all of which this script fixes:

1. **Queue mixing.** Only ``queueB`` is representative of pbatch; queueA/C are
   small, short-job queues that together offer ~1% load.

2. **Submit-time ordering.** Only ~75% of adjacent rows ascend by
   ``job.submittime``. The ensemble's interarrival shake computes each gap from
   *adjacent rows*, so out-of-order rows produce negative interarrivals.

3. **Straggler head.** A positional slice picks up jobs that merely *ended* in
   the window but were submitted weeks earlier. The simulation replays from the
   earliest submit, so <1% of rows can prepend hundreds of hours of simulated
   dead time, which dominates makespan and utilization.

Selecting a contiguous *submit-time* window fixes 2 and 3 together: every job in
the output was submitted inside the window, in order.

The window itself is chosen for the fragmentation study, not just for size:

* **offered load near saturation** -- below ~90% the cluster is rarely full, so
  makespan tracks the arrival span no matter what the scheduler does and the
  rabbit sweep would read flat. Load is computed as
  ``sum(duration x nodes) / (nnodes x span)``.
* **no multi-hour submission gap** -- an idle gap lets the queue drain and
  resets whatever fragmentation had built up.
* **not a job-array burst.** The raw trace contains several bursts of ~10k
  single-node jobs submitted inside an hour. Those score wonderfully on gap and
  load and are useless here: identical 1-node jobs cannot fragment anything.
  ``--max-hour-share`` rejects them.

Usage:
    python3 util/make_queueb_slice.py --survey          # show candidate windows
    python3 util/make_queueb_slice.py --out trace.csv   # write the best window
"""

from __future__ import annotations

import argparse
import bisect
import csv
from datetime import datetime
from pathlib import Path
import sys


DEFAULT_SOURCE = "/usr/WS1/ashworth12/ff-podman/tuo_data_chrono_clean.csv"
# The simulated machine, used only to turn demand into an offered-load percentage.
DEFAULT_NNODES = 1153


def parse_dt(value: str | None) -> datetime | None:
    text = (value or "").strip().strip('"')
    for fmt in ("%b %d, %Y @ %H:%M:%S.%f", "%b %d, %Y @ %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_num(value: str | None) -> float | None:
    text = (value or "").strip().strip('"').replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def load_rows(source: Path, queue: str) -> tuple[list[str], list[dict[str, str]]]:
    with source.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        rows = [r for r in reader if r.get("job.queue") == queue]
    kept = []
    for row in rows:
        submit = parse_dt(row.get("job.submittime"))
        duration = parse_num(row.get("event.duration_seconds"))
        if submit is None or duration is None:
            continue
        row["_submit"] = submit
        row["_duration"] = max(1.0, duration)
        row["_nodes"] = max(1, int(float(row.get("job.node.count") or 1)))
        kept.append(row)
    kept.sort(key=lambda r: r["_submit"])
    return fields, kept


def window_stats(window: list[dict], nnodes: int) -> dict | None:
    span = (window[-1]["_submit"] - window[0]["_submit"]).total_seconds()
    if span <= 0:
        return None
    gaps = sorted(
        (window[i + 1]["_submit"] - window[i]["_submit"]).total_seconds()
        for i in range(len(window) - 1)
    )
    demand = sum(r["_duration"] * r["_nodes"] for r in window)
    # Largest share of the window's jobs landing inside any one-hour period.
    # Sorted, so a single forward-walking cursor gives this in one pass.
    stamps = [r["_submit"].timestamp() for r in window]
    worst = 0
    j = 0
    for i, stamp in enumerate(stamps):
        limit = stamp + 3600.0
        if j < i:
            j = i
        while j < len(stamps) and stamps[j] <= limit:
            j += 1
        worst = max(worst, j - i)
    return {
        "start": window[0]["_submit"],
        "end": window[-1]["_submit"],
        "span_days": span / 86400.0,
        "load_pct": 100.0 * demand / (nnodes * span),
        "max_gap_min": gaps[-1] / 60.0,
        "p99_gap_s": gaps[int(0.99 * len(gaps))],
        "max_hour_share_pct": 100.0 * worst / len(window),
    }


def survey(
    rows: list[dict],
    *,
    count: int,
    nnodes: int,
    step: int,
    min_span_days: float,
    load_range: tuple[float, float],
    max_hour_share: float,
) -> list[tuple[int, dict]]:
    out = []
    for start in range(0, len(rows) - count + 1, step):
        stats = window_stats(rows[start : start + count], nnodes)
        if stats is None:
            continue
        if stats["span_days"] < min_span_days:
            continue
        if not load_range[0] <= stats["load_pct"] <= load_range[1]:
            continue
        if stats["max_hour_share_pct"] > max_hour_share:
            continue
        out.append((start, stats))
    # Best = smallest worst-case submission gap.
    out.sort(key=lambda item: item[1]["max_gap_min"])
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default=DEFAULT_SOURCE)
    ap.add_argument("--queue", default="queueB")
    ap.add_argument("--count", type=int, default=10000, help="jobs in the slice")
    ap.add_argument("--nnodes", type=int, default=DEFAULT_NNODES)
    ap.add_argument("--step", type=int, default=500, help="survey stride, in rows")
    ap.add_argument("--min-span-days", type=float, default=1.5)
    ap.add_argument("--min-load", type=float, default=95.0)
    ap.add_argument("--max-load", type=float, default=160.0)
    ap.add_argument("--max-hour-share", type=float, default=15.0,
                    help="reject windows where one hour holds more than this %% of jobs")
    ap.add_argument("--start-index", type=int, default=None,
                    help="skip the survey and take the window at this row index")
    ap.add_argument("--survey", action="store_true", help="print candidates and exit")
    ap.add_argument("--out", default=None, help="write the selected window here")
    args = ap.parse_args(argv)

    fields, rows = load_rows(Path(args.source), args.queue)
    print(f"{args.queue}: {len(rows)} rows with a parseable submit time and duration", file=sys.stderr)

    if args.start_index is None:
        candidates = survey(
            rows,
            count=args.count,
            nnodes=args.nnodes,
            step=args.step,
            min_span_days=args.min_span_days,
            load_range=(args.min_load, args.max_load),
            max_hour_share=args.max_hour_share,
        )
        if not candidates:
            print("No window satisfied the constraints; loosen them.", file=sys.stderr)
            return 1
        if args.survey:
            header = f"{'idx':>8} {'submit_start':<20} {'span_d':>7} {'load%':>7} {'maxgap_m':>9} {'p99gap_s':>9} {'max1h%':>7}"
            print(header)
            for start, s in candidates[:15]:
                print(
                    f"{start:>8} {str(s['start'])[:19]:<20} {s['span_days']:>7.2f} "
                    f"{s['load_pct']:>7.1f} {s['max_gap_min']:>9.1f} {s['p99_gap_s']:>9.1f} "
                    f"{s['max_hour_share_pct']:>7.1f}"
                )
            return 0
        start_index = candidates[0][0]
    else:
        start_index = args.start_index

    window = rows[start_index : start_index + args.count]
    stats = window_stats(window, args.nnodes)
    assert stats is not None
    print(
        "selected rows [{a}:{b}]  {start} -> {end}\n"
        "  span={span:.2f} d  offered_load={load:.1f}%  max_submit_gap={gap:.1f} min  "
        "p99_gap={p99:.0f} s  max_1h_share={share:.1f}%".format(
            a=start_index,
            b=start_index + args.count,
            start=stats["start"],
            end=stats["end"],
            span=stats["span_days"],
            load=stats["load_pct"],
            gap=stats["max_gap_min"],
            p99=stats["p99_gap_s"],
            share=stats["max_hour_share_pct"],
        ),
        file=sys.stderr,
    )

    if not args.out:
        return 0

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in window:
            writer.writerow({key: row.get(key, "") for key in fields})
    print(f"wrote {out_path} ({len(window)} rows)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
