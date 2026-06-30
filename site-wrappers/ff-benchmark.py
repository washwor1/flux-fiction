#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import glob
import json
import os
from pathlib import Path
import shlex
import statistics
import subprocess
import sys
import time
from typing import Any


PATH_LABELS = {
    "Source config": "source_config",
    "Generated config": "generated_config",
    "Output dir": "output_dir",
    "Broker log": "broker_log",
    "Run log": "run_log",
    "Stamp file": "stamp_file",
    "OTel socket": "otel_socket",
    "OTel summary": "otel_summary",
    "OTel spans": "otel_spans",
    "OTel bridge log": "otel_bridge_log",
    "Reproducer": "reproducer",
    "Parallel status": "parallel_status",
    "Parallel summary": "parallel_summary",
    "Manifest snapshot": "manifest_snapshot",
}

SUMMARY_SNIPPET_KEYS = (
    "state",
    "jobs_total",
    "jobs_completed",
    "makespan_seconds",
    "makespan_hours",
    "resource_summary",
)

STATUS_SNIPPET_KEYS = (
    "state",
    "jobs_total",
    "jobs_submitted",
    "jobs_started",
    "jobs_running",
    "jobs_completed",
    "current_sim_time",
    "time_step",
    "failure_reason",
    "return_code",
)

PARALLEL_SNIPPET_KEYS = (
    "state",
    "total_runs",
    "queued",
    "launching",
    "running",
    "succeeded",
    "failed",
    "skipped",
    "interrupted",
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_path(value: str, cwd: Path) -> Path:
    path = Path(value.strip()).expanduser()
    if path.is_absolute():
        return path
    return (cwd / path).resolve()


def parse_env_overrides(entries: list[str]) -> dict[str, str]:
    env: dict[str, str] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Environment override must look like KEY=VALUE: {entry!r}")
        key, value = entry.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Environment override key must be non-empty: {entry!r}")
        env[key] = value
    return env


def _coerce_text_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def augment_flux_pythonpath(env: dict[str, str]) -> dict[str, str]:
    updated = dict(env)
    flux_prefix = Path(updated.get("FLUX_PREFIX", "/workspace/container-installs/flux-core"))
    candidates = [
        flux_prefix / "lib" / "flux" / "python3.12",
        flux_prefix / "lib" / "python3.12" / "site-packages",
        flux_prefix / "local" / "lib" / "python3.12" / "dist-packages",
    ]
    existing = [part for part in updated.get("PYTHONPATH", "").split(os.pathsep) if part]
    merged: list[str] = []
    for path in [*(str(candidate) for candidate in candidates if candidate.exists()), *existing]:
        if path and path not in merged:
            merged.append(path)
    if merged:
        updated["PYTHONPATH"] = os.pathsep.join(merged)
    return updated


def parse_stdout_paths(stdout: str, cwd: Path) -> dict[str, str]:
    discovered: dict[str, str] = {}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        label, _, value = line.partition(":")
        key = PATH_LABELS.get(label.strip())
        if key is None:
            continue
        value = value.strip()
        if not value:
            continue
        discovered[key] = str(_resolve_path(value, cwd))
    return discovered


def read_json_dict(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def parse_otel_summary(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "service": row.get("service", ""),
                    "source": row.get("source", ""),
                    "name": row.get("name", ""),
                    "count": int(float(row.get("count") or 0)),
                    "total_ms": float(row.get("total_ms") or 0.0),
                    "avg_ms": float(row.get("avg_ms") or 0.0),
                    "max_ms": float(row.get("max_ms") or 0.0),
                    "min_ms": float(row.get("min_ms") or 0.0),
                }
            )
    return rows


def parse_otel_spans_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            service = str(row.get("service", ""))
            source = str(row.get("source", ""))
            name = str(row.get("name", ""))
            duration_ms = float(row.get("duration_ms") or 0.0)
            key = (service, source, name)
            bucket = grouped.setdefault(
                key,
                {
                    "service": service,
                    "source": source,
                    "name": name,
                    "count": 0,
                    "total_ms": 0.0,
                    "max_ms": 0.0,
                    "min_ms": None,
                },
            )
            bucket["count"] += 1
            bucket["total_ms"] += duration_ms
            bucket["max_ms"] = max(float(bucket["max_ms"]), duration_ms)
            bucket["min_ms"] = duration_ms if bucket["min_ms"] is None else min(float(bucket["min_ms"]), duration_ms)

    rows: list[dict[str, Any]] = []
    for bucket in grouped.values():
        count = int(bucket["count"])
        total_ms = float(bucket["total_ms"])
        rows.append(
            {
                "service": bucket["service"],
                "source": bucket["source"],
                "name": bucket["name"],
                "count": count,
                "total_ms": total_ms,
                "avg_ms": total_ms / count if count else 0.0,
                "max_ms": float(bucket["max_ms"]),
                "min_ms": float(bucket["min_ms"] or 0.0),
            }
        )
    rows.sort(key=lambda item: item["total_ms"], reverse=True)
    return rows


def summarize_otel_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"span_rows": [], "total_ms_all_spans": 0.0}
    total_ms = sum(float(row.get("total_ms") or 0.0) for row in rows)
    return {
        "span_rows": rows,
        "total_ms_all_spans": total_ms,
    }


def discover_artifacts(stdout: str, cwd: Path, artifact_globs: list[str]) -> dict[str, Any]:
    paths = parse_stdout_paths(stdout, cwd)
    discovered: dict[str, Any] = {"paths": dict(paths), "extra_artifacts": []}

    serial_run_root: Path | None = None
    if "generated_config" in paths:
        serial_run_root = Path(paths["generated_config"]).parent
    elif "reproducer" in paths:
        serial_run_root = Path(paths["reproducer"]).parent

    parallel_root: Path | None = None
    if "parallel_summary" in paths:
        parallel_root = Path(paths["parallel_summary"]).parent
    elif "parallel_status" in paths:
        parallel_root = Path(paths["parallel_status"]).parent

    if serial_run_root is not None:
        discovered["run_root"] = str(serial_run_root)
        for key, candidate in (
            ("summary_json", serial_run_root / "summary.json"),
            ("status_json", serial_run_root / "status.json"),
            ("otel_summary", serial_run_root / "otel_summary.csv"),
            ("otel_spans", serial_run_root / "otel_spans.jsonl"),
        ):
            if key not in paths and candidate.exists():
                paths[key] = str(candidate)

    if parallel_root is not None:
        discovered["parallel_root"] = str(parallel_root)
        for key, candidate in (
            ("parallel_summary", parallel_root / "parallel_summary.json"),
            ("parallel_status", parallel_root / "parallel_status.json"),
        ):
            if key not in paths and candidate.exists():
                paths[key] = str(candidate)
        if "run_root" not in discovered:
            discovered["run_root"] = str(parallel_root)

    search_root = Path(discovered.get("run_root") or cwd)
    extra_artifacts: list[str] = []
    for pattern in artifact_globs:
        matches: list[str] = []
        if os.path.isabs(pattern):
            matches = sorted(glob.glob(pattern, recursive=True))
        else:
            matches = sorted(str(path) for path in search_root.glob(pattern))
        extra_artifacts.extend(matches)
    discovered["extra_artifacts"] = extra_artifacts
    discovered["paths"] = paths
    return discovered


def snippet_from_payload(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: payload[key] for key in keys if key in payload}


def compute_duration_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "mean_s": None,
            "median_s": None,
            "min_s": None,
            "max_s": None,
            "stdev_s": None,
        }
    return {
        "count": len(values),
        "mean_s": statistics.mean(values),
        "median_s": statistics.median(values),
        "min_s": min(values),
        "max_s": max(values),
        "stdev_s": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def aggregate_otel_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_span: dict[tuple[str, str, str], list[float]] = {}
    for sample in samples:
        if not sample.get("success"):
            continue
        for row in sample.get("otel", {}).get("span_rows", []):
            key = (str(row["service"]), str(row["source"]), str(row["name"]))
            by_span.setdefault(key, []).append(float(row["total_ms"]))

    aggregates = []
    for (service, source, name), totals in by_span.items():
        aggregates.append(
            {
                "service": service,
                "source": source,
                "name": name,
                "sample_count": len(totals),
                "mean_total_ms": statistics.mean(totals),
                "median_total_ms": statistics.median(totals),
                "min_total_ms": min(totals),
                "max_total_ms": max(totals),
            }
        )
    aggregates.sort(key=lambda item: item["median_total_ms"], reverse=True)
    return aggregates


def first_successful_sample(samples: list[dict[str, Any]]) -> dict[str, Any] | None:
    for sample in samples:
        if sample.get("success"):
            return sample
    return None


def consistency_warnings(samples: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    fields = ("jobs_total", "jobs_completed", "makespan_seconds", "state")
    for field in fields:
        values = []
        for sample in samples:
            summary = sample.get("summary", {})
            if field in summary:
                values.append(summary[field])
        unique = {json.dumps(value, sort_keys=True, default=str) for value in values}
        if len(unique) > 1:
            warnings.append(f"Inconsistent summary field across measured samples: {field}")

    parallel_fields = ("total_runs", "succeeded", "failed", "interrupted", "skipped")
    for field in parallel_fields:
        values = []
        for sample in samples:
            payload = sample.get("parallel_summary", {})
            if field in payload:
                values.append(payload[field])
        unique = {json.dumps(value, sort_keys=True, default=str) for value in values}
        if len(unique) > 1:
            warnings.append(f"Inconsistent parallel summary field across measured samples: {field}")
    return warnings


def build_variant_summary(label: str, command: str, warmups: list[dict[str, Any]], samples: list[dict[str, Any]]) -> dict[str, Any]:
    successful_durations = [float(sample["duration_s"]) for sample in samples if sample.get("success")]
    return {
        "label": label,
        "command": command,
        "warmups": warmups,
        "samples": samples,
        "stats": compute_duration_stats(successful_durations),
        "sample_count": len(samples),
        "success_count": sum(1 for sample in samples if sample.get("success")),
        "failure_count": sum(1 for sample in samples if not sample.get("success")),
        "otel_span_stats": aggregate_otel_samples(samples),
        "warnings": consistency_warnings(samples),
    }


def _delta_pct(baseline: float | None, candidate: float | None) -> float | None:
    if baseline in (None, 0) or candidate is None:
        return None
    return ((candidate - baseline) / baseline) * 100.0


def build_comparison(
    label_a: str,
    variant_a: dict[str, Any],
    label_b: str,
    variant_b: dict[str, Any],
) -> dict[str, Any]:
    stats_a = variant_a.get("stats", {})
    stats_b = variant_b.get("stats", {})
    median_a = stats_a.get("median_s")
    median_b = stats_b.get("median_s")
    delta_s = None if median_a is None or median_b is None else float(median_b) - float(median_a)
    runtime = {
        "baseline_label": label_a,
        "candidate_label": label_b,
        "baseline_median_s": median_a,
        "candidate_median_s": median_b,
        "delta_s": delta_s,
        "delta_pct": _delta_pct(median_a, median_b),
    }

    spans_a = {
        (row["service"], row["source"], row["name"]): row
        for row in variant_a.get("otel_span_stats", [])
    }
    spans_b = {
        (row["service"], row["source"], row["name"]): row
        for row in variant_b.get("otel_span_stats", [])
    }
    span_deltas: list[dict[str, Any]] = []
    for key in sorted(set(spans_a) | set(spans_b)):
        row_a = spans_a.get(key)
        row_b = spans_b.get(key)
        median_ms_a = None if row_a is None else float(row_a["median_total_ms"])
        median_ms_b = None if row_b is None else float(row_b["median_total_ms"])
        delta_ms = None
        if median_ms_a is not None and median_ms_b is not None:
            delta_ms = median_ms_b - median_ms_a
        elif median_ms_b is not None:
            delta_ms = median_ms_b
        elif median_ms_a is not None:
            delta_ms = -median_ms_a
        span_deltas.append(
            {
                "service": key[0],
                "source": key[1],
                "name": key[2],
                "baseline_median_total_ms": median_ms_a,
                "candidate_median_total_ms": median_ms_b,
                "delta_ms": delta_ms,
                "delta_pct": _delta_pct(median_ms_a, median_ms_b),
            }
        )
    span_deltas.sort(key=lambda item: abs(float(item["delta_ms"] or 0.0)), reverse=True)

    warnings: list[str] = []
    summary_fields = ("jobs_total", "jobs_completed", "makespan_seconds", "state")
    sample_a = first_successful_sample(variant_a.get("samples", []))
    sample_b = first_successful_sample(variant_b.get("samples", []))
    if sample_a and sample_b:
        for field in summary_fields:
            value_a = sample_a.get("summary", {}).get(field)
            value_b = sample_b.get("summary", {}).get(field)
            if value_a is not None and value_b is not None and value_a != value_b:
                warnings.append(
                    f"Variant summary mismatch for {field}: {label_a}={value_a!r}, {label_b}={value_b!r}"
                )

        parallel_fields = ("total_runs", "succeeded", "failed", "interrupted", "skipped")
        for field in parallel_fields:
            value_a = sample_a.get("parallel_summary", {}).get(field)
            value_b = sample_b.get("parallel_summary", {}).get(field)
            if value_a is not None and value_b is not None and value_a != value_b:
                warnings.append(
                    f"Variant parallel summary mismatch for {field}: {label_a}={value_a!r}, {label_b}={value_b!r}"
                )

    return {
        "runtime": runtime,
        "span_deltas": span_deltas,
        "warnings": warnings,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _fmt_float(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.6f}"


def _fmt_seconds_from_ms(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) / 1000.0:.3f}"


def _runtime_report_lines(variant: dict[str, Any]) -> list[str]:
    stats = variant["stats"]
    lines = [
        f"- Successful measured samples: `{variant['success_count']}/{variant['sample_count']}`",
    ]
    if int(stats.get("count") or 0) <= 1:
        lines.append(f"- Wall time: `{_fmt_float(stats['median_s'])}` s")
        return lines
    lines.extend(
        [
            f"- Median wall time: `{_fmt_float(stats['median_s'])}` s",
            f"- Mean wall time: `{_fmt_float(stats['mean_s'])}` s",
            f"- Min wall time: `{_fmt_float(stats['min_s'])}` s",
            f"- Max wall time: `{_fmt_float(stats['max_s'])}` s",
            f"- Stddev wall time: `{_fmt_float(stats['stdev_s'])}` s",
        ]
    )
    return lines


def _markdown_table(
    headers: list[str],
    rows: list[list[str]],
    *,
    right_align: set[int] | None = None,
) -> list[str]:
    right_align = right_align or set()
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _format_row(row: list[str]) -> str:
        cells: list[str] = []
        for idx, cell in enumerate(row):
            if idx in right_align:
                cells.append(cell.rjust(widths[idx]))
            else:
                cells.append(cell.ljust(widths[idx]))
        return f"| {' | '.join(cells)} |"

    separator_cells: list[str] = []
    for idx, width in enumerate(widths):
        if idx in right_align:
            separator_cells.append("-" * max(width - 1, 1) + ":")
        else:
            separator_cells.append("-" * width)

    table = [_format_row(headers), f"| {' | '.join(separator_cells)} |"]
    table.extend(_format_row(row) for row in rows)
    return table


def write_report(path: Path, benchmark: dict[str, Any]) -> None:
    lines = [
        "# Benchmark Report",
        "",
        f"- Created: `{benchmark['created_at']}`",
        f"- Mode: `{benchmark['mode']}`",
        f"- Repeat: `{benchmark['repeat']}`",
        f"- Warmup: `{benchmark['warmup']}`",
    ]
    if benchmark.get("notes"):
        lines.append(f"- Notes: `{benchmark['notes']}`")

    variants = list(benchmark["variants"].values())
    if benchmark["mode"] == "single":
        variant = variants[0]
        lines.extend(
            [
                f"- Label: `{variant['label']}`",
                f"- Command: `{variant['command']}`",
                "",
                "## Runtime",
                "",
            ]
        )
        lines.extend(_runtime_report_lines(variant))
    else:
        lines.extend(["", "## Variants", ""])
        for variant in variants:
            lines.extend(
                [
                    f"### {variant['label']}",
                    "",
                    f"- Command: `{variant['command']}`",
                ]
            )
            lines.extend(_runtime_report_lines(variant))
            lines.append("")

        comparison = benchmark.get("comparison", {})
        runtime = comparison.get("runtime", {})
        lines.extend(
            [
                "## Comparison",
                "",
                f"- Baseline: `{runtime.get('baseline_label', '')}`",
                f"- Candidate: `{runtime.get('candidate_label', '')}`",
                f"- Median delta: `{_fmt_float(runtime.get('delta_s'))}` s",
                f"- Median delta pct: `{_fmt_float(runtime.get('delta_pct'))}` %",
                "",
            ]
        )
        span_deltas = comparison.get("span_deltas", [])
        if span_deltas:
            span_rows = [
                [
                    str(row["service"]),
                    str(row["source"]),
                    str(row["name"]),
                    _fmt_seconds_from_ms(row["baseline_median_total_ms"]),
                    _fmt_seconds_from_ms(row["candidate_median_total_ms"]),
                    _fmt_seconds_from_ms(row["delta_ms"]),
                    _fmt_float(row["delta_pct"]),
                ]
                for row in span_deltas[:15]
            ]
            lines.extend(["## Span Deltas", ""])
            lines.extend(
                _markdown_table(
                    [
                        "Service",
                        "Source",
                        "Span",
                        "Baseline Median s",
                        "Candidate Median s",
                        "Delta s",
                        "Delta %",
                    ],
                    span_rows,
                    right_align={3, 4, 5, 6},
                )
            )

    for variant in variants:
        sample_rows = [
            [
                str(sample["sample_id"]),
                "yes" if sample.get("measured") else "no",
                "yes" if sample.get("success") else "no",
                str(sample.get("return_code", "n/a")),
                _fmt_float(sample.get("duration_s")),
                f"`{sample.get('run_root') or ''}`",
            ]
            for sample in [*variant["warmups"], *variant["samples"]]
        ]
        lines.extend(["", f"## Samples: {variant['label']}", ""])
        lines.extend(
            _markdown_table(
                ["Sample", "Measured", "Success", "RC", "Duration (s)", "Run Root"],
                sample_rows,
                right_align={3, 4},
            )
        )
        if variant["warnings"]:
            lines.extend(["", f"### Warnings: {variant['label']}", ""])
            for warning in variant["warnings"]:
                lines.append(f"- {warning}")
        if variant["otel_span_stats"]:
            top_span_rows = [
                [
                    str(row["service"]),
                    str(row["source"]),
                    str(row["name"]),
                    str(row["sample_count"]),
                    _fmt_seconds_from_ms(row["median_total_ms"]),
                    _fmt_seconds_from_ms(row["mean_total_ms"]),
                ]
                for row in variant["otel_span_stats"][:10]
            ]
            lines.extend(["", f"### Top OTel Spans: {variant['label']}", ""])
            lines.extend(
                _markdown_table(
                    ["Service", "Source", "Span", "Samples", "Median Total s", "Mean Total s"],
                    top_span_rows,
                    right_align={3, 4, 5},
                )
            )

    comparison_warnings = benchmark.get("comparison", {}).get("warnings", [])
    if comparison_warnings:
        lines.extend(["", "## Comparison Warnings", ""])
        for warning in comparison_warnings:
            lines.append(f"- {warning}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_sample(
    *,
    sample_id: str,
    measured: bool,
    command: str,
    sample_dir: Path,
    cwd: Path,
    env: dict[str, str],
    timeout: float | None,
    require_otel: bool,
    artifact_globs: list[str],
) -> dict[str, Any]:
    sample_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = sample_dir / "stdout.log"
    stderr_path = sample_dir / "stderr.log"
    started_at = utcnow_iso()
    started_perf = time.perf_counter()
    timed_out = False
    proc: subprocess.CompletedProcess[str] | None = None
    stdout = ""
    stderr = ""
    return_code: int | None = None

    try:
        proc = subprocess.run(
            ["bash", "-lc", command],
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        stdout = proc.stdout
        stderr = proc.stderr
        return_code = proc.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = _coerce_text_output(exc.stdout)
        stderr = _coerce_text_output(exc.stderr)
        return_code = None

    finished_at = utcnow_iso()
    duration_s = time.perf_counter() - started_perf
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")

    artifact_data = discover_artifacts(stdout, cwd, artifact_globs)
    path_map = artifact_data["paths"]
    summary_json_path = Path(path_map["summary_json"]) if "summary_json" in path_map else None
    status_json_path = Path(path_map["status_json"]) if "status_json" in path_map else None
    otel_summary_path = Path(path_map["otel_summary"]) if "otel_summary" in path_map else None
    otel_spans_path = Path(path_map["otel_spans"]) if "otel_spans" in path_map else None
    parallel_summary_path = Path(path_map["parallel_summary"]) if "parallel_summary" in path_map else None
    parallel_status_path = Path(path_map["parallel_status"]) if "parallel_status" in path_map else None

    summary_payload = snippet_from_payload(read_json_dict(summary_json_path), SUMMARY_SNIPPET_KEYS)
    status_payload = snippet_from_payload(read_json_dict(status_json_path), STATUS_SNIPPET_KEYS)
    parallel_summary_payload = snippet_from_payload(read_json_dict(parallel_summary_path), PARALLEL_SNIPPET_KEYS)
    parallel_status_payload = snippet_from_payload(read_json_dict(parallel_status_path), PARALLEL_SNIPPET_KEYS)
    otel_rows = parse_otel_summary(otel_summary_path)
    if not otel_rows:
        otel_rows = parse_otel_spans_jsonl(otel_spans_path)
    otel = summarize_otel_rows(otel_rows)

    warnings: list[str] = []
    success = (return_code == 0 and not timed_out)
    if require_otel and not otel_rows:
        success = False
        warnings.append("Required OTel summary was not discovered.")
    if timed_out:
        warnings.append("Command timed out.")

    sample = {
        "sample_id": sample_id,
        "measured": measured,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": duration_s,
        "return_code": return_code,
        "timed_out": timed_out,
        "success": success,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "run_root": artifact_data.get("run_root"),
        "parallel_root": artifact_data.get("parallel_root"),
        "discovered_paths": path_map,
        "extra_artifacts": artifact_data["extra_artifacts"],
        "summary": summary_payload,
        "status": status_payload,
        "parallel_summary": parallel_summary_payload,
        "parallel_status": parallel_status_payload,
        "otel": otel,
        "warnings": warnings,
    }
    write_json(sample_dir / "sample.json", sample)
    return sample


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark a Flux Fiction command by repeated execution.")
    parser.add_argument("--cmd", default=None, help="Command to benchmark in single-variant mode.")
    parser.add_argument("--label", default="current", help="Display label for single-variant mode.")
    parser.add_argument("--cmd-a", default=None, help="Baseline command for compare mode.")
    parser.add_argument("--label-a", default="baseline", help="Baseline label for compare mode.")
    parser.add_argument("--cmd-b", default=None, help="Candidate command for compare mode.")
    parser.add_argument("--label-b", default="candidate", help="Candidate label for compare mode.")
    parser.add_argument("--repeat", type=int, default=5, help="Measured repetitions.")
    parser.add_argument("--warmup", type=int, default=1, help="Unmeasured warmup repetitions.")
    parser.add_argument("--output-dir", required=True, help="Benchmark output directory.")
    parser.add_argument("--timeout", type=float, default=None, help="Optional per-run timeout in seconds.")
    parser.add_argument("--cwd", default=None, help="Working directory for the benchmarked command.")
    parser.add_argument("--env", action="append", default=[], help="Environment override in KEY=VALUE form.")
    parser.add_argument("--require-otel", action="store_true", help="Fail samples that do not produce an OTel summary.")
    parser.add_argument("--artifact-glob", action="append", default=[], help="Extra artifact glob relative to the discovered run root or cwd.")
    parser.add_argument("--notes", default=None, help="Optional free-form notes stored in benchmark.json.")
    parser.add_argument("--keep-all-artifacts", action="store_true", help="Accepted for forward compatibility; child artifacts are currently always left in place.")
    return parser


def run_variant(
    *,
    benchmark: dict[str, Any],
    output_dir: Path,
    label: str,
    command: str,
    repeat: int,
    warmup: int,
    cwd: Path,
    env: dict[str, str],
    timeout: float | None,
    require_otel: bool,
    artifact_globs: list[str],
) -> dict[str, Any]:
    warmups: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    variant_dir = output_dir / label
    variant_dir.mkdir(parents=True, exist_ok=True)

    benchmark["variants"][label] = {
        "label": label,
        "command": command,
        "warmups": warmups,
        "samples": samples,
    }
    write_json(output_dir / "benchmark.json", benchmark)

    for idx in range(1, warmup + 1):
        sample = run_sample(
            sample_id=f"warmup_{idx:03d}",
            measured=False,
            command=command,
            sample_dir=variant_dir / f"warmup_{idx:03d}",
            cwd=cwd,
            env=env,
            timeout=timeout,
            require_otel=require_otel,
            artifact_globs=artifact_globs,
        )
        warmups.append(sample)
        benchmark["variants"][label]["warmups"] = warmups
        write_json(output_dir / "benchmark.json", benchmark)

    for idx in range(1, repeat + 1):
        sample = run_sample(
            sample_id=f"sample_{idx:03d}",
            measured=True,
            command=command,
            sample_dir=variant_dir / f"sample_{idx:03d}",
            cwd=cwd,
            env=env,
            timeout=timeout,
            require_otel=require_otel,
            artifact_globs=artifact_globs,
        )
        samples.append(sample)
        benchmark["variants"][label] = build_variant_summary(label, command, warmups, samples)
        write_json(output_dir / "benchmark.json", benchmark)

    return benchmark["variants"][label]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.repeat < 1:
        raise SystemExit("--repeat must be at least 1")
    if args.warmup < 0:
        raise SystemExit("--warmup must be non-negative")

    single_mode = bool(args.cmd)
    compare_mode = bool(args.cmd_a or args.cmd_b)
    if single_mode and compare_mode:
        raise SystemExit("Use either --cmd for single mode or --cmd-a/--cmd-b for compare mode, not both.")
    if not single_mode and not compare_mode:
        raise SystemExit("Provide --cmd for single mode or both --cmd-a and --cmd-b for compare mode.")
    if compare_mode and (not args.cmd_a or not args.cmd_b):
        raise SystemExit("Compare mode requires both --cmd-a and --cmd-b.")

    cwd = Path(args.cwd).expanduser().resolve() if args.cwd else Path.cwd()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(parse_env_overrides(args.env))
    env = augment_flux_pythonpath(env)

    benchmark: dict[str, Any] = {
        "schema_version": 1,
        "created_at": utcnow_iso(),
        "mode": "single" if single_mode else "compare",
        "repeat": args.repeat,
        "warmup": args.warmup,
        "notes": args.notes,
        "variants": {},
        "comparison": {},
    }
    write_json(output_dir / "benchmark.json", benchmark)

    if single_mode:
        variant_summary = run_variant(
            benchmark=benchmark,
            output_dir=output_dir,
            label=args.label,
            command=args.cmd,
            repeat=args.repeat,
            warmup=args.warmup,
            cwd=cwd,
            env=env,
            timeout=args.timeout,
            require_otel=args.require_otel,
            artifact_globs=args.artifact_glob,
        )
    else:
        variant_a = run_variant(
            benchmark=benchmark,
            output_dir=output_dir,
            label=args.label_a,
            command=args.cmd_a,
            repeat=args.repeat,
            warmup=args.warmup,
            cwd=cwd,
            env=env,
            timeout=args.timeout,
            require_otel=args.require_otel,
            artifact_globs=args.artifact_glob,
        )
        variant_b = run_variant(
            benchmark=benchmark,
            output_dir=output_dir,
            label=args.label_b,
            command=args.cmd_b,
            repeat=args.repeat,
            warmup=args.warmup,
            cwd=cwd,
            env=env,
            timeout=args.timeout,
            require_otel=args.require_otel,
            artifact_globs=args.artifact_glob,
        )
        benchmark["comparison"] = build_comparison(args.label_a, variant_a, args.label_b, variant_b)
        variant_summary = None

    write_json(output_dir / "benchmark.json", benchmark)
    write_report(output_dir / "report.md", benchmark)

    print(f"Benchmark JSON: {output_dir / 'benchmark.json'}")
    print(f"Benchmark report: {output_dir / 'report.md'}")
    if single_mode and variant_summary is not None:
        print(f"Successful measured samples: {variant_summary['success_count']}/{variant_summary['sample_count']}")
        median_s = variant_summary["stats"]["median_s"]
        if median_s is not None:
            print(f"Median wall time: {median_s:.6f}s")
        return 0 if variant_summary["failure_count"] == 0 else 1

    success_total = 0
    failure_total = 0
    for variant in benchmark["variants"].values():
        success_total += int(variant["success_count"])
        failure_total += int(variant["failure_count"])
    print(f"Successful measured samples: {success_total}/{success_total + failure_total}")
    delta_s = benchmark["comparison"].get("runtime", {}).get("delta_s")
    if delta_s is not None:
        print(f"Median delta ({args.label_b} vs {args.label_a}): {delta_s:.6f}s")
    return 0 if failure_total == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
