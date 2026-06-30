from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys


def _benchmark_script() -> Path:
    return Path(__file__).resolve().parents[2] / "site-wrappers" / "ff-benchmark.py"


def _load_benchmark_module():
    script = _benchmark_script()
    spec = importlib.util.spec_from_file_location("ff_benchmark_test_module", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_benchmark(
    tmp_path: Path,
    command: str,
    *,
    require_otel: bool = False,
    repeat: int = 2,
    warmup: int = 1,
) -> subprocess.CompletedProcess[str]:
    args = [
        sys.executable,
        str(_benchmark_script()),
        "--label",
        "demo",
        "--cmd",
        command,
        "--repeat",
        str(repeat),
        "--warmup",
        str(warmup),
        "--output-dir",
        str(tmp_path / "bench"),
    ]
    if require_otel:
        args.append("--require-otel")
    return subprocess.run(args, capture_output=True, text=True, check=False)


def _run_compare_benchmark(
    tmp_path: Path,
    command_a: str,
    command_b: str,
    *,
    require_otel: bool = False,
    repeat: int = 2,
    warmup: int = 0,
) -> subprocess.CompletedProcess[str]:
    args = [
        sys.executable,
        str(_benchmark_script()),
        "--label-a",
        "baseline",
        "--cmd-a",
        command_a,
        "--label-b",
        "candidate",
        "--cmd-b",
        command_b,
        "--repeat",
        str(repeat),
        "--warmup",
        str(warmup),
        "--output-dir",
        str(tmp_path / "bench"),
    ]
    if require_otel:
        args.append("--require-otel")
    return subprocess.run(args, capture_output=True, text=True, check=False)


def test_benchmark_utility_serial_artifacts(tmp_path: Path):
    fake_run = tmp_path / "fake_run.py"
    run_root = tmp_path / "serial-run"
    fake_run.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import csv, json, os",
                f"run_root = Path({str(run_root)!r})",
                "run_root.mkdir(parents=True, exist_ok=True)",
                "(run_root / 'output').mkdir(exist_ok=True)",
                "(run_root / 'generated.toml').write_text('', encoding='utf-8')",
                "(run_root / 'reproduce.sh').write_text('', encoding='utf-8')",
                "(run_root / 'summary.json').write_text(json.dumps({'state': 'succeeded', 'jobs_total': 3, 'jobs_completed': 3, 'makespan_seconds': 12.5}) + '\\n', encoding='utf-8')",
                "(run_root / 'status.json').write_text(json.dumps({'state': 'running', 'jobs_total': 3, 'jobs_completed': 3}) + '\\n', encoding='utf-8')",
                "with (run_root / 'otel_summary.csv').open('w', newline='', encoding='utf-8') as f:",
                "    writer = csv.DictWriter(f, fieldnames=['service','source','name','count','total_ms','avg_ms','max_ms','min_ms'])",
                "    writer.writeheader()",
                "    writer.writerow({'service':'svc','source':'engine','name':'simulation.advance.bucket','count':'2','total_ms':'20','avg_ms':'10','max_ms':'11','min_ms':'9'})",
                "print(f'Generated config: {run_root / \"generated.toml\"}')",
                "print(f'Output dir: {run_root / \"output\"}')",
                "print(f'OTel summary: {run_root / \"otel_summary.csv\"}')",
                "print(f'Reproducer: {run_root / \"reproduce.sh\"}')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    command = f"{shlex.quote(sys.executable)} {shlex.quote(str(fake_run))}"
    proc = _run_benchmark(tmp_path, command, require_otel=True)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    benchmark = json.loads((tmp_path / "bench" / "benchmark.json").read_text(encoding="utf-8"))
    variant = benchmark["variants"]["demo"]
    assert variant["sample_count"] == 2
    assert variant["success_count"] == 2
    assert variant["stats"]["median_s"] is not None
    assert variant["otel_span_stats"][0]["name"] == "simulation.advance.bucket"
    report = (tmp_path / "bench" / "report.md").read_text(encoding="utf-8")
    assert "Top OTel Spans" in report


def test_benchmark_utility_otel_jsonl_fallback(tmp_path: Path):
    fake_run = tmp_path / "fake_run_jsonl.py"
    run_root = tmp_path / "serial-run-jsonl"
    fake_run.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import json",
                f"run_root = Path({str(run_root)!r})",
                "run_root.mkdir(parents=True, exist_ok=True)",
                "(run_root / 'output').mkdir(exist_ok=True)",
                "(run_root / 'generated.toml').write_text('', encoding='utf-8')",
                "(run_root / 'reproduce.sh').write_text('', encoding='utf-8')",
                "(run_root / 'summary.json').write_text(json.dumps({'state': 'succeeded', 'jobs_total': 1, 'jobs_completed': 1, 'makespan_seconds': 5.0}) + '\\n', encoding='utf-8')",
                "(run_root / 'status.json').write_text(json.dumps({'state': 'succeeded', 'jobs_total': 1, 'jobs_completed': 1}) + '\\n', encoding='utf-8')",
                "(run_root / 'otel_spans.jsonl').write_text(json.dumps({'service':'svc','source':'engine','name':'simulation.query_quiescent','duration_ms':7.5}) + '\\n', encoding='utf-8')",
                "print(f'Generated config: {run_root / \"generated.toml\"}')",
                "print(f'OTel spans: {run_root / \"otel_spans.jsonl\"}')",
                "print(f'Reproducer: {run_root / \"reproduce.sh\"}')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    command = f"{shlex.quote(sys.executable)} {shlex.quote(str(fake_run))}"
    proc = _run_benchmark(tmp_path, command, require_otel=True)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    benchmark = json.loads((tmp_path / "bench" / "benchmark.json").read_text(encoding="utf-8"))
    span_stats = benchmark["variants"]["demo"]["otel_span_stats"]
    assert span_stats[0]["name"] == "simulation.query_quiescent"
    assert span_stats[0]["median_total_ms"] == 7.5


def test_benchmark_utility_parallel_artifacts(tmp_path: Path):
    fake_run = tmp_path / "fake_parallel.py"
    run_root = tmp_path / "parallel-run"
    fake_run.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import json",
                f"run_root = Path({str(run_root)!r})",
                "run_root.mkdir(parents=True, exist_ok=True)",
                "(run_root / 'parallel_summary.json').write_text(json.dumps({'state': 'succeeded', 'total_runs': 2, 'succeeded': 2, 'failed': 0, 'interrupted': 0, 'skipped': 0}) + '\\n', encoding='utf-8')",
                "(run_root / 'parallel_status.json').write_text(json.dumps({'state': 'running', 'total_runs': 2}) + '\\n', encoding='utf-8')",
                "print(f'Parallel summary: {run_root / \"parallel_summary.json\"}')",
                "print(f'Parallel status: {run_root / \"parallel_status.json\"}')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    command = f"{shlex.quote(sys.executable)} {shlex.quote(str(fake_run))}"
    proc = _run_benchmark(tmp_path, command)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    benchmark = json.loads((tmp_path / "bench" / "benchmark.json").read_text(encoding="utf-8"))
    sample = benchmark["variants"]["demo"]["samples"][0]
    assert sample["parallel_summary"]["total_runs"] == 2
    assert sample["parallel_summary"]["succeeded"] == 2


def test_benchmark_utility_compare_mode(tmp_path: Path):
    fake_run = tmp_path / "fake_compare.py"
    fake_run.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import csv, json, sys, time",
                "variant = sys.argv[1]",
                "run_root = Path(sys.argv[2])",
                "sleep_s = 0.01 if variant == 'baseline' else 0.03",
                "makespan = 10.0 if variant == 'baseline' else 12.0",
                "span_total = 5.0 if variant == 'baseline' else 9.0",
                "run_root.mkdir(parents=True, exist_ok=True)",
                "(run_root / 'output').mkdir(exist_ok=True)",
                "(run_root / 'generated.toml').write_text('', encoding='utf-8')",
                "(run_root / 'reproduce.sh').write_text('', encoding='utf-8')",
                "time.sleep(sleep_s)",
                "(run_root / 'summary.json').write_text(json.dumps({'state': 'succeeded', 'jobs_total': 2, 'jobs_completed': 2, 'makespan_seconds': makespan}) + '\\n', encoding='utf-8')",
                "(run_root / 'status.json').write_text(json.dumps({'state': 'succeeded', 'jobs_total': 2, 'jobs_completed': 2}) + '\\n', encoding='utf-8')",
                "with (run_root / 'otel_summary.csv').open('w', newline='', encoding='utf-8') as f:",
                "    writer = csv.DictWriter(f, fieldnames=['service','source','name','count','total_ms','avg_ms','max_ms','min_ms'])",
                "    writer.writeheader()",
                "    writer.writerow({'service':'svc','source':'engine','name':'simulation.advance.bucket','count':'2','total_ms':str(span_total),'avg_ms':str(span_total / 2),'max_ms':str(span_total / 2),'min_ms':str(span_total / 2)})",
                "print(f'Generated config: {run_root / \"generated.toml\"}')",
                "print(f'Output dir: {run_root / \"output\"}')",
                "print(f'OTel summary: {run_root / \"otel_summary.csv\"}')",
                "print(f'Reproducer: {run_root / \"reproduce.sh\"}')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    baseline_root = tmp_path / "baseline-run"
    candidate_root = tmp_path / "candidate-run"
    command_a = (
        f"{shlex.quote(sys.executable)} {shlex.quote(str(fake_run))} "
        f"baseline {shlex.quote(str(baseline_root))}"
    )
    command_b = (
        f"{shlex.quote(sys.executable)} {shlex.quote(str(fake_run))} "
        f"candidate {shlex.quote(str(candidate_root))}"
    )
    proc = _run_compare_benchmark(tmp_path, command_a, command_b, require_otel=True, repeat=1)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    benchmark = json.loads((tmp_path / "bench" / "benchmark.json").read_text(encoding="utf-8"))
    assert benchmark["mode"] == "compare"
    assert set(benchmark["variants"]) == {"baseline", "candidate"}
    runtime = benchmark["comparison"]["runtime"]
    assert runtime["baseline_label"] == "baseline"
    assert runtime["candidate_label"] == "candidate"
    assert runtime["delta_s"] is not None and runtime["delta_s"] > 0
    span_delta = benchmark["comparison"]["span_deltas"][0]
    assert span_delta["name"] == "simulation.advance.bucket"
    assert span_delta["delta_ms"] == 4.0
    warnings = benchmark["comparison"]["warnings"]
    assert any("makespan_seconds" in warning for warning in warnings)

    report = (tmp_path / "bench" / "report.md").read_text(encoding="utf-8")
    assert "## Comparison" in report
    assert "## Span Deltas" in report
    assert "## Comparison Warnings" in report
    assert "- Wall time: `" in report
    assert "Median wall time" not in report


def test_run_sample_timeout_bytes_are_decoded(tmp_path: Path, monkeypatch):
    benchmark = _load_benchmark_module()

    def _fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=kwargs.get("args", args[0] if args else ["bash", "-lc", "sleep 999"]),
            timeout=1.0,
            output=b"Generated config: /tmp/generated.toml\n",
            stderr=b"timed out\n",
        )

    monkeypatch.setattr(benchmark.subprocess, "run", _fake_run)

    sample = benchmark.run_sample(
        sample_id="sample_001",
        measured=True,
        command="sleep 999",
        sample_dir=tmp_path / "sample_001",
        cwd=tmp_path,
        env=os.environ.copy(),
        timeout=1.0,
        require_otel=False,
        artifact_globs=[],
    )

    assert sample["timed_out"] is True
    assert sample["success"] is False
    assert "Command timed out." in sample["warnings"]
    assert (tmp_path / "sample_001" / "stdout.log").read_text(encoding="utf-8") == "Generated config: /tmp/generated.toml\n"
    assert (tmp_path / "sample_001" / "stderr.log").read_text(encoding="utf-8") == "timed out\n"
