from __future__ import annotations

import csv
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


def _workspace_root() -> Path:
    root = Path("/workspace")
    if root.exists():
        return root
    return Path(__file__).resolve().parents[4]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _write_condensed_trace(source_trace: Path, dest_trace: Path, n_jobs: int = 100) -> None:
    with source_trace.open(encoding="utf-8") as src, dest_trace.open("w", encoding="utf-8") as dst:
        for idx, line in enumerate(src):
            if idx == 0 or idx <= n_jobs:
                dst.write(line)
            else:
                break


def _run_harness(config_path: Path, run_dir: Path, *, no_faketime: bool) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    python_paths = [str(_repo_root() / "src")]
    flux_prefix = _workspace_root() / "container-installs" / "flux-core"
    for candidate in (
        flux_prefix / "lib" / "flux" / "python3.12",
        flux_prefix / "lib" / "python3.12" / "site-packages",
        flux_prefix / "local" / "lib" / "python3.12" / "dist-packages",
    ):
        if candidate.exists():
            python_paths.append(str(candidate))
    current = env.get("PYTHONPATH")
    if current:
        python_paths.append(current)
    env["PYTHONPATH"] = ":".join(dict.fromkeys(python_paths))

    cmd = [
        sys.executable,
        "-m",
        "flux_fiction.cli.run_ff",
        str(config_path),
        "--run-dir",
        str(run_dir),
    ]
    if no_faketime:
        cmd.append("--no-faketime")

    return subprocess.run(
        cmd,
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        timeout=240,
    )


def _max_active_jobs(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return max(int(row["active_jobs"]) for row in rows)


def _first_start_times(path: Path, n: int = 10) -> list[float]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return [float(row["START"]) for row in rows[:n]]


def test_condensed_rabbit_trace_matches_with_and_without_faketime(tmp_path: Path):
    if shutil.which("flux") is None:
        pytest.skip("flux executable not available")

    workspace = _workspace_root()
    source_trace = workspace / "tmp" / "ff-rabbit-1500-topslot-100-trace.csv"
    resource_r = workspace / "migrated_data" / "resource_graphs" / "tuolumne.json"
    config_json = workspace / "tmp" / "resource-match-profile-slot-350" / "config.json"

    missing = [str(path) for path in (source_trace, resource_r, config_json) if not path.exists()]
    if missing:
        pytest.skip("Missing rabbit regression fixtures: {}".format(", ".join(missing)))

    condensed_trace = tmp_path / "ff-rabbit-100-trace.csv"
    _write_condensed_trace(source_trace, condensed_trace, n_jobs=100)

    config_path = tmp_path / "rabbit-regression.toml"
    config_path.write_text(
        (
            "[flux_fiction]\n"
            f"job_traces = \"{condensed_trace}\"\n"
            f"resource_R = \"{resource_r}\"\n"
            f"config_json = \"{config_json}\"\n"
            "ncpus = 96\n"
            "ngpus = 4\n"
            "backend = \"flux\"\n"
            "quiet = true\n"
            "batch_job_starts = false\n"
            "account_system_latency = true\n"
            "jobtap_logging = false\n"
            "rabbit_storage_emit_dw = false\n"
            "rabbit_storage_name = \"rabbitxfs\"\n"
        ),
        encoding="utf-8",
    )

    no_fake_dir = tmp_path / "run-no-faketime"
    fake_dir = tmp_path / "run-faketime"

    no_fake = _run_harness(config_path, no_fake_dir, no_faketime=True)
    assert no_fake.returncode == 0, (
        "no-faketime harness run failed\nSTDOUT:\n{}\nSTDERR:\n{}"
    ).format(no_fake.stdout, no_fake.stderr)

    fake = _run_harness(config_path, fake_dir, no_faketime=False)
    assert fake.returncode == 0, (
        "faketime harness run failed\nSTDOUT:\n{}\nSTDERR:\n{}"
    ).format(fake.stdout, fake.stderr)

    no_fake_usage = no_fake_dir / "output" / "resource_usage_timeseries.csv"
    fake_usage = fake_dir / "output" / "resource_usage_timeseries.csv"
    no_fake_transitions = no_fake_dir / "output" / "job_transitions.csv"
    fake_transitions = fake_dir / "output" / "job_transitions.csv"

    assert no_fake_usage.exists()
    assert fake_usage.exists()
    assert no_fake_transitions.exists()
    assert fake_transitions.exists()

    no_fake_max_active = _max_active_jobs(no_fake_usage)
    fake_max_active = _max_active_jobs(fake_usage)
    no_fake_starts = _first_start_times(no_fake_transitions)
    fake_starts = _first_start_times(fake_transitions)

    assert fake_max_active == no_fake_max_active
    assert fake_starts == no_fake_starts
