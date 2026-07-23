from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import re

import pytest

from flux_fiction.faketime_paths import default_stampfile_path
from flux_fiction.cli import run_ff_parallel
from flux_fiction.parallel import ParallelValidationError, load_parallel_manifest, resolve_parallel_plan


def _write_manifest(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_load_parallel_manifest_resolves_relative_paths(tmp_path):
    config = tmp_path / "config.toml"
    trace = tmp_path / "trace.csv"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    trace.write_text("Submit,Elapsed,Timelimit,NNodes,NCPUS\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 2
output_root = "./parallel-out"

[[run]]
name = "alpha"
config_file = "./config.toml"
job_traces = "./trace.csv"
""".strip()
        + "\n",
    )

    manifest = load_parallel_manifest(manifest_path)

    assert manifest.parallel.output_root == str((tmp_path / "parallel-out").resolve())
    assert manifest.run[0].config_file == str(config.resolve())
    assert manifest.run[0].job_traces == str(trace.resolve())


def test_load_parallel_manifest_rejects_duplicate_names(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "dup"
config_file = "./config.toml"

[[run]]
name = "dup"
config_file = "./config.toml"
""".strip()
        + "\n",
    )

    with pytest.raises(ParallelValidationError, match="Run names must be unique"):
        load_parallel_manifest(manifest_path)


def test_load_parallel_manifest_rejects_missing_paths(tmp_path):
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "missing"
config_file = "./does-not-exist.toml"
""".strip()
        + "\n",
    )

    with pytest.raises(ParallelValidationError, match="Referenced path does not exist"):
        load_parallel_manifest(manifest_path)


def test_resolve_parallel_plan_applies_defaults_and_paths(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 3
default_no_faketime = true
default_broker_log_level = 9
output_root = "./planned-output"

[[run]]
name = "alpha run"
config_file = "./config.toml"
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)

    plan = resolve_parallel_plan(manifest)

    assert plan.max_concurrent == 3
    assert len(plan.runs) == 1
    assert re.search(r"/planned-output/\d{8}_\d{6}_manifest(?:_\d+)?$", plan.output_root)
    run = plan.runs[0]
    run_root = Path(run.run_root)
    assert run.no_faketime is True
    assert run.broker_log_level == 9
    assert run_root.parent == Path(plan.output_root) / "runs"
    assert run_root.name == "0001_alpha-run"
    assert run.child_run_dir == str(run_root / "child")
    assert run.stampfile == str(default_stampfile_path(run_root / "child"))


def test_resolve_parallel_plan_supports_sharding(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 2

[[run]]
name = "run-a"
config_file = "./config.toml"

[[run]]
name = "run-b"
config_file = "./config.toml"

[[run]]
name = "run-c"
config_file = "./config.toml"
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)

    plan = resolve_parallel_plan(manifest, shard_index=1, shard_count=2)

    assert [run.name for run in plan.runs] == ["run-b"]
    assert "shard-2-of-2" in plan.output_root


def test_resolve_parallel_plan_avoids_existing_execution_root(tmp_path, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1
output_root = "./parallel-out"

[[run]]
name = "alpha"
config_file = "./config.toml"
""".strip()
        + "\n",
    )
    manifest = load_parallel_manifest(manifest_path)
    monkeypatch.setattr(
        "flux_fiction.parallel.config._make_parallel_run_id",
        lambda *_args, **_kwargs: "20260621_120000_manifest",
    )

    first = resolve_parallel_plan(manifest)
    Path(first.output_root).mkdir(parents=True)
    second = resolve_parallel_plan(manifest)

    assert first.output_root.endswith("/parallel-out/20260621_120000_manifest")
    assert second.output_root.endswith("/parallel-out/20260621_120000_manifest_2")


def test_load_parallel_manifest_accepts_serial_config_overrides(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "alpha"
config_file = "./config.toml"
account_system_latency = false
backend = "mock"
quiet = true
rabbit_storage_emit_dw = false
rabbit_storage_name = "rabbitxfs"
""".strip()
        + "\n",
    )

    manifest = load_parallel_manifest(manifest_path)

    overrides = manifest.run[0].config_overrides
    assert overrides["account_system_latency"] is False
    assert overrides["backend"] == "mock"
    assert overrides["quiet"] is True
    assert overrides["rabbit_storage_emit_dw"] is False
    assert overrides["rabbit_storage_name"] == "rabbitxfs"


def test_load_parallel_manifest_rejects_unknown_run_override_field(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "alpha"
config_file = "./config.toml"
definitely_not_a_real_field = 123
""".strip()
        + "\n",
    )

    with pytest.raises(ParallelValidationError, match="Unsupported run override field"):
        load_parallel_manifest(manifest_path)


def test_run_ff_parallel_dry_run_prints_plan(tmp_path, capsys):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "alpha"
config_file = "./config.toml"
""".strip()
        + "\n",
    )

    rc = run_ff_parallel.main([str(manifest_path), "--dry-run"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "Planned runs:      1" in captured.out
    assert "Dry run requested; not launching child runs." in captured.out


def test_run_ff_parallel_executes_runner_and_prints_result_paths(tmp_path, capsys, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    manifest_path = _write_manifest(
        tmp_path / "manifest.toml",
        """
version = 1

[parallel]
max_concurrent = 1

[[run]]
name = "alpha"
config_file = "./config.toml"
""".strip()
        + "\n",
    )

    fake_result = SimpleNamespace(
        return_code=0,
        status_path=tmp_path / "parallel_status.json",
        summary_path=tmp_path / "parallel_summary.json",
        manifest_snapshot_path=tmp_path / "manifest.snapshot.toml",
    )
    monkeypatch.setattr(run_ff_parallel, "run_parallel_plan", lambda plan, **kwargs: fake_result)

    rc = run_ff_parallel.main([str(manifest_path)])

    captured = capsys.readouterr()
    assert rc == 0
    assert "Parallel status:" in captured.out
    assert "Parallel summary:" in captured.out
    assert "Manifest snapshot:" in captured.out
