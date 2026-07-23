from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from flux_fiction.cli import run_ff


def test_path_mappings_and_remap_path_with_env(tmp_path: Path, monkeypatch):
    src = tmp_path / "host"
    dst = tmp_path / "container"
    src.mkdir()
    dst.mkdir()
    (dst / "config.toml").write_text("x\n", encoding="utf-8")
    monkeypatch.setenv("FLUX_FICTION_PATH_MAP", f"{src}={dst}")

    mappings = run_ff.path_mappings()
    assert (src, dst) in mappings
    assert run_ff.remap_path(src / "config.toml") == dst / "config.toml"
    assert run_ff.remap_path(src / "out", for_output=True) == dst / "out"


def test_path_mappings_reject_invalid_env_entry(monkeypatch):
    monkeypatch.setenv("FLUX_FICTION_PATH_MAP", "/bad")
    with pytest.raises(ValueError):
        run_ff.path_mappings()


def test_write_flux_fiction_toml_and_load_toml(tmp_path: Path):
    path = tmp_path / "cfg.toml"
    run_ff.write_flux_fiction_toml(
        path,
        {"job_traces": "/tmp/trace.csv", "backend": "mock", "quiet": True, "none": None},
    )

    loaded = run_ff.load_toml(path)
    assert loaded["flux_fiction"]["backend"] == "mock"
    assert loaded["flux_fiction"]["quiet"] is True
    assert loaded["flux_fiction"]["none"] == ""


def test_drop_faketime_env_and_broker_log_matches(tmp_path: Path):
    env = {
        "LD_PRELOAD": "x",
        "FAKETIME": "@2026-04-01 00:00:00",
        "FAKETIME_ONE": "1",
        "STAMPFILE": "/tmp/stamp",
        "KEEP": "ok",
    }
    cleaned = run_ff.drop_faketime_env(env)
    assert cleaned == {"KEEP": "ok"}

    log_path = tmp_path / "broker.log"
    log_path.write_text("one\nsched-fluxion-resource.err bad\nother\n", encoding="utf-8")
    matches = run_ff.broker_log_matches(log_path, "sched-fluxion-resource.err")
    assert matches == ["2: sched-fluxion-resource.err bad"]
    assert run_ff.broker_log_matches(tmp_path / "missing.log", "x") == []


def test_first_submit_epoch_uses_t_submit_and_submit(tmp_path: Path):
    trace_a = tmp_path / "a.csv"
    trace_a.write_text("JobID,t_submit,Elapsed\n1,12.5,1\n", encoding="utf-8")
    assert run_ff.first_submit_epoch(trace_a) == 12.5

    trace_b = tmp_path / "b.csv"
    trace_b.write_text("JobID,Submit,Elapsed\n1,1970-01-01T00:00:05,1\n", encoding="utf-8")
    assert run_ff.first_submit_epoch(trace_b) == 5.0

    trace_c = tmp_path / "c.csv"
    trace_c.write_text("JobID,Elapsed\n1,1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Cannot find Submit"):
        run_ff.first_submit_epoch(trace_c)

    trace_d = tmp_path / "d.csv"
    trace_d.write_text("JobID,Submit\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        run_ff.first_submit_epoch(trace_d)


def test_configure_flux_env_uses_existing_prefix_and_default(monkeypatch, tmp_path: Path):
    prefix = tmp_path / "flux"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "lib").mkdir()
    (prefix / "lib64").mkdir()
    ((prefix / "lib") / "pkgconfig").mkdir()
    ((prefix / "lib64") / "pkgconfig").mkdir()

    env = {"PATH": "/usr/bin", "LD_LIBRARY_PATH": "", "PKG_CONFIG_PATH": "", "FLUX_PREFIX": str(prefix)}
    run_ff.configure_flux_env(env)
    assert env["FLUX_PREFIX"] == str(prefix)
    assert str(prefix / "bin") in env["PATH"]

    env2 = {}
    monkeypatch.delenv("FLUX_PREFIX", raising=False)
    monkeypatch.setattr(run_ff, "default_flux_prefix", lambda: prefix)
    run_ff.configure_flux_env(env2)
    assert env2["FLUX_PREFIX"] == str(prefix)


def test_prepare_config_copies_json_and_rewrites_paths(tmp_path: Path, monkeypatch):
    src_dir = tmp_path / "src"
    mapped_dir = tmp_path / "mapped"
    src_dir.mkdir()
    mapped_dir.mkdir()
    trace = mapped_dir / "trace.csv"
    trace.write_text("JobID\n", encoding="utf-8")
    res = mapped_dir / "resource.json"
    res.write_text("{}\n", encoding="utf-8")
    cfg_json = mapped_dir / "flux.json"
    cfg_json.write_text("{}\n", encoding="utf-8")

    monkeypatch.setenv("FLUX_FICTION_PATH_MAP", f"{src_dir}={mapped_dir}")
    source_config = tmp_path / "input.toml"
    run_ff.write_flux_fiction_toml(
        source_config,
        {
            "job_traces": str(src_dir / "trace.csv"),
            "resource_file": str(src_dir / "resource.json"),
            "config_json": str(src_dir / "flux.json"),
            "backend": "mock",
        },
    )

    generated, trace_path, output_dir = run_ff.prepare_config(source_config, tmp_path / "run")
    payload = run_ff.load_toml(generated)["flux_fiction"]

    assert trace_path == trace
    assert Path(payload["config_json"]).exists()
    assert payload["output_dir"].endswith("/output/")
    assert Path(output_dir).is_dir()


def test_prepare_config_rejects_missing_mapped_inputs(tmp_path: Path):
    source_config = tmp_path / "input.toml"
    run_ff.write_flux_fiction_toml(
        source_config,
        {"job_traces": str(tmp_path / "missing.csv"), "backend": "mock"},
    )
    with pytest.raises(FileNotFoundError, match="job_traces"):
        run_ff.prepare_config(source_config, tmp_path / "run")


def test_command_builders_and_dependency_checks(tmp_path: Path, monkeypatch):
    generated = tmp_path / "gen.toml"
    generated.write_text("", encoding="utf-8")
    api_cmd = run_ff.build_api_child_command(generated, "/tmp/stamp")
    assert "--faketime_no_seed" in api_cmd
    assert run_ff.build_diagnostics_command("emergency-dump", jobs_path=tmp_path / "jobs.json")[4:] == [
        "--jobs-path",
        str(tmp_path / "jobs.json"),
    ]

    bridge_cmd = run_ff.build_bridge_command(
        ff_root=tmp_path,
        socket_path=tmp_path / "sock",
        endpoint="http://collector",
        service_name="svc",
        summary_file=tmp_path / "summary.csv",
        spans_file=tmp_path / "spans.jsonl",
        log_file=tmp_path / "bridge.log",
    )
    assert bridge_cmd[0] == os.sys.executable
    assert "--socket" in bridge_cmd

    monkeypatch.setattr(run_ff.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(RuntimeError, match="OpenTelemetry"):
        run_ff.ensure_otel_dependencies()


def test_build_inner_script_and_write_reproducer(tmp_path: Path):
    script = run_ff.build_inner_script(tmp_path, tmp_path / "run" / "cfg.toml", "/tmp/stamp")
    assert "source ./load_jobtap.sh" in script
    assert "capture-sample-jobspec" in script
    assert "faketime_tolerance" in script

    repro = tmp_path / "reproduce.sh"
    run_ff.write_reproducer(
        repro,
        ["flux", "start"],
        {"X": "1"},
        extra_lines=["echo hi"],
    )
    text = repro.read_text(encoding="utf-8")
    assert "export X='1'" in text or "export X=1" in text
    assert "echo hi" in text


def test_make_otel_socket_path_is_stable_and_unique(tmp_path: Path):
    first = run_ff.make_otel_socket_path(tmp_path / "a")
    second = run_ff.make_otel_socket_path(tmp_path / "a")
    third = run_ff.make_otel_socket_path(tmp_path / "b")
    assert first == second
    assert first != third
    assert first.name.startswith("flux-fiction-otel-")
