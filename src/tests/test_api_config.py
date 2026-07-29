from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pytest

from flux_fiction.api import config as api_config


def test_parse_path_map_entry_supports_both_separators():
    assert api_config._parse_path_map_entry("/a=/b") == ("/a", "/b")
    assert api_config._parse_path_map_entry("/a>/b") == ("/a", "/b")


def test_parse_path_map_entry_rejects_invalid_values():
    with pytest.raises(ValueError):
        api_config._parse_path_map_entry("/a")
    with pytest.raises(ValueError):
        api_config._parse_path_map_entry("=/b")


def test_rewrite_path_uses_env_mappings_and_output_mode(tmp_path: Path, monkeypatch):
    host_root = tmp_path / "host"
    mapped_root = tmp_path / "mapped"
    host_root.mkdir()
    mapped_root.mkdir()
    mapped_file = mapped_root / "trace.csv"
    mapped_file.write_text("data\n", encoding="utf-8")

    monkeypatch.setenv("FLUX_FICTION_PATH_MAP", f"{host_root}={mapped_root}")

    assert api_config._rewrite_path("relative/path") == "relative/path"
    assert api_config._rewrite_path(str(host_root / "trace.csv")) == str(mapped_file)
    assert api_config._rewrite_path(str(host_root / "out"), for_output=True) == str(mapped_root / "out")


def test_normalize_config_paths_only_rewrites_string_fields(tmp_path: Path, monkeypatch):
    host_root = tmp_path / "host"
    mapped_root = tmp_path / "mapped"
    host_root.mkdir()
    mapped_root.mkdir()
    mapped_file = mapped_root / "trace.csv"
    mapped_file.write_text("data\n", encoding="utf-8")
    monkeypatch.setenv("FLUX_FICTION_PATH_MAP", f"{host_root}={mapped_root}")

    data = {
        "job_traces": str(host_root / "trace.csv"),
        "nnodes": 2,
        "status_file": str(host_root / "status.json"),
    }
    normalized = api_config._normalize_config_paths(data)

    assert normalized["job_traces"] == str(mapped_file)
    assert normalized["status_file"] == str(mapped_root / "status.json")
    assert normalized["nnodes"] == 2


def test_to_dataclass_validates_and_normalizes_output_dir():
    cfg = api_config._to_dataclass(
        {
            "job_traces": "/tmp/trace.csv",
            "output_dir": "/tmp/out",
            "backend": "mock",
        }
    )

    assert cfg.output_dir == "/tmp/out/"
    assert cfg.backend == "mock"

    with pytest.raises(ValueError):
        api_config._to_dataclass({"job_traces": "/tmp/trace.csv", "backend": "bad"})

    with pytest.raises(ValueError):
        api_config._to_dataclass(
            {
                "job_traces": "/tmp/trace.csv",
                "resource_file": "/tmp/a",
                "resource_R": "/tmp/b",
            }
        )


def test_load_toml_and_from_toml_support_top_level_and_overrides(tmp_path: Path):
    trace_path = tmp_path / "trace.csv"
    trace_path.write_text("JobID\n", encoding="utf-8")
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[flux_fiction]",
                f"job_traces = {json.dumps(str(trace_path))}",
                "backend = \"mock\"",
                "output_dir = \"./output\"",
                "jobtap_logging = false",
                "submit_novalidate = true",
                "quiescent_accumulation_window = 0.25",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    args = argparse.Namespace(
        config_file=str(config_path),
        job_traces=None,
        faketime_timestamp_file="/tmp/stamp",
        faketime_initial_epoch=None,
        faketime_seed=None,
        faketime_tolerance=None,
        faketime_near_event_threshold=None,
        quiescent_accumulation_window=None,
        account_system_latency=False,
        jobtap_logging=True,
        submit_novalidate=False,
        otel_enabled=None,
        otel_endpoint=None,
        otel_service_name=None,
        otel_bridge_socket=None,
        otel_summary_file=None,
        otel_spans_file=None,
        otel_bridge_log_file=None,
    )

    cfg = api_config.from_toml(args)

    assert cfg.config_file == str(config_path)
    assert cfg.job_traces == str(trace_path)
    assert cfg.jobtap_logging is True
    assert cfg.submit_novalidate is False
    assert cfg.account_system_latency is False
    assert cfg.faketime_timestamp_file == "/tmp/stamp"
    assert cfg.quiescent_accumulation_window == 0.25
    assert cfg.output_dir.endswith("/")


def test_from_toml_rejects_missing_or_invalid_payloads(tmp_path: Path):
    missing = argparse.Namespace(config_file=str(tmp_path / "missing.toml"))
    with pytest.raises(FileNotFoundError):
        api_config.from_toml(missing)

    bad_shape = tmp_path / "bad.toml"
    bad_shape.write_text("flux_fiction = 3\n", encoding="utf-8")
    with pytest.raises(ValueError, match="table"):
        api_config.from_toml({"config_file": str(bad_shape)})

    no_cfg = argparse.Namespace(config_file=None)
    with pytest.raises(ValueError, match="missing"):
        api_config.from_toml(no_cfg)


def test_from_cli_args_uses_direct_values_without_toml():
    args = argparse.Namespace(
        job_traces="/tmp/trace.csv",
        config_file=None,
        source_config_file=None,
        config_json=None,
        raw_jobspec_file=None,
        resource_file=None,
        resource_R=None,
        nnodes=1,
        nsockets=1,
        ncpus=1,
        ngpus=0,
        log_level=10,
        log_file=None,
        status_file=None,
        summary_file=None,
        exclusive=False,
        quiet=False,
        backend="mock",
        batch_job_starts=True,
        account_system_latency=True,
        jobtap_logging=False,
        submit_novalidate=True,
        rabbit_storage_emit_dw=False,
        rabbit_storage_name="rabbit",
        output_dir="/tmp/out",
        faketime_timestamp_file=None,
        faketime_initial_epoch=0.0,
        faketime_seed=True,
        faketime_tolerance=1e-6,
        faketime_near_event_threshold=0.0,
        quiescent_accumulation_window=0.0,
        otel_enabled=False,
        otel_endpoint="http://127.0.0.1:4318/v1/traces",
        otel_service_name="flux-fiction",
        otel_bridge_socket=None,
        otel_summary_file=None,
        otel_spans_file=None,
        otel_bridge_log_file=None,
    )

    cfg = api_config.from_cli_args(args)
    assert cfg.backend == "mock"
    assert cfg.submit_novalidate is True
    assert cfg.output_dir == "/tmp/out/"


def test_setup_logging_replaces_handlers_and_supports_quiet(tmp_path: Path, capsys):
    log_file = tmp_path / "app.log"
    root = logging.getLogger()
    old = logging.StreamHandler()
    root.addHandler(old)

    api_config.setup_logging(level=logging.INFO, log_file=str(log_file), quiet=True)
    logging.getLogger("flux_fiction.test").info("quiet-file-only")

    assert old not in logging.getLogger().handlers
    assert "quiet-file-only" in log_file.read_text(encoding="utf-8")

    api_config.setup_logging(level=logging.INFO, quiet=False)
    logging.getLogger("flux_fiction.test").info("console-visible")
    captured = capsys.readouterr()
    assert "console-visible" in captured.err
