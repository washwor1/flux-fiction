from __future__ import annotations

from pathlib import Path
import tempfile

import pytest

from flux_fiction.cli import run_ff
from flux_fiction.faketime_paths import default_stampfile_path


def test_create_run_root_creates_unique_auto_directories(tmp_path, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    monkeypatch.setattr(run_ff, "make_run_id", lambda _config, _tag: "fixed_run")

    first = run_ff.create_run_root(config, None, None)
    second = run_ff.create_run_root(config, None, None)

    assert first == config.parent / "runs" / "fixed_run"
    assert second == config.parent / "runs" / "fixed_run_2"
    assert first.is_dir()
    assert second.is_dir()


def test_create_run_root_uses_explicit_run_dir_once(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[flux_fiction]\njob_traces = \"trace.csv\"\n", encoding="ascii")
    explicit = tmp_path / "my-run"

    created = run_ff.create_run_root(config, explicit, None)

    assert created == explicit
    assert explicit.is_dir()

    with pytest.raises(FileExistsError):
        run_ff.create_run_root(config, explicit, None)


def test_resolve_stampfile_path_defaults_to_container_local_tmp(tmp_path, monkeypatch):
    run_root = tmp_path / "run-root"
    monkeypatch.delenv("STAMPFILE", raising=False)

    stamp, source = run_ff.resolve_stampfile_path(None, run_root)

    assert stamp == default_stampfile_path(run_root)
    assert Path(tempfile.gettempdir()) in stamp.parents
    assert source == "default"


def test_resolve_stampfile_path_respects_explicit_override(tmp_path, monkeypatch):
    run_root = tmp_path / "run-root"
    explicit = tmp_path / "custom" / "stamp"
    monkeypatch.setenv("STAMPFILE", str(tmp_path / "env-stamp"))

    stamp, source = run_ff.resolve_stampfile_path(str(explicit), run_root)

    assert stamp == explicit
    assert source == "explicit"


def test_resolve_stampfile_path_respects_environment_override(tmp_path, monkeypatch):
    run_root = tmp_path / "run-root"
    env_stamp = tmp_path / "env-stamp"
    monkeypatch.setenv("STAMPFILE", str(env_stamp))

    stamp, source = run_ff.resolve_stampfile_path(None, run_root)

    assert stamp == env_stamp
    assert source == "environment"


def test_warn_on_implicit_stampfile_for_environment(tmp_path, capsys):
    stamp = tmp_path / "env-stamp"

    run_ff.warn_on_implicit_stampfile(stamp, "environment")

    captured = capsys.readouterr()
    assert "WARNING:" in captured.err
    assert "STAMPFILE" in captured.err
    assert str(stamp) in captured.err


def test_warn_on_implicit_stampfile_for_default(tmp_path, capsys):
    stamp = default_stampfile_path(tmp_path / "run-root")

    run_ff.warn_on_implicit_stampfile(stamp, "default")

    captured = capsys.readouterr()
    assert "WARNING:" in captured.err
    assert "default container-local faketime stamp file" in captured.err
    assert str(stamp) in captured.err
