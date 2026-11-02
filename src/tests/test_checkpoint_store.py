from __future__ import annotations

import json
from pathlib import Path

import pytest

from flux_fiction.cli import checkpoint_store


def _state(generation: int) -> dict:
    return {
        "generation": generation,
        "sim": {"current_time": 100.0 * generation, "num_complete": 3 * generation},
        "next_event_time": 100.0 * generation + 5,
        "faketime_effective": 100.0 * generation + 0.25,
        "jobs_total": 10,
        "running_jobids": [1, 2],
        "pending_jobids": [3],
    }


def _stopped_run(tmp_path: Path, generation: int) -> dict:
    run = tmp_path / f"run{generation}"
    statedir = run / "statedir"
    statedir.mkdir(parents=True)
    (statedir / "content.sqlite").write_bytes(b"kvs" * 100)
    flux_json = run / "flux.json"
    flux_json.write_text('{"sched-fluxion-qmanager": {}}\n')
    config = run / "config.toml"
    config.write_text(
        "[flux_fiction]\n"
        f'job_traces = "/traces/t.csv"\nconfig_json = "{flux_json}"\nnnodes = 128\n'
    )
    state = run / "checkpoint_state.json"
    state.write_text(json.dumps(_state(generation)))
    return {"state_file": state, "statedir": statedir, "generated_config": config, "run_root": run}


def test_package_then_open_latest_generation(tmp_path: Path):
    store = tmp_path / "store"
    checkpoint_store.package_generation(store, **_stopped_run(tmp_path, 1))
    gen2 = checkpoint_store.package_generation(store, **_stopped_run(tmp_path, 2))

    opened = checkpoint_store.open_generation(store)
    assert opened.path == gen2.path == store / "gen-0002"
    assert opened.initial_fake_time == pytest.approx(200.25)
    assert opened.manifest["jobs_running"] == 2
    assert not list(store.glob(".*partial"))

    # The generation carries its own copy of the Flux config, and its config
    # points at that copy rather than at the stopped run's directory.
    config = opened.config_file.read_text()
    assert str(opened.path / "flux.json") in config
    assert (opened.path / "flux.json").exists()

    restored = checkpoint_store.extract_statedir(opened, tmp_path / "restored")
    assert (restored / "content.sqlite").read_bytes() == b"kvs" * 100


def test_open_rejects_incomplete_or_corrupt_generation(tmp_path: Path):
    store = tmp_path / "store"
    gen = checkpoint_store.package_generation(store, **_stopped_run(tmp_path, 1))

    with (gen.path / "statedir.tar").open("ab") as f:
        f.write(b"x")
    with pytest.raises(ValueError, match="checksum"):
        checkpoint_store.open_generation(gen.path)

    (gen.path / "MANIFEST.json").unlink()
    with pytest.raises(FileNotFoundError, match="MANIFEST"):
        checkpoint_store.open_generation(gen.path)


def test_generation_numbers_are_never_reused(tmp_path: Path):
    store = tmp_path / "store"
    checkpoint_store.package_generation(store, **_stopped_run(tmp_path, 1))
    again = _stopped_run(tmp_path / "again", 1)
    with pytest.raises(FileExistsError):
        checkpoint_store.package_generation(store, **again)
