from __future__ import annotations

import json
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


def _flux_python_paths() -> list[str]:
    flux_prefix = _workspace_root() / "container-installs" / "flux-core"
    candidates = [
        flux_prefix / "lib" / "flux" / "python3.12",
        flux_prefix / "lib" / "python3.12" / "site-packages",
        flux_prefix / "local" / "lib" / "python3.12" / "dist-packages",
    ]
    return [str(path) for path in candidates if path.exists()]


def _integration_env() -> dict[str, str]:
    env = os.environ.copy()
    paths = [str(_repo_root() / "src"), *_flux_python_paths()]
    current = env.get("PYTHONPATH")
    if current:
        paths.append(current)
    env["PYTHONPATH"] = ":".join(dict.fromkeys(paths))
    return env


def _run_in_private_broker(
    tmp_path: Path,
    python_source: str,
    *,
    load_jobtap: bool = False,
    timeout: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    if shutil.which("flux") is None:
        pytest.skip("flux executable not available")

    inner_py = tmp_path / "inner.py"
    inner_sh = tmp_path / "inner.sh"
    inner_py.write_text(python_source, encoding="utf-8")

    lines = ["#!/usr/bin/env bash", "set -euo pipefail", f"cd {_repo_root()}"]
    if load_jobtap:
        lines.append(f"source {_repo_root() / 'load_jobtap.sh'}")
    lines.append(f"{sys.executable} {inner_py}")
    inner_sh.write_text("\n".join(lines) + "\n", encoding="utf-8")
    inner_sh.chmod(0o755)

    return subprocess.run(
        ["flux", "start", "bash", str(inner_sh)],
        cwd=_repo_root(),
        env=_integration_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_real_flux_jobtap_rpc_is_missing_without_plugin(tmp_path: Path):
    proc = _run_in_private_broker(
        tmp_path,
        """
import json
import flux

try:
    flux.Flux().rpc(
        "job-manager.emu-jobtap.accumulate",
        payload=json.dumps({"submits": 1, "finishes": 0}),
    ).get()
except Exception as exc:
    print(repr(exc))
    raise SystemExit(0)

raise SystemExit("unexpected success calling emu-jobtap without plugin")
""".strip(),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "emu-jobtap" in (proc.stdout + proc.stderr)


def test_real_flux_jobtap_accumulate_works_after_plugin_load(tmp_path: Path):
    proc = _run_in_private_broker(
        tmp_path,
        """
import json
import flux

flux.Flux().rpc(
    "job-manager.emu-jobtap.accumulate",
    payload=json.dumps({"submits": 2, "finishes": 1}),
).get()
print("accumulate-ok")
""".strip(),
        load_jobtap=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "accumulate-ok" in proc.stdout


def test_real_flux_jobtap_quiescent_succeeds_with_default_scheduler_stack(tmp_path: Path):
    proc = _run_in_private_broker(
        tmp_path,
        """
import json
import flux

response = flux.Flux().rpc(
    "job-manager.emu-jobtap.quiescent",
    payload=json.dumps({"submits": 0, "finishes": 0}),
).get()
print(json.dumps(response, sort_keys=True))
""".strip(),
        load_jobtap=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload == {"submits": 0, "finishes": 0}


def test_real_flux_jobtap_round_trips_epoch_probe_payload(tmp_path: Path):
    proc = _run_in_private_broker(
        tmp_path,
        """
import json
import flux

handle = flux.Flux()
handle.rpc(
    "job-manager.emu-jobtap.accumulate",
    payload=json.dumps({
        "epoch": 7,
        "logical_time": 42.5,
        "expect": {"submits": 0, "finishes": 0},
    }),
).get()
probe = {
    "epoch": 7,
    "logical_time": 42.5,
    "expect": {"submits": 0, "finishes": 0},
}
response = handle.rpc(
    "job-manager.emu-jobtap.quiescent",
    payload=json.dumps(probe),
).get()
print(json.dumps(response, sort_keys=True))
""".strip(),
        load_jobtap=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["epoch"] == 7
    assert payload["logical_time"] == 42.5


def test_real_fluxion_unsatisfiable_jobspec_is_exposed_in_attempts(tmp_path: Path):
    config_json = _workspace_root() / "tmp" / "resource-match-profile-slot-350" / "config.json"
    if not config_json.exists():
        pytest.skip(f"Missing Fluxion config fixture: {config_json}")

    proc = _run_in_private_broker(
        tmp_path,
        f"""
import json
from types import SimpleNamespace

import flux

from flux_fiction._adapters.flux.adapter import FluxAdapter

cfg = SimpleNamespace(
    resource_R=None,
    resource_file=None,
    nnodes=1,
    ncpus=1,
    ngpus=0,
    config_json={str(config_json)!r},
)

adapter = FluxAdapter()
adapter._handle = flux.Flux()
adapter.install_resources(cfg)
adapter.reload_scheduler(cfg)

jobspec = {{
    "version": 1,
    "resources": [
        {{
            "type": "node",
            "count": 2,
            "with": [
                {{
                    "type": "slot",
                    "count": 1,
                    "label": "task",
                    "with": [{{"type": "core", "count": 1}}],
                }}
            ],
        }}
    ],
    "tasks": [{{"command": ["command", "200"], "slot": "task", "count": {{"per_slot": 1}}}}],
    "attributes": {{"system": {{"duration": 60.0}}}},
}}

result = adapter.check_jobspec_satisfiability(json.dumps(jobspec))
print(json.dumps(result, sort_keys=True))
raise SystemExit(0 if result.get("satisfiable") is None else 1)
""".strip(),
        timeout=90.0,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["satisfiable"] is None
    assert any(
        "Unsatisfiable request" in attempt.get("error", "")
        for attempt in payload["attempts"]
    )


def test_real_fluxion_satisfiability_remains_unknown_when_resource_module_removed(tmp_path: Path):
    config_json = _workspace_root() / "tmp" / "resource-match-profile-slot-350" / "config.json"
    if not config_json.exists():
        pytest.skip(f"Missing Fluxion config fixture: {config_json}")

    proc = _run_in_private_broker(
        tmp_path,
        f"""
import json
from types import SimpleNamespace

import flux

from flux_fiction._adapters.flux.adapter import FluxAdapter

cfg = SimpleNamespace(
    resource_R=None,
    resource_file=None,
    nnodes=1,
    ncpus=1,
    ngpus=0,
    config_json={str(config_json)!r},
)

handle = flux.Flux()
adapter = FluxAdapter()
adapter._handle = handle
adapter.install_resources(cfg)
adapter.reload_scheduler(cfg)
handle.rpc("module.remove", payload={{"name": "sched-fluxion-resource"}}).get()

jobspec = {{
    "version": 1,
    "resources": [
        {{
            "type": "node",
            "count": 2,
            "with": [
                {{
                    "type": "slot",
                    "count": 1,
                    "label": "task",
                    "with": [{{"type": "core", "count": 1}}],
                }}
            ],
        }}
    ],
    "tasks": [{{"command": ["command", "200"], "slot": "task", "count": {{"per_slot": 1}}}}],
    "attributes": {{"system": {{"duration": 60.0}}}},
}}

result = adapter.check_jobspec_satisfiability(json.dumps(jobspec))
print(json.dumps(result, sort_keys=True))
raise SystemExit(0 if result.get("satisfiable") is None else 1)
""".strip(),
        timeout=90.0,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["satisfiable"] is None
    assert payload["attempts"]
    assert any(
        attempt.get("method") == "sched-fluxion-resource.satisfiability"
        for attempt in payload["attempts"]
    )
