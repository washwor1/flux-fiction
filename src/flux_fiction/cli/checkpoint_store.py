"""
On-disk checkpoint generations for flux-fiction-run.

A generation is a directory holding everything needed to resume a run on a
different node in a later allocation:

    <checkpoint_dir>/gen-0001/
        MANIFEST.json   written last; a generation without it is incomplete
        state.json      simulator state (flux_fiction._core.checkpoint)
        config.toml     the run's generated config, paths rewritten to here
        <config_json>   the Flux config the run loaded
        statedir.tar    the Flux instance's statedir (content.sqlite) after a
                        clean shutdown with job cleanup disabled
    <checkpoint_dir>/LATEST   name of the newest complete generation

The instance itself runs with its statedir on node-local storage, which is
only copied here once the broker has exited.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any

from flux_fiction.api.status import utcnow_iso

MANIFEST_FORMAT = "flux-fiction-checkpoint-generation"
MANIFEST_VERSION = 1

# Broker config for an instance restarted from a checkpointed statedir. rc1
# must not bring up anything that would act on the recovered jobs before the
# simulator takes over: job-exec would fail every reattach (it has no record of
# the simulated jobs) and raise fatal exceptions on them, and the recovered
# 128-node resource.R must not be verified against this one real node, which
# would drain rank 0 permanently in the resource eventlog. The scheduler is kept
# out separately with FLUX_SCHED_MODULE=none.
RESTART_BROKER_CONFIG = """\
[resource]
noverify = true

[modules.job-exec]
disabled = true
"""

# Environment for the restarted broker, on top of the normal run environment.
RESTART_BROKER_ENV = {
    "FLUX_SCHED_MODULE": "none",
}

# Environment for any broker whose statedir may be checkpointed: rc1's default
# cleanup would wait for the queue to idle and then cancel every running job.
CHECKPOINT_BROKER_ENV = {
    "FLUX_DISABLE_JOB_CLEANUP": "1",
}


@dataclass(frozen=True)
class Generation:
    path: Path
    manifest: dict[str, Any]

    @property
    def state_file(self) -> Path:
        return self.path / self.manifest["state"]

    @property
    def config_file(self) -> Path:
        return self.path / self.manifest["config"]

    @property
    def statedir_tar(self) -> Path:
        return self.path / self.manifest["statedir"]

    @property
    def initial_fake_time(self) -> float:
        """Fake wall clock the restarted instance resumes at."""
        value = self.manifest.get("faketime_effective")
        if value is None:
            value = self.manifest["sim_time"]
        return float(value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore
    with path.open("rb") as f:
        return tomllib.load(f)


def make_local_statedir(prefix: str = "ff-statedir-") -> Path:
    root = os.environ.get("TMPDIR") or "/tmp"
    return Path(tempfile.mkdtemp(prefix=prefix, dir=root))


def write_restart_broker_config(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "flux-fiction-restart.toml").write_text(RESTART_BROKER_CONFIG, encoding="utf-8")
    return directory


def _tar_create(source_dir: Path, tarball: Path) -> None:
    # tar rather than cp/shutil: bulk copies onto the setgid NFS prefixes fail
    # with spurious ENOENT, and tar reports real errors on stderr.
    subprocess.run(
        ["tar", "-C", str(source_dir), "-cf", str(tarball), "."],
        check=True,
    )


def _tar_extract(tarball: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["tar", "-C", str(dest_dir), "-xf", str(tarball)], check=True)


def package_generation(
    checkpoint_dir: Path,
    *,
    state_file: Path,
    statedir: Path,
    generated_config: Path,
    run_root: Path,
) -> Generation:
    """Copy a stopped run into a new, complete generation directory."""
    state = json.loads(state_file.read_text(encoding="utf-8"))
    generation = int(state["generation"])
    name = f"gen-{generation:04d}"
    final = checkpoint_dir / name
    if final.exists():
        raise FileExistsError(f"checkpoint generation already exists: {final}")
    work = checkpoint_dir / f".{name}.partial"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    shutil.copyfile(state_file, work / "state.json")

    cfg_doc = _load_toml(generated_config)
    cfg = dict(cfg_doc.get("flux_fiction", cfg_doc))
    config_json_name = None
    if cfg.get("config_json"):
        source_json = Path(cfg["config_json"])
        config_json_name = source_json.name
        shutil.copyfile(source_json, work / config_json_name)
        cfg["config_json"] = str(final / config_json_name)
    lines = ["[flux_fiction]"]
    for key, value in cfg.items():
        lines.append(f"{key} = {json.dumps(value)}")
    (work / "config.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")

    _tar_create(statedir, work / "statedir.tar")

    manifest = {
        "format": MANIFEST_FORMAT,
        "version": MANIFEST_VERSION,
        "generation": generation,
        "created_at": utcnow_iso(),
        "source_run_dir": str(run_root),
        "state": "state.json",
        "config": "config.toml",
        "config_json": config_json_name,
        "statedir": "statedir.tar",
        "sim_time": state["sim"]["current_time"],
        "next_event_time": state.get("next_event_time"),
        "faketime_effective": state.get("faketime_effective"),
        "reason": state.get("reason"),
        "jobs_total": state.get("jobs_total"),
        "jobs_completed": state["sim"]["num_complete"],
        "jobs_running": len(state.get("running_jobids") or []),
        "jobs_pending": len(state.get("pending_jobids") or []),
        "sha256": {
            "state.json": _sha256(work / "state.json"),
            "statedir.tar": _sha256(work / "statedir.tar"),
        },
    }
    # Manifest last, then the rename: a generation is either complete or absent.
    (work / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                                        encoding="utf-8")
    os.replace(work, final)
    latest_tmp = checkpoint_dir / ".LATEST.tmp"
    latest_tmp.write_text(name + "\n", encoding="utf-8")
    os.replace(latest_tmp, checkpoint_dir / "LATEST")
    return Generation(final, manifest)


def open_generation(path: Path) -> Generation:
    """Open a generation directory, or the LATEST one of a checkpoint directory."""
    path = Path(path)
    if (path / "LATEST").is_file():
        path = path / (path / "LATEST").read_text(encoding="utf-8").strip()
    manifest_path = path / "MANIFEST.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"not a complete checkpoint generation (no MANIFEST.json): {path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != MANIFEST_FORMAT or manifest.get("version") != MANIFEST_VERSION:
        raise ValueError(f"unsupported checkpoint manifest in {path}")
    gen = Generation(path, manifest)
    for name, digest in (manifest.get("sha256") or {}).items():
        if _sha256(path / name) != digest:
            raise ValueError(f"checkpoint file {path / name} does not match its manifest checksum")
    return gen


def extract_statedir(gen: Generation, dest_dir: Path) -> Path:
    _tar_extract(gen.statedir_tar, dest_dir)
    return dest_dir
