from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile


def stampfile_root() -> Path:
    """
    Pick the fastest memory-backed directory available for the stamp file.

    We run with ``FAKETIME_NO_CACHE=1``, so libfaketime does a full
    ``fopen``/``fgets``/``fclose`` on the timestamp file for *every* clock call
    in every preloaded process. The cost of that loop is dominated entirely by
    which filesystem backs the file:

        bare clock_gettime (vDSO)   ~0.02 us
        open+read+close on tmpfs      ~20 us
        open+read+close on NFS       ~311 us

    Inside Podman, ``/tmp`` belongs to the container's overlay, and this site
    configures overlay with the ``fuse-overlayfs`` mount program -- so every one
    of those opens round-trips through a userspace FUSE daemon. ``/dev/shm`` is
    mounted directly as tmpfs by Podman and bypasses the overlay entirely, which
    is as close to "in memory" as libfaketime can be made to read.

    ``FLUX_FICTION_FAKETIME_DIR`` overrides the choice for sites where
    ``/dev/shm`` is unusably small or unavailable.
    """
    override = os.environ.get("FLUX_FICTION_FAKETIME_DIR")
    if override:
        return Path(override).expanduser()

    shm = Path("/dev/shm")
    if os.path.isdir(shm) and os.access(shm, os.W_OK):
        return shm

    return Path(tempfile.gettempdir())


def default_stampfile_path(run_root: str | os.PathLike[str]) -> Path:
    """
    Derive a stable memory-backed stamp file path from the run directory.

    The path must be stable across processes in a run (the controller writes it,
    every libfaketime-preloaded child reads it) but distinct between concurrent
    runs, hence the digest of the resolved run root. See :func:`stampfile_root`
    for why this never lives under the bind-mounted workspace.
    """
    resolved = Path(run_root).expanduser()
    if not resolved.is_absolute():
        resolved = resolved.resolve()

    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    safe_name = resolved.name or "run"
    return stampfile_root() / "flux-fiction-faketime" / f"{safe_name}-{digest}" / "faketime_stamp"
