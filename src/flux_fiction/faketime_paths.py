from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile


def default_stampfile_path(run_root: str | os.PathLike[str]) -> Path:
    """
    Keep the libfaketime stamp file on container-local temp storage.

    libfaketime rereads the timestamp file frequently when caching is disabled.
    Using a bind-mounted workspace path turns those reads into a major startup
    bottleneck under Podman, so derive a stable tmp-backed path from the run
    directory instead.
    """
    resolved = Path(run_root).expanduser()
    if not resolved.is_absolute():
        resolved = resolved.resolve()

    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    safe_name = resolved.name or "run"
    return Path(tempfile.gettempdir()) / "flux-fiction-faketime" / f"{safe_name}-{digest}" / "faketime_stamp"
