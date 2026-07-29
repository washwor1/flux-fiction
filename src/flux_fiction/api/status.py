from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any

logger = logging.getLogger(__name__)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class RunStatusWriter:
    def __init__(self, path: str | os.PathLike[str] | None) -> None:
        self.path = Path(path) if path else None

    @property
    def enabled(self) -> bool:
        return self.path is not None

    def read(self) -> dict[str, Any]:
        # No exists() pre-check: it only swallows a fixed set of errnos, so a
        # Lustre hiccup raises out of os.stat instead of reading as "absent".
        if self.path is None:
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def update(self, **fields: Any) -> None:
        if self.path is None:
            return

        payload = self.read()
        payload.setdefault("version", 1)
        payload.update({key: value for key, value in fields.items() if value is not None})
        payload["updated_at"] = utcnow_iso()

        # Status writing is pure reporting and must never take down the run it
        # is reporting on. Unguarded, a transient filesystem error here unwound
        # out of the parent runner's progress loop and killed every live child
        # with it: measured 2026-07-27, when lustre1 returned ENOTCONN/EIO
        # under the campaign's own load and 267 dane child runs died this way.
        tmp_path: Path | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                delete=False,
            ) as tmp:
                json.dump(payload, tmp, indent=2, sort_keys=True)
                tmp.write("\n")
                tmp_path = Path(tmp.name)
            os.replace(tmp_path, self.path)
        except Exception as exc:
            logger.warning("could not write status to %s: %r", self.path, exc)
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
