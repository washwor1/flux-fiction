"""CLI wrapper for the standalone ensemble campaign runner."""

from flux_fiction.ensemble.cli import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
