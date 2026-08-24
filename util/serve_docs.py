#!/usr/bin/env python3
"""Build and serve the Flux Fiction documentation.

This is intentionally lightweight:

- MkDocs generates the static files in ``site/``.
- Python's standard-library HTTP server serves those files.
- The default host is ``0.0.0.0`` so the page can be viewed remotely when the
  port is reachable or forwarded.
"""

from __future__ import annotations

import argparse
import functools
import http.server
import pathlib
import socket
import subprocess
import sys


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def build_docs(root: pathlib.Path) -> None:
    config = root / "mkdocs.yml"
    if not config.exists():
        raise SystemExit(f"missing MkDocs config: {config}")

    try:
        import mkdocs  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "MkDocs is not installed for this Python interpreter.\n"
            "Install the docs dependencies with: "
            f"{sys.executable} -m pip install -r requirements-docs.txt"
        ) from exc

    subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--config-file",
            str(config),
        ],
        cwd=root,
        check=True,
    )


def serve(site_dir: pathlib.Path, host: str, port: int) -> None:
    if not site_dir.is_dir():
        raise SystemExit(
            f"missing generated docs directory: {site_dir}\n"
            "Run without --no-build, or run `python -m mkdocs build --strict` first."
        )

    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler,
        directory=str(site_dir),
    )

    class ReusableThreadingHTTPServer(http.server.ThreadingHTTPServer):
        allow_reuse_address = True

    with ReusableThreadingHTTPServer((host, port), handler) as httpd:
        actual_host, actual_port = httpd.server_address[:2]
        display_host = socket.gethostname() if actual_host == "0.0.0.0" else actual_host

        print(f"Serving {site_dir}")
        print(f"Listening on http://{actual_host}:{actual_port}/")
        if actual_host == "0.0.0.0":
            print(f"Remote URL: http://{display_host}:{actual_port}/")
        print("Press Ctrl+C to stop.")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and serve the Flux Fiction docs for local or remote viewing.",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"interface to bind; use 127.0.0.1 for local-only serving (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        default=DEFAULT_PORT,
        type=int,
        help=f"TCP port to listen on (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--site-dir",
        default="site",
        type=pathlib.Path,
        help="generated site directory, relative to the repo root unless absolute (default: site)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="serve an existing site directory without running MkDocs first",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    root = repo_root()
    site_dir = args.site_dir if args.site_dir.is_absolute() else root / args.site_dir

    if not args.no_build:
        build_docs(root)

    serve(site_dir.resolve(), args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
