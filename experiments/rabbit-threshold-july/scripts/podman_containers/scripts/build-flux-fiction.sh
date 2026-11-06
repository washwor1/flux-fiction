#!/bin/bash
set -euo pipefail

export SRC_ROOT=/workspace

git config --global --add safe.directory "$SRC_ROOT/flux-fiction" >/dev/null 2>&1 || true
source /usr/local/bin/flux-dev-env.sh

cd "$SRC_ROOT/flux-fiction"
python3 -m pip install --break-system-packages --no-build-isolation --no-deps -e .

echo
echo "flux-fiction installed into the container Python environment"
echo "CLI commands: flux-fiction, flux-fiction-run, flux-fiction-jobtap-path"
