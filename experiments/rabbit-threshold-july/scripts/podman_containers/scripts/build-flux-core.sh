#!/bin/bash
set -euo pipefail

export SRC_ROOT=/workspace
export INSTALL_ROOT="$SRC_ROOT/container-installs"
export FLUX_PREFIX="$INSTALL_ROOT/flux-core"

git config --global --add safe.directory "$SRC_ROOT/flux-core" >/dev/null 2>&1 || true
mkdir -p "$INSTALL_ROOT/flux-core"

cd "$SRC_ROOT/flux-core"

if [ ! -x ./configure ]; then
    ./autogen.sh
fi

CC=gcc-12 CXX=g++-12 ./configure --prefix="$FLUX_PREFIX" --disable-docs

make -j"$(nproc)"
make install

echo
echo "flux-core installed to: $FLUX_PREFIX"
