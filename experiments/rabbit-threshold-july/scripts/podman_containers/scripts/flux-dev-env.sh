#!/bin/bash

git config --global --add safe.directory /workspace/flux-core >/dev/null 2>&1 || true
git config --global --add safe.directory /workspace/flux-sched >/dev/null 2>&1 || true
git config --global --add safe.directory /workspace/flux-fiction >/dev/null 2>&1 || true

export FLUX_PREFIX=/workspace/container-installs/flux-core
export PATH="$FLUX_PREFIX/bin:${PATH:-}"
export LD_LIBRARY_PATH="$FLUX_PREFIX/lib:$FLUX_PREFIX/lib64:${LD_LIBRARY_PATH:-}"
export PKG_CONFIG_PATH="$FLUX_PREFIX/lib/pkgconfig:$FLUX_PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
export CC=gcc-12
export CXX=g++-12

if command -v flux >/dev/null 2>&1; then
    eval "$(flux env)" 2>/dev/null || true
fi
