#!/bin/bash
set -euo pipefail

export SRC_ROOT=/workspace
export INSTALL_ROOT="$SRC_ROOT/container-installs"
export FLUX_PREFIX="$INSTALL_ROOT/flux-core"

git config --global --add safe.directory "$SRC_ROOT/flux-sched" >/dev/null 2>&1 || true
source /usr/local/bin/flux-dev-env.sh

if [ -z "${FLUX_SCHED_VERSION:-}" ]; then
  FLUX_SCHED_GIT_VERSION="$(cd "$SRC_ROOT/flux-sched" && git describe --tags --always 2>/dev/null || true)"
  case "$FLUX_SCHED_GIT_VERSION" in
    ''|*[!0-9.]*)
      export FLUX_SCHED_VERSION=0.0.0
      ;;
    *)
      export FLUX_SCHED_VERSION="${FLUX_SCHED_GIT_VERSION#v}"
      ;;
  esac
fi

cd "$SRC_ROOT/flux-sched"
mkdir -p build
# CMAKE_BUILD_TYPE is REQUIRED here. With it unset, CMake adds no optimization
# flags at all and Fluxion is effectively -O0. Measured 2026-07-26 on the 20-job
# probe (conservative, queue-depth 2048, full 1153-node tuolumne graph):
#   unset  -> 495.4 s median, sched-fluxion-resource.so = 671,016 bytes
#   Release-> 182.2 s median, sched-fluxion-resource.so = 158,568 bytes
# i.e. a 2.72x slowdown, which was the whole of the container-vs-Spack gap.
CC=gcc-12 CXX=g++-12 cmake -S . -B build \
  -DCMAKE_BUILD_TYPE="${FLUX_SCHED_BUILD_TYPE:-Release}" \
  -DCMAKE_INSTALL_PREFIX="$FLUX_PREFIX" \
  -DCMAKE_PREFIX_PATH="$FLUX_PREFIX" \
  -DENABLE_DOCS=Off
cmake --build build -j"$(nproc)"
cmake --install build

echo
echo "flux-sched installed to: $FLUX_PREFIX"
