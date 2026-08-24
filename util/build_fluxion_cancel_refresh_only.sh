#!/usr/bin/env bash
# Build Fluxion with the cancel-refresh traverser patch ONLY -- no skip-empty.
#
# Why this exists: every cancel-refresh build in the workspace is revision
# ee7e5c7a, which descends from 8ad64768 ("skip expansion when next is empty").
# That makes it impossible to attribute a measurement to the cancel-refresh patch
# alone. This builds the missing cell: d21ef77a (baseline) + a cherry-pick of
# ee7e5c7a, with skip-empty absent from the history.
#
# Source tree: flux-sched-dft-matrix-cancel-refresh-only, a git WORKTREE at
# 957c1fdc. Its .git is a file, not a directory -- do not test it with `-d .git`.
#
# Must run on a compute node: podman storage is node-local (/var/tmp/$USER), so
# the image has to be loaded in the same allocation that builds. Podman flags get
# mangled if passed inline through `flux run`, which is why this is a script file:
#   flux run -N1 -n1 --exclusive -q pdebug -t 60m bash <this script>
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/usr/WS1/ashworth12/ff-podman}"
SRC_NAME="${SRC_NAME:-flux-sched-dft-matrix-cancel-refresh-only}"
OUT_ROOT="${OUT_ROOT:?set OUT_ROOT to the build root}"
VARIANT="${VARIANT:-cancel-refresh-only}"
CORE_PREFIX="${CORE_PREFIX:-/p/lustre5/ashworth12/flux-core-nodeonly.l5WGzh/prefix}"
IMAGE="${FLUX_FICTION_IMAGE:-localhost/flux-fiction-dev-dft:latest}"
IMAGE_TAR="${IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev-dft.tar}"
BUILD_DIR="${BUILD_DIR:-build-cancel-refresh-only}"
# Matches the baseline and skip-empty builds this variant is compared against.
DFT_LEVEL="${DFT_LEVEL:-2}"

PREFIX="${OUT_ROOT}/installs-${VARIANT}/flux-sched"
PROV="${OUT_ROOT}/provenance"
mkdir -p "${PREFIX}" "${PROV}"

if [[ -z "${XDG_RUNTIME_DIR:-}" || ! -d "${XDG_RUNTIME_DIR}" ]]; then
    export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"
    mkdir -p "${XDG_RUNTIME_DIR}"
fi
podman info >/dev/null 2>&1 </dev/null || true
cleanup() {
    pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
    pkill -u "$(id -un)" -x conmon 2>/dev/null || true
    pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
}
trap cleanup EXIT

if ! podman image exists "${IMAGE}" </dev/null; then
    echo "=== loading image from ${IMAGE_TAR}"
    podman load -i "${IMAGE_TAR}" </dev/null
fi

# Provenance, recorded the same way the other variant builds record it, so the
# ledger row can cite files rather than assertions.
( cd "${WORKSPACE_ROOT}/${SRC_NAME}"
  git rev-parse HEAD                       > "${PROV}/${VARIANT}-revision.txt"
  git log --oneline --stat -3              > "${PROV}/${VARIANT}-log.txt"
  git status --porcelain                   > "${PROV}/${VARIANT}-status.txt"
  git diff                                 > "${PROV}/${VARIANT}-local.diff"
  # The discriminating check: skip-empty must NOT be in this history, and
  # dfu_impl.cpp must be identical to baseline.
  { echo "skip-empty (8ad64768) present in history: $(git log --oneline | grep -c 8ad64768 || true)"
    echo "dfu_impl.cpp vs baseline d21ef77a:"
    git diff --stat d21ef77a HEAD -- resource/traversers/dfu_impl.cpp
  } > "${PROV}/${VARIANT}-isolation-check.txt" )

cat > "${PROV}/build.env" <<EOF
variant=${VARIANT}
source=${WORKSPACE_ROOT}/${SRC_NAME}
prefix=${PREFIX}
core_prefix=${CORE_PREFIX}
image=${IMAGE}
cmake_build_type=Release
dftracer_annotation_level=${DFT_LEVEL}
built_on=$(hostname)
built_at=$(date -uIseconds)
EOF

echo "=== building ${VARIANT} -> ${PREFIX}"
podman run --rm --pull=never \
    -v "${WORKSPACE_ROOT}:/workspace" \
    -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}" \
    -v "${OUT_ROOT}:/out" \
    -v "${CORE_PREFIX}:${CORE_PREFIX}:ro" \
    -v "${CORE_PREFIX}:/test-root/prefix:ro" \
    "${IMAGE}" \
    bash -lc "
set -euo pipefail
source /usr/local/bin/flux-dev-env.sh 2>/dev/null || true
# The node-only flux-core was configured with /test-root/prefix baked in, so its
# pkg-config files hand cmake that path for includes and libs. Mounting the
# prefix there (which is also what the ensemble worker does) is what makes the
# imported PkgConfig::FLUX_CORE target resolve.
export PATH=/test-root/prefix/bin:\$PATH
# DFTRACER_ROOT is what the 2026-07-30 baseline/skip-empty builds used; without
# it cmake fails outright at DFTRACER_ANNOTATION_LEVEL=2.
export DFTRACER_ROOT=\"\${DFTRACER_ROOT:-/usr/lib/python3/dist-packages/dftracer}\"
# The worktree's .git is a file pointing into the parent repo, so BOTH paths
# need to be marked safe or git describe fails and the version goes 'unknown'.
git config --global --add safe.directory /workspace/${SRC_NAME} >/dev/null 2>&1 || true
git config --global --add safe.directory /workspace/flux-sched  >/dev/null 2>&1 || true
cd /workspace/${SRC_NAME}
echo 'flux-sched source: '\$(git describe --tags --always 2>/dev/null || echo unknown)
CC=gcc-12 CXX=g++-12 cmake -S . -B ${BUILD_DIR} \
    -DCMAKE_BUILD_TYPE=Release \
    -DDFTRACER_ANNOTATION_LEVEL=${DFT_LEVEL} \
    -DCMAKE_INSTALL_PREFIX=/out/installs-${VARIANT}/flux-sched \
    -DCMAKE_PREFIX_PATH=/test-root/prefix \
    -DENABLE_DOCS=Off
grep -m1 'CMAKE_BUILD_TYPE' ${BUILD_DIR}/CMakeCache.txt || true
cmake --build ${BUILD_DIR} -j\$(nproc)
cmake --install ${BUILD_DIR}
echo 'build+install ok'
" </dev/null

echo "=== installed modules:"
find "${PREFIX}" -name 'sched-fluxion-*.so' -printf '   %10s  %f\n'
echo "=== NOTE: the ~158 KB Release size check is only valid for the container toolchain."
echo "=== Authoritative check is the explicit -DCMAKE_BUILD_TYPE=Release above."
