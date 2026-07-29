#!/usr/bin/env bash
# Build the container's Fluxion with -O3 into a SEPARATE prefix.
#
# Why a separate prefix: podman_containers/scripts/build-flux-sched.sh runs cmake
# with no -DCMAKE_BUILD_TYPE, so CMake adds no optimization flags at all and the
# modules are effectively -O0. Rebuilding in place would overwrite .so files that
# live Flux brokers already have loaded, so this installs alongside instead and
# leaves the running campaign untouched.
#
# Run it on a compute node -- podman's flags get mangled if passed inline through
# `flux run`, hence this being a script file:
#   flux run -N1 -n1 --exclusive -q pdebug -t 30m bash util/build_fluxion_o3.sh
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/usr/WS1/ashworth12/ff-podman}"
BASE_INSTALLS="${WORKSPACE_ROOT}/container-installs"
OPT_INSTALLS="${OPT_INSTALLS:-${WORKSPACE_ROOT}/container-installs-o3}"
IMAGE="${FLUX_FICTION_IMAGE:-localhost/flux-fiction-dev:latest}"
IMAGE_TAR="${WORKSPACE_ROOT}/flux-fiction-dev.tar"
BUILD_DIR="${BUILD_DIR:-build-o3}"

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
    echo "loading image..."
    podman load -i "${IMAGE_TAR}" </dev/null
fi

# Seed the new prefix from the existing one so flux-core headers/libs are present
# for flux-sched to link against; only the Fluxion modules get replaced.
if [[ ! -d "${OPT_INSTALLS}" ]]; then
    echo "seeding ${OPT_INSTALLS} from ${BASE_INSTALLS} ..."
    # tar, not cp -a: cp -a fails with spurious ENOENT on this NFS prefix
    # (setgid dirs), leaving a partial copy.
    mkdir -p "${OPT_INSTALLS}"
    ( cd "${BASE_INSTALLS}" && tar cf - . ) | ( cd "${OPT_INSTALLS}" && tar xf - )
fi

echo "=== before:"
ls -l "${OPT_INSTALLS}/flux-core/lib/flux/modules/sched-fluxion-resource.so" | awk '{print "   ",$5,"bytes"}'

# The second -v shadows container-installs inside the workspace mount, so the
# build installs into the new prefix while reading sources from the same tree
# the running campaign uses.
podman run --rm --pull=never \
    -v "${WORKSPACE_ROOT}:/workspace" \
    -v "${OPT_INSTALLS}:/workspace/container-installs" \
    "${IMAGE}" \
    bash -lc "
set -euo pipefail
source /usr/local/bin/flux-dev-env.sh 2>/dev/null || true
git config --global --add safe.directory /workspace/flux-sched >/dev/null 2>&1 || true
cd /workspace/flux-sched
echo 'flux-sched source: '\$(git describe --tags --always 2>/dev/null || echo unknown)
CC=gcc-12 CXX=g++-12 cmake -S . -B ${BUILD_DIR} \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/workspace/container-installs/flux-core \
    -DCMAKE_PREFIX_PATH=/workspace/container-installs/flux-core \
    -DENABLE_DOCS=Off > /tmp/cfg.log 2>&1 || { tail -25 /tmp/cfg.log; exit 1; }
grep -m1 'CMAKE_BUILD_TYPE' ${BUILD_DIR}/CMakeCache.txt || true
cmake --build ${BUILD_DIR} -j\$(nproc) > /tmp/build.log 2>&1 || { tail -30 /tmp/build.log; exit 1; }
cmake --install ${BUILD_DIR} > /tmp/install.log 2>&1 || { tail -20 /tmp/install.log; exit 1; }
echo 'build+install ok'
" </dev/null

echo "=== after:"
ls -l "${OPT_INSTALLS}/flux-core/lib/flux/modules/sched-fluxion-resource.so" | awk '{print "   ",$5,"bytes"}'
echo "=== reference: 671016 = -O0 (current), ~163528 = Release"
