#!/usr/bin/env bash
# Complete the conservative core-request A/B using the private level-2
# scheduler prefixes already built by the Gaussian matrix attempt.
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/g/g14/ashworth12/workspace/ff-podman}"
FF_SOURCE="${FF_SOURCE:-${WORKSPACE_ROOT}/flux-fiction-shared-clock}"
SOURCE_ROOT="${SOURCE_ROOT:-/p/lustre5/ashworth12/dftracer-gaussian-backfill-rd4-20260730_033450}"
IMAGE="${IMAGE:-localhost/flux-fiction-dev-dft:latest}"
IMAGE_TAR="${IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev-dft.tar}"
OUT_ROOT="${OUT_ROOT:-/p/lustre5/ashworth12/dftracer-gaussian-conservative-cores-$(date -u +%Y%m%d_%H%M%S)}"

[[ -f "${SOURCE_ROOT}/matrix/configs/baseline-conservative-cores.toml" ]] || {
    echo "missing source conservative configuration" >&2
    exit 2
}
[[ -d "${SOURCE_ROOT}/installs-baseline/flux-core" ]] || {
    echo "missing baseline private Flux prefix" >&2
    exit 2
}
[[ -d "${SOURCE_ROOT}/installs-skip-empty-dfu-expansion/flux-core" ]] || {
    echo "missing feature private Flux prefix" >&2
    exit 2
}
[[ ! -e "${OUT_ROOT}" ]] || { echo "output root exists: ${OUT_ROOT}" >&2; exit 2; }
mkdir -p "${OUT_ROOT}/dftracer"

if [[ ! -d "${XDG_RUNTIME_DIR:-}" ]]; then
    export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"
    mkdir -p "${XDG_RUNTIME_DIR}"
    chmod 700 "${XDG_RUNTIME_DIR}"
fi
podman image exists "${IMAGE}" </dev/null || podman load -i "${IMAGE_TAR}" </dev/null

if [[ -d /l/ssd && -w /l/ssd ]]; then
    scratch_parent=/l/ssd
else
    scratch_parent=/var/tmp
fi
scratch_host="$(mktemp -d "${scratch_parent}/ff-dft-conservative.XXXXXX")"
cleanup() {
    pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
    pkill -u "$(id -un)" -x conmon 2>/dev/null || true
    pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
    rm -rf "${scratch_host}"
}
trap cleanup EXIT

run_one() {
    local name="$1"
    local prefix="$2"
    local config="$3"
    podman run --rm --pull=never \
        --shm-size=2g \
        -v "${prefix}:/workspace/container-installs/flux-core:ro" \
        -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:ro" \
        -v "${SOURCE_ROOT}:${SOURCE_ROOT}:ro" \
        -v "${OUT_ROOT}:${OUT_ROOT}:rw" \
        -v "${scratch_host}:/scratch" \
        -e PYTHONPATH="${FF_SOURCE}/src" \
        -e FAKETIME_LIB="${WORKSPACE_ROOT}/libfaketime-shared-clock/src/libfaketimeMT.so.1" \
        -e FLUX_FICTION_FAKETIME_MODE=shared \
        -e FLUX_FICTION_WALLTIME_BUDGET_SECONDS=600 \
        -e FLUX_FICTION_FINALIZE_RESERVE_SECONDS=60 \
        -e FLUX_FICTION_JOBTAP_SO="${FF_SOURCE}/build/cp312/src/emu-jobtap.so" \
        -e FLUX_FICTION_FLUXION_RESOURCE_MODULE=/workspace/container-installs/flux-core/lib/flux/modules/sched-fluxion-resource.so \
        -e FLUX_FICTION_FLUXION_FEASIBILITY_MODULE=/workspace/container-installs/flux-core/lib/flux/modules/sched-fluxion-feasibility.so \
        -e FLUX_FICTION_FLUXION_QMANAGER_MODULE=/workspace/container-installs/flux-core/lib/flux/modules/sched-fluxion-qmanager.so \
        -e DFTRACER_ENABLE=1 \
        -e DFTRACER_TIME_METRIC=NS \
        -e DFTRACER_INC_METADATA=1 \
        -e DFTRACER_DISABLE_IO=1 \
        -e DFTRACER_DATA_DIR="${OUT_ROOT}/dftracer" \
        -e DFTRACER_LOG_FILE="${OUT_ROOT}/dftracer/${name}/fluxion" \
        -e NO_FAKE_STAT=1 \
        -e RUN_CONFIG="${config}" \
        -e RUN_DIR="${OUT_ROOT}/runs/${name}" \
        "${IMAGE}" bash -lc '
            set -euo pipefail
            source /usr/local/bin/flux-dev-env.sh
            export LD_LIBRARY_PATH="/workspace/container-installs/flux-core/lib:${LD_LIBRARY_PATH:-}"
            python3 -m flux_fiction.cli.run_ff "$RUN_CONFIG" \
                --run-dir "$RUN_DIR" \
                --faketime-mode shared \
                --submit-novalidate \
                --no-broker-log-file
        ' >"${OUT_ROOT}/${name}.log" 2>&1
}

run_one baseline "${SOURCE_ROOT}/installs-baseline/flux-core" \
    "${SOURCE_ROOT}/matrix/configs/baseline-conservative-cores.toml" &
baseline_pid=$!
run_one skip-empty-dfu-expansion "${SOURCE_ROOT}/installs-skip-empty-dfu-expansion/flux-core" \
    "${SOURCE_ROOT}/matrix/configs/skip-empty-dfu-expansion-conservative-cores.toml" &
feature_pid=$!
set +e
wait "${baseline_pid}"; baseline_rc=$?
wait "${feature_pid}"; feature_rc=$?
set -e
printf "baseline=%s\nskip_empty_dfu_expansion=%s\n" "${baseline_rc}" "${feature_rc}" \
    >"${OUT_ROOT}/return_codes.txt"
test "${baseline_rc}" -eq 0
test "${feature_rc}" -eq 0
