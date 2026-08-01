#!/usr/bin/env bash
# Build two level-2 DFTracer Fluxion variants, then execute the 12-run Gaussian
# backfill matrix inside one bounded pdebug allocation.
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/g/g14/ashworth12/workspace/ff-podman}"
FF_SOURCE="${FF_SOURCE:-${WORKSPACE_ROOT}/flux-fiction-shared-clock}"
BASE_SOURCE="${BASE_SOURCE:-${WORKSPACE_ROOT}/flux-sched-dft-matrix-baseline}"
FEATURE_SOURCE="${FEATURE_SOURCE:-${WORKSPACE_ROOT}/flux-sched-dft-matrix-skip-empty}"
GAUSSIAN_TRACE="${GAUSSIAN_TRACE:-${WORKSPACE_ROOT}/salishan_runs/experiment_tuolumne_easy_gaussian_random/processed/input_with_latencies_patched.csv}"
PATCHED_CORE_PREFIX="${PATCHED_CORE_PREFIX:-/p/lustre5/ashworth12/flux-core-nodeonly.l5WGzh/prefix}"
IMAGE="${IMAGE:-localhost/flux-fiction-dev-dft:latest}"
IMAGE_TAR="${IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev-dft.tar}"
MATRIX_BUILD_TYPE="${MATRIX_BUILD_TYPE:-Release}"
OUT_ROOT="${OUT_ROOT:-/p/lustre5/ashworth12/dftracer-gaussian-backfill-rd4-$(date -u +%Y%m%d_%H%M%S)}"

[[ -d "${FF_SOURCE}" ]] || { echo "missing Flux Fiction source: ${FF_SOURCE}" >&2; exit 2; }
[[ -d "${BASE_SOURCE}" ]] || { echo "missing baseline scheduler source: ${BASE_SOURCE}" >&2; exit 2; }
[[ -d "${FEATURE_SOURCE}" ]] || { echo "missing feature scheduler source: ${FEATURE_SOURCE}" >&2; exit 2; }
[[ -f "${GAUSSIAN_TRACE}" ]] || { echo "missing Gaussian trace: ${GAUSSIAN_TRACE}" >&2; exit 2; }
[[ -x "${PATCHED_CORE_PREFIX}/bin/flux" ]] || { echo "missing patched Flux-core prefix: ${PATCHED_CORE_PREFIX}" >&2; exit 2; }

if [[ -e "${OUT_ROOT}" ]]; then
    echo "refusing to reuse existing output root: ${OUT_ROOT}" >&2
    exit 2
fi
mkdir -p "${OUT_ROOT}"

if [[ ! -d "${XDG_RUNTIME_DIR:-}" ]]; then
    export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"
    mkdir -p "${XDG_RUNTIME_DIR}"
    chmod 700 "${XDG_RUNTIME_DIR}"
fi
podman info >/dev/null 2>&1 </dev/null || true
podman image exists "${IMAGE}" </dev/null || podman load -i "${IMAGE_TAR}" </dev/null

if [[ -d /l/ssd && -w /l/ssd ]]; then
    scratch_parent=/l/ssd
else
    scratch_parent=/var/tmp
fi
scratch_host="$(mktemp -d "${scratch_parent}/ff-dft-matrix.XXXXXX")"
cleanup() {
    pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
    pkill -u "$(id -un)" -x conmon 2>/dev/null || true
    pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
    rm -rf "${scratch_host}"
}
trap cleanup EXIT

podman run --rm --pull=never \
    -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:ro" \
    -v "${OUT_ROOT}:${OUT_ROOT}:rw" \
    "${IMAGE}" python3 "${FF_SOURCE}/util/generate_dftracer_gaussian_matrix.py" \
    --out "${OUT_ROOT}" \
    --trace "${GAUSSIAN_TRACE}"

write_provenance() {
    python3 - "${OUT_ROOT}" "${BASE_SOURCE}" "${FEATURE_SOURCE}" "${MATRIX_BUILD_TYPE}" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

out, base, feature = map(Path, sys.argv[1:4])
build_type = sys.argv[4]
def revision(path: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        universal_newlines=True,
    ).strip()

(out / "scheduler_provenance.json").write_text(json.dumps({
    "baseline": {"source": str(base), "revision": revision(base)},
    "skip_empty_dfu_expansion": {"source": str(feature), "revision": revision(feature)},
    "dftracer_annotation_level": 2,
    "cmake_build_type": build_type or "<empty; unoptimized>",
}, indent=2, sort_keys=True) + "\n")
PY
}
write_provenance

build_variant() {
    local name="$1"
    local source="$2"
    local prefix="${OUT_ROOT}/installs-${name}/flux-sched"
    podman run --rm --pull=never \
        -v "${WORKSPACE_ROOT}:/workspace:ro" \
        -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:rw" \
        -v "${OUT_ROOT}:${OUT_ROOT}:rw" \
        -v "${PATCHED_CORE_PREFIX}:${PATCHED_CORE_PREFIX}:ro" \
        -v "${PATCHED_CORE_PREFIX}:/test-root/prefix:ro" \
        -e MATRIX_SOURCE="${source}" \
        -e MATRIX_PREFIX="${prefix}" \
        -e PATCHED_CORE_PREFIX="${PATCHED_CORE_PREFIX}" \
        -e MATRIX_BUILD_TYPE="${MATRIX_BUILD_TYPE}" \
        -e MATRIX_BUILD_DIR="/tmp/flux-sched-${name}-l2" \
        "${IMAGE}" bash -lc '
            set -euo pipefail
            source /usr/local/bin/flux-dev-env.sh
            export DFTRACER_ROOT=/usr/lib/python3/dist-packages/dftracer
            export FLUX_SCHED_VERSION=0.53.0
            test -f "$DFTRACER_ROOT/include/dftracer/dftracer.h"
            test -f "$DFTRACER_ROOT/lib/libdftracer_core.so"
            CC=gcc-12 CXX=g++-12 cmake -S "$MATRIX_SOURCE" -B "$MATRIX_BUILD_DIR" \
                -DCMAKE_BUILD_TYPE="$MATRIX_BUILD_TYPE" \
                -DCMAKE_INSTALL_PREFIX="$MATRIX_PREFIX" \
                -DCMAKE_PREFIX_PATH="$PATCHED_CORE_PREFIX" \
                -DDFTRACER_ANNOTATION_LEVEL=2 \
                -DENABLE_DOCS=Off
            cmake --build "$MATRIX_BUILD_DIR" -j"$(nproc)"
            cmake --install "$MATRIX_BUILD_DIR"
            resource_so="$MATRIX_PREFIX/lib/flux/modules/sched-fluxion-resource.so"
            qmanager_so="$MATRIX_PREFIX/lib/flux/modules/sched-fluxion-qmanager.so"
            readelf -d "$resource_so" | grep -q dftracer
            readelf -d "$qmanager_so" | grep -q dftracer
            if LD_LIBRARY_PATH="$MATRIX_PREFIX/lib:$DFTRACER_ROOT/lib:$PATCHED_CORE_PREFIX/lib" \
                    ldd -r "$resource_so" 2>&1 | grep -q "undefined symbol"; then
                echo "unresolved symbols in $resource_so" >&2
                exit 1
            fi
        ' >"${OUT_ROOT}/build-${name}.log" 2>&1
}

# The allocation is 58 minutes.  Leave time for both scheduler builds and for
# the final trace merge even if one simulation runs long.
build_variant baseline "${BASE_SOURCE}"
build_variant skip-empty-dfu-expansion "${FEATURE_SOURCE}"

run_group() {
    local name="$1"
    local scheduler_prefix="$2"
    podman run --rm --pull=never \
        --shm-size=2g \
        -v "${PATCHED_CORE_PREFIX}:/workspace/container-installs/flux-core:ro" \
        -v "${PATCHED_CORE_PREFIX}:/test-root/prefix:ro" \
        -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:ro" \
        -v "${OUT_ROOT}:${OUT_ROOT}:rw" \
        -v "${scratch_host}:/scratch" \
        -e PYTHONPATH="${FF_SOURCE}/src" \
        -e FAKETIME_LIB="${WORKSPACE_ROOT}/libfaketime-shared-clock/src/libfaketimeMT.so.1" \
        -e FLUX_FICTION_FAKETIME_MODE=shared \
        -e FLUX_FICTION_NO_BROKER_LOG_FILE=true \
        -e FLUX_CONF_DIR="${FF_SOURCE}/util/dftracer_no_ingest.toml" \
        -e FLUX_FICTION_WALLTIME_BUDGET_SECONDS=3000 \
        -e FLUX_FICTION_FINALIZE_RESERVE_SECONDS=300 \
        -e FLUX_FICTION_JOBTAP_SO="${FF_SOURCE}/build/cp312/src/emu-jobtap.so" \
        -e FLUX_FICTION_FLUXION_RESOURCE_MODULE="${scheduler_prefix}/lib/flux/modules/sched-fluxion-resource.so" \
        -e FLUX_FICTION_FLUXION_FEASIBILITY_MODULE="${scheduler_prefix}/lib/flux/modules/sched-fluxion-feasibility.so" \
        -e FLUX_FICTION_FLUXION_QMANAGER_MODULE="${scheduler_prefix}/lib/flux/modules/sched-fluxion-qmanager.so" \
        -e DFTRACER_ENABLE=1 \
        -e DFTRACER_TIME_METRIC=NS \
        -e DFTRACER_INC_METADATA=1 \
        -e DFTRACER_DISABLE_IO=1 \
        -e DFTRACER_FLUXION_FINALIZE_GENERATION=0 \
        -e DFTRACER_DATA_DIR="${OUT_ROOT}/dftracer" \
        -e DFTRACER_LOG_FILE="${OUT_ROOT}/dftracer/${name}/fluxion" \
        -e NO_FAKE_STAT=1 \
        -e MATRIX_OUT="${OUT_ROOT}" \
        -e MATRIX_GROUP="${name}" \
        "${IMAGE}" bash -lc '
            set -euo pipefail
            source /usr/local/bin/flux-dev-env.sh
            export LD_LIBRARY_PATH="/workspace/container-installs/flux-core/lib:${LD_LIBRARY_PATH:-}"
            mkdir -p "$MATRIX_OUT/dftracer"
            python3 -m flux_fiction.cli.run_ff_parallel \
                "$MATRIX_OUT/matrix/manifests/$MATRIX_GROUP.toml" \
                --show-makespan-extremes
        ' >"${OUT_ROOT}/${name}-parallel.log" 2>&1
}

run_group baseline "${OUT_ROOT}/installs-baseline/flux-sched" &
baseline_pid=$!
run_group skip-empty-dfu-expansion "${OUT_ROOT}/installs-skip-empty-dfu-expansion/flux-sched" &
feature_pid=$!
set +e
wait "$baseline_pid"; baseline_rc=$?
wait "$feature_pid"; feature_rc=$?
set -e
printf "baseline=%s\nskip_empty_dfu_expansion=%s\n" "$baseline_rc" "$feature_rc" \
    >"${OUT_ROOT}/matrix_return_codes.txt"
test "$baseline_rc" -eq 0
test "$feature_rc" -eq 0

python3 - "${OUT_ROOT}" <<'PY'
import gzip
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
traces = sorted(root.glob("dftracer/*/*/unified-trace.pfw.gz"))
summary = []
for trace in traces:
    events = 0
    jobid_events = 0
    with gzip.open(trace, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip().rstrip(",")
            if line in ("", "[", "]"):
                continue
            event = json.loads(line)
            events += 1
            args = event.get("args")
            if event.get("name") == "run_match" and isinstance(args, dict) and "jobid" in args:
                jobid_events += 1
    summary.append({
        "trace": str(trace),
        "events": events,
        "run_match_events_with_jobid": jobid_events,
    })

(root / "dftracer_trace_validation.json").write_text(
    json.dumps({"trace_count": len(summary), "traces": summary}, indent=2) + "\n",
    encoding="utf-8",
)
if len(summary) != 12 or any(item["run_match_events_with_jobid"] == 0 for item in summary):
    raise SystemExit("DFTracer validation failed: expected 12 merged traces with run_match jobid metadata")
PY

echo "matrix completed: ${OUT_ROOT}"
