#!/usr/bin/env bash
# Build O3 level-2 current/v0.48 Fluxion and run one accuracy-control pair.
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: $0 OUT_ROOT POLICY latency-on|latency-off" >&2
    exit 2
fi

OUT_ROOT="$1"
POLICY="$2"
LATENCY_NAME="$3"

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/g/g14/ashworth12/workspace/ff-podman}"
FF_SOURCE="${FF_SOURCE:-${WORKSPACE_ROOT}/flux-fiction-shared-clock}"
V053_SOURCE="${V053_SOURCE:-${WORKSPACE_ROOT}/flux-sched-dft-matrix-baseline}"
V048_SOURCE="${V048_SOURCE:-${WORKSPACE_ROOT}/flux-sched-dft-v048}"
PATCHED_CORE_PREFIX="${PATCHED_CORE_PREFIX:-/p/lustre5/ashworth12/flux-core-nodeonly.l5WGzh/prefix}"
IMAGE="${IMAGE:-localhost/flux-fiction-dev-dft:latest}"
IMAGE_TAR="${IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev-dft.tar}"
PAIR_NAME="${POLICY}-${LATENCY_NAME}"
PAIR_ROOT="${OUT_ROOT}/pairs/${PAIR_NAME}"

case "${POLICY}" in easy|conservative|hybrid) ;; *) echo "invalid policy: ${POLICY}" >&2; exit 2 ;; esac
case "${LATENCY_NAME}" in latency-on|latency-off) ;; *) echo "invalid latency setting: ${LATENCY_NAME}" >&2; exit 2 ;; esac
[[ -f "${OUT_ROOT}/matrix.json" ]] || { echo "missing generated matrix: ${OUT_ROOT}/matrix.json" >&2; exit 2; }
[[ -d "${V053_SOURCE}" ]] || { echo "missing current scheduler: ${V053_SOURCE}" >&2; exit 2; }
[[ -d "${V048_SOURCE}" ]] || { echo "missing v0.48 scheduler: ${V048_SOURCE}" >&2; exit 2; }
[[ -x "${PATCHED_CORE_PREFIX}/bin/flux" ]] || { echo "missing Flux core: ${PATCHED_CORE_PREFIX}" >&2; exit 2; }
mkdir -p "${PAIR_ROOT}"

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
scratch_host="$(mktemp -d "${scratch_parent}/ff-dft-accuracy-${PAIR_NAME}.XXXXXX")"
cleanup() {
    pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
    pkill -u "$(id -un)" -x conmon 2>/dev/null || true
    pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
    rm -rf "${scratch_host}"
}
trap cleanup EXIT

# CMake's legacy v0.48 Valgrind probe writes config.h into the source tree.
# Stage private source copies so six pdebug pairs can configure concurrently
# without racing on either shared checkout.  This also snapshots dirty DFTracer
# metadata changes consistently for the duration of this pair.
mkdir -p "${scratch_host}/sources"
cp -a "${V053_SOURCE}" "${scratch_host}/sources/v053"
cp -a "${V048_SOURCE}" "${scratch_host}/sources/v048"

revision() {
    git -C "$1" rev-parse HEAD
}
dirty_state() {
    if [[ -n "$(git -C "$1" status --short)" ]]; then printf dirty; else printf clean; fi
}

python3 - "${PAIR_ROOT}" "${V053_SOURCE}" "${V048_SOURCE}" \
    "$(revision "${V053_SOURCE}")" "$(revision "${V048_SOURCE}")" \
    "$(dirty_state "${V053_SOURCE}")" "$(dirty_state "${V048_SOURCE}")" <<'PY'
import json
import sys
from pathlib import Path

pair_root, v053, v048 = map(Path, sys.argv[1:4])
(pair_root / "scheduler_provenance.json").write_text(json.dumps({
    "v053": {"source": str(v053), "revision": sys.argv[4], "worktree": sys.argv[6]},
    "v048": {"source": str(v048), "revision": sys.argv[5], "worktree": sys.argv[7]},
    "dftracer_annotation_level": 2,
    "cmake_build_type": "Release",
}, indent=2, sort_keys=True) + "\n")
PY

build_release() {
    local release="$1"
    local source="$2"
    local version="$3"
    local prefix="${PAIR_ROOT}/installs-${release}/flux-sched"
    podman run --rm --pull=never \
        -v "${WORKSPACE_ROOT}:/workspace:ro" \
        -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:rw" \
        -v "${OUT_ROOT}:${OUT_ROOT}:rw" \
        -v "${scratch_host}:/scratch:rw" \
        -v "${PATCHED_CORE_PREFIX}:${PATCHED_CORE_PREFIX}:ro" \
        -v "${PATCHED_CORE_PREFIX}:/test-root/prefix:ro" \
        -e MATRIX_SOURCE="${source}" \
        -e MATRIX_PREFIX="${prefix}" \
        -e PATCHED_CORE_PREFIX="${PATCHED_CORE_PREFIX}" \
        -e FLUX_SCHED_VERSION="${version}" \
        -e MATRIX_BUILD_DIR="/tmp/flux-sched-${release}-${PAIR_NAME}-l2" \
        "${IMAGE}" bash -lc '
            set -euo pipefail
            source /usr/local/bin/flux-dev-env.sh
            export DFTRACER_ROOT=/usr/lib/python3/dist-packages/dftracer
            CC=gcc-12 CXX=g++-12 cmake -S "$MATRIX_SOURCE" -B "$MATRIX_BUILD_DIR" \
                -DCMAKE_BUILD_TYPE=Release \
                -DCMAKE_INSTALL_PREFIX="$MATRIX_PREFIX" \
                -DCMAKE_PREFIX_PATH="$PATCHED_CORE_PREFIX" \
                -DDFTRACER_ANNOTATION_LEVEL=2 \
                -DENABLE_DOCS=Off
            cmake --build "$MATRIX_BUILD_DIR" -j"$(nproc)"
            cmake --install "$MATRIX_BUILD_DIR"
            for module in sched-fluxion-resource sched-fluxion-qmanager; do
                so="$MATRIX_PREFIX/lib/flux/modules/$module.so"
                readelf -d "$so" | grep -q dftracer
                if LD_LIBRARY_PATH="$MATRIX_PREFIX/lib:$DFTRACER_ROOT/lib:$PATCHED_CORE_PREFIX/lib" \
                        ldd -r "$so" 2>&1 | grep -q "undefined symbol"; then
                    echo "unresolved symbols in $so" >&2
                    exit 1
                fi
            done
        ' >"${PAIR_ROOT}/build-${release}.log" 2>&1
}

build_release v053 /scratch/sources/v053 0.53.0
build_release v048 /scratch/sources/v048 0.48.0

run_release() {
    local release="$1"
    local scheduler_prefix="${PAIR_ROOT}/installs-${release}/flux-sched"
    local manifest="${OUT_ROOT}/matrix/manifests/${release}-${POLICY}-${LATENCY_NAME}.toml"
    local trace_dir="${OUT_ROOT}/dftracer/${release}/${PAIR_NAME}"
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
        -e FLUX_FICTION_WALLTIME_BUDGET_SECONDS=3300 \
        -e FLUX_FICTION_FINALIZE_RESERVE_SECONDS=180 \
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
        -e DFTRACER_LOG_FILE="${trace_dir}/fluxion" \
        -e NO_FAKE_STAT=1 \
        "${IMAGE}" bash -lc '
            set -euo pipefail
            source /usr/local/bin/flux-dev-env.sh
            export LD_LIBRARY_PATH="/workspace/container-installs/flux-core/lib:${LD_LIBRARY_PATH:-}"
            mkdir -p "$(dirname "$DFTRACER_LOG_FILE")"
            python3 -m flux_fiction.cli.run_ff_parallel "'"${manifest}"'" --show-makespan-extremes
        ' >"${PAIR_ROOT}/${release}-parallel.log" 2>&1
}

run_release v053 &
v053_pid=$!
run_release v048 &
v048_pid=$!
set +e
wait "${v053_pid}"; v053_rc=$?
wait "${v048_pid}"; v048_rc=$?
set -e
printf "v053=%s\nv048=%s\n" "${v053_rc}" "${v048_rc}" >"${PAIR_ROOT}/return_codes.txt"
test "${v053_rc}" -eq 0
test "${v048_rc}" -eq 0

python3 - "${OUT_ROOT}" "${PAIR_NAME}" <<'PY'
import gzip
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
pair = sys.argv[2]
records = []
for release in ("v053", "v048"):
    matches = sorted((root / "dftracer" / release / pair).glob("**/unified-trace.pfw.gz"))
    if len(matches) != 1:
        raise SystemExit(f"{release}/{pair}: expected one unified trace, found {len(matches)}")
    events = run_match_jobids = 0
    with gzip.open(matches[0], "rt", encoding="utf-8") as stream:
        for raw in stream:
            raw = raw.strip().rstrip(",")
            if raw in ("", "[", "]"):
                continue
            event = json.loads(raw)
            events += 1
            args = event.get("args")
            if event.get("name") == "run_match" and isinstance(args, dict) and "jobid" in args:
                run_match_jobids += 1
    records.append({
        "release": release,
        "trace": str(matches[0]),
        "events": events,
        "run_match_events_with_jobid": run_match_jobids,
    })
if any(record["run_match_events_with_jobid"] == 0 for record in records):
    raise SystemExit("DFTracer validation failed: run_match jobid metadata is missing")
(root / "pairs" / pair / "trace_validation.json").write_text(
    json.dumps({"pair": pair, "traces": records}, indent=2) + "\n",
    encoding="utf-8",
)
PY

echo "accuracy pair completed: ${PAIR_NAME}"
