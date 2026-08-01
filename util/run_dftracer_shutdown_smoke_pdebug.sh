#!/usr/bin/env bash
# Exercise a complete 500-job run against an already-built Fluxion install.
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/g/g14/ashworth12/workspace/ff-podman}"
FF_SOURCE="${FF_SOURCE:-${WORKSPACE_ROOT}/flux-fiction-shared-clock}"
GAUSSIAN_TRACE="${GAUSSIAN_TRACE:-${WORKSPACE_ROOT}/salishan_runs/experiment_tuolumne_easy_gaussian_random/processed/input_with_latencies_patched.csv}"
PATCHED_CORE_PREFIX="${PATCHED_CORE_PREFIX:-/p/lustre5/ashworth12/flux-core-nodeonly.l5WGzh/prefix}"
SCHEDULER_PREFIX="${SCHEDULER_PREFIX:-/p/lustre5/ashworth12/dftracer-gaussian-backfill-rd4-correct-20260730_043831/installs-skip-empty-dfu-expansion/flux-sched}"
IMAGE="${IMAGE:-localhost/flux-fiction-dev-dft:latest}"
IMAGE_TAR="${IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev-dft.tar}"
: "${OUT_ROOT:?set OUT_ROOT to a new output directory}"

[[ ! -e "${OUT_ROOT}" ]] || { echo "refusing to reuse ${OUT_ROOT}" >&2; exit 2; }
mkdir -p "${OUT_ROOT}"

if [[ ! -d "${XDG_RUNTIME_DIR:-}" ]]; then
    export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"
    mkdir -p "${XDG_RUNTIME_DIR}"
    chmod 700 "${XDG_RUNTIME_DIR}"
fi
podman info >/dev/null 2>&1 </dev/null || true
podman image exists "${IMAGE}" </dev/null || podman load -i "${IMAGE_TAR}" </dev/null

scratch_host="$(mktemp -d /var/tmp/ff-dft-shutdown-smoke.XXXXXX)"
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

podman run --rm --pull=never \
    --shm-size=2g \
    -v "${PATCHED_CORE_PREFIX}:/workspace/container-installs/flux-core:ro" \
    -v "${PATCHED_CORE_PREFIX}:/test-root/prefix:ro" \
    -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:ro" \
    -v "${SCHEDULER_PREFIX}:${SCHEDULER_PREFIX}:ro" \
    -v "${OUT_ROOT}:${OUT_ROOT}:rw" \
    -v "${scratch_host}:/scratch" \
    -e PYTHONPATH="${FF_SOURCE}/src" \
    -e FAKETIME_LIB="${WORKSPACE_ROOT}/libfaketime-shared-clock/src/libfaketimeMT.so.1" \
    -e FLUX_FICTION_FAKETIME_MODE=shared \
    -e FLUX_FICTION_NO_BROKER_LOG_FILE=true \
    -e FLUX_CONF_DIR="${FF_SOURCE}/util/dftracer_no_ingest.toml" \
    -e FLUX_FICTION_JOBTAP_SO="${FF_SOURCE}/build/cp312/src/emu-jobtap.so" \
    -e FLUX_FICTION_FLUXION_RESOURCE_MODULE="${SCHEDULER_PREFIX}/lib/flux/modules/sched-fluxion-resource.so" \
    -e FLUX_FICTION_FLUXION_FEASIBILITY_MODULE="${SCHEDULER_PREFIX}/lib/flux/modules/sched-fluxion-feasibility.so" \
    -e FLUX_FICTION_FLUXION_QMANAGER_MODULE="${SCHEDULER_PREFIX}/lib/flux/modules/sched-fluxion-qmanager.so" \
    -e DFTRACER_ENABLE=1 \
    -e DFTRACER_TIME_METRIC=NS \
    -e DFTRACER_INC_METADATA=1 \
    -e DFTRACER_DISABLE_IO=1 \
    -e DFTRACER_FLUXION_FINALIZE_GENERATION=0 \
    -e DFTRACER_DATA_DIR="${OUT_ROOT}/dftracer" \
    -e DFTRACER_LOG_FILE="${OUT_ROOT}/dftracer/fluxion" \
    -e NO_FAKE_STAT=1 \
    -e SMOKE_CONFIG="${OUT_ROOT}/matrix/configs/skip-empty-dfu-expansion-easy-no-cores.toml" \
    -e SMOKE_RUN="${OUT_ROOT}/run" \
    "${IMAGE}" bash -lc '
        set -euo pipefail
        source /usr/local/bin/flux-dev-env.sh
        export LD_LIBRARY_PATH="/workspace/container-installs/flux-core/lib:${LD_LIBRARY_PATH:-}"
        python3 -m flux_fiction.cli.run_ff \
            "$SMOKE_CONFIG" \
            --run-dir "$SMOKE_RUN" \
            --submit-novalidate \
            --no-broker-log-file
    ' >"${OUT_ROOT}/smoke.log" 2>&1

python3 - "${OUT_ROOT}" <<'PY'
import json
import gzip
import sys
from pathlib import Path

root = Path(sys.argv[1])
status = json.loads((root / "run" / "status.json").read_text())
if status.get("state") != "succeeded" or status.get("return_code") != 0:
    raise SystemExit(f"unexpected run status: {status}")
if status.get("dftracer_merged_events", 0) <= 0:
    raise SystemExit(f"DFTracer emitted no events: {status}")
trace = Path(status["dftracer_merged_trace"])
run_match_with_jobid = 0
with gzip.open(trace, "rt", encoding="utf-8") as stream:
    for line in stream:
        line = line.strip().rstrip(",")
        if line in ("", "[", "]"):
            continue
        event = json.loads(line)
        args = event.get("args")
        if event.get("name") == "run_match" and isinstance(args, dict) and "jobid" in args:
            run_match_with_jobid += 1
if run_match_with_jobid <= 0:
    raise SystemExit(f"no run_match events have jobid metadata in {trace}")
text = (root / "smoke.log").read_text(errors="replace")
bad = [needle for needle in ("not properly shut down", "terminate called") if needle in text]
if bad:
    raise SystemExit(f"shutdown failure remains: {bad}")
print(f"run_match events with jobid: {run_match_with_jobid}")
print(json.dumps(status, indent=2, sort_keys=True))
PY
