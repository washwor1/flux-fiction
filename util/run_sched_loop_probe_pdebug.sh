#!/usr/bin/env bash
set -euo pipefail

PROBE_ROOT="${PROBE_ROOT:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-probe}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-/usr/WS1/ashworth12/ff-podman}"
REPO="${REPO:-${WORKSPACE_ROOT}/flux-fiction-develop}"
RESOURCE_R="${RESOURCE_R:-${WORKSPACE_ROOT}/resource_graphs/tuolumne.json}"
IMAGE="${FLUX_FICTION_CONTAINER_IMAGE:-localhost/flux-fiction-dev:latest}"
IMAGE_TAR="${FLUX_FICTION_CONTAINER_IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev.tar}"
CONTAINER_INSTALLS="${FLUX_FICTION_CONTAINER_INSTALLS:-${WORKSPACE_ROOT}/container-installs}"
JOB_LIMIT="${JOB_LIMIT:-50}"
VANILLA_STEP_TIMEOUT="${VANILLA_STEP_TIMEOUT:-420s}"
VANILLA_MATCH_TIMEOUT="${VANILLA_MATCH_TIMEOUT:-120}"
MATCH_FORMATS="${MATCH_FORMATS:-rv1_shorthand rv1_nosched}"
FF_WALLTIME_BUDGET_SECONDS="${FF_WALLTIME_BUDGET_SECONDS:-900}"
FF_FINALIZE_RESERVE_SECONDS="${FF_FINALIZE_RESERVE_SECONDS:-90}"

ARTIFACTS="${PROBE_ROOT}/artifacts/$(date +%Y%m%d_%H%M%S)"
mkdir -p "${ARTIFACTS}"
echo "${ARTIFACTS}" > "${PROBE_ROOT}/latest_artifacts.txt"

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

run_logged() {
  local name="$1"
  shift
  log "START ${name}"
  set +e
  "$@" >"${ARTIFACTS}/${name}.log" 2>&1
  local rc=$?
  set -e
  log "END ${name} rc=${rc}"
  return "${rc}"
}

FAILURES=0
run_step() {
  if ! run_logged "$@"; then
    FAILURES=$((FAILURES + 1))
  fi
}

log "probe root: ${PROBE_ROOT}"
log "artifacts: ${ARTIFACTS}"
log "host: $(hostname)"
log "flux: $(flux --version | tr '\n' '; ')"

run_step env env
run_step flux_module_list flux module list

if [[ "${SKIP_VANILLA:-0}" != "1" ]]; then
  for match_format in ${MATCH_FORMATS}; do
    run_step "vanilla_${match_format}" \
      timeout "${VANILLA_STEP_TIMEOUT}" \
      flux start -s 1 \
        --setattr="log-filename=${ARTIFACTS}/vanilla_${match_format}_broker.log" \
        -- \
        bash -lc "cd '${REPO}' && PYTHONPATH=src exec flux python util/sched_loop_probe_vanilla.py --resource-r '${RESOURCE_R}' --scheduler-json '${PROBE_ROOT}/configs/scheduler_${match_format}.json' --jobspec-dir '${PROBE_ROOT}/jobspecs' --out '${ARTIFACTS}/vanilla_${match_format}.json' --label 'vanilla_${match_format}' --limit '${JOB_LIMIT}' --per-match-timeout '${VANILLA_MATCH_TIMEOUT}'"
  done
else
  log "SKIP_VANILLA=1; skipping direct Fluxion resource-match probes"
fi

command -v podman >/dev/null 2>&1 || {
  log "podman is required for Flux Fiction container probes"
  exit 127
}

if [[ ! -d "${XDG_RUNTIME_DIR:-/nonexistent}" ]]; then
  export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"
  mkdir -p "${XDG_RUNTIME_DIR}" || true
  chmod 700 "${XDG_RUNTIME_DIR}" 2>/dev/null || true
  log "using XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR}"
fi

podman info >/dev/null 2>&1 </dev/null || true
if ! podman image exists "${IMAGE}" </dev/null; then
  [[ -f "${IMAGE_TAR}" ]] || {
    log "container image is missing and tar was not found: ${IMAGE_TAR}"
    exit 3
  }
  run_step podman_load podman load -i "${IMAGE_TAR}"
fi

SCRATCH_ROOT="${FLUX_FICTION_SCRATCH_HOST_ROOT:-}"
if [[ -z "${SCRATCH_ROOT}" ]]; then
  if [[ -d /l/ssd && -w /l/ssd ]]; then
    SCRATCH_ROOT=/l/ssd
  else
    SCRATCH_ROOT=/var/tmp
  fi
fi
SCRATCH_HOST="$(mktemp -d "${SCRATCH_ROOT}/ff-sched-loop.XXXXXX")"
cleanup() {
  rm -rf "${SCRATCH_HOST}" 2>/dev/null || true
  pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
  pkill -u "$(id -un)" -x conmon 2>/dev/null || true
  pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
}
trap cleanup EXIT

run_ff_container() {
  local match_format="$1"
  local run_dir="${ARTIFACTS}/ff_${match_format}"
  run_step "ff_${match_format}" \
    podman run --rm --pull=never \
      -e MPLBACKEND=Agg \
      -e PYTHONPATH="${REPO}/src" \
      -e FLUX_FICTION_FAKETIME_DIR=/dev/shm \
      -e FLUX_FICTION_SCRATCH=/scratch \
      -e TMPDIR=/scratch \
      -e DFTRACER_ENABLE=1 \
      -e DFTRACER_TIME_METRIC=NS \
      -e DFTRACER_INC_METADATA=1 \
      --shm-size=1g \
      -v "${SCRATCH_HOST}:/scratch" \
      -v "${CONTAINER_INSTALLS}:/workspace/container-installs:ro" \
      -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:rw" \
      -v "${PROBE_ROOT}:${PROBE_ROOT}:rw" \
      "${IMAGE}" \
      bash -lc "if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then source /usr/local/bin/flux-dev-env.sh; fi; cd '${REPO}' && exec python3 -m flux_fiction.cli.run_ff '${PROBE_ROOT}/configs/flux_fiction_${match_format}.toml' --run-dir '${run_dir}' --walltime-budget-seconds '${FF_WALLTIME_BUDGET_SECONDS}' --finalize-reserve-seconds '${FF_FINALIZE_RESERVE_SECONDS}' --broker-log-level 3 --no-broker-log-file --faketime-start-lead 30"
}

if [[ "${SKIP_FF:-0}" != "1" ]]; then
  for match_format in ${MATCH_FORMATS}; do
    run_ff_container "${match_format}"
  done
else
  log "SKIP_FF=1; skipping Flux Fiction container probes"
fi

run_step artifact_listing find "${ARTIFACTS}" -maxdepth 4 -type f
log "complete: ${ARTIFACTS}"
if [[ "${FAILURES}" -gt 0 ]]; then
  log "probe completed with ${FAILURES} failed step(s)"
  exit 1
fi
