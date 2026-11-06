#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: worker.sh <campaign-root> <batch-id>" >&2
  exit 64
fi

CAMPAIGN_ROOT="$(cd "$1" && pwd)"
BATCH_ID="$2"
# Stamp job start BEFORE the container image load so the
# walltime budget handed to the simulations counts that time.
FLUX_FICTION_WORKER_EPOCH="${FLUX_FICTION_WORKER_EPOCH:-$(date +%s)}"
WORKSPACE_ROOT="${FLUX_FICTION_WORKSPACE_ROOT:-__WORKSPACE_ROOT__}"
CONTAINER_INSTALLS="${FLUX_FICTION_CONTAINER_INSTALLS:-__WORKSPACE_ROOT__/container-installs}"
IMAGE="${FLUX_FICTION_CONTAINER_IMAGE:-localhost/flux-fiction-dev:latest}"
IMAGE_TAR="${FLUX_FICTION_CONTAINER_IMAGE_TAR:-__WORKSPACE_ROOT__/flux-fiction-dev.tar}"
CONTAINER_PYTHONPATH="${FLUX_FICTION_CONTAINER_PYTHONPATH:-__WORKSPACE_ROOT__/flux-fiction-develop/src}"
SOURCE_ROOT="${FLUX_FICTION_SOURCE_ROOT:-__WORKSPACE_ROOT__/flux-fiction-develop/src}"
REPO_ROOT="${FLUX_FICTION_REPO_ROOT:-__WORKSPACE_ROOT__/flux-fiction-develop}"
DEFAULT_WORKER_RUNTIME="container"
DEFAULT_FLUX_LAUNCH=""
DEFAULT_FLUX_LAUNCH_CORES=""
SPACK_FLUXION_PREFIX="${SPACK_FLUXION_PREFIX:-__SPACK_ROOT__/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/flux-sched-0.53.0-rben2bzg2txikxftd7w34mcio3u6mzdc}"
SPACK_FAKETIME_PREFIX="${SPACK_FAKETIME_PREFIX:-__SPACK_ROOT__/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/faketime-0.9.10-n2lvnlfsett6t3xoh4h4hrfb7ew3bppo}"
SPACK_GCC_RUNTIME="${SPACK_GCC_RUNTIME:-__SPACK_ROOT__/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/gcc-runtime-12.2.0-w244juoi3w2lwcowcvtkm6t5vqu5wd2t}"
SPACK_VIEW="${SPACK_VIEW:-__SPACK_ROOT__/spack/var/spack/environments/emulator/.spack-env/view}"
SPACK_NINJA_PREFIX="${SPACK_NINJA_PREFIX:-__SPACK_ROOT__/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/ninja-1.12.1-buf67bdsc6njmhhy3ipymclhnni74us2}"
MESON_PYTHONPATH="${MESON_PYTHONPATH:-__LUSTRE5_ROOT__/flux-fiction-ensemble-tuo/codex-perf/host-python-deps/meson-1.8.2}"
HOST_JOBTAP_CC="${HOST_JOBTAP_CC:-gcc}"
DEFAULT_HOST_TMPDIR=""
WORKER_RUNTIME="${FLUX_FICTION_WORKER_RUNTIME:-${DEFAULT_WORKER_RUNTIME}}"
if [[ "${WORKER_RUNTIME}" == "podman" ]]; then WORKER_RUNTIME=container; fi
if [[ -z "${FLUX_FICTION_FLUX_LAUNCH+x}" && -n "${DEFAULT_FLUX_LAUNCH}" ]]; then
  export FLUX_FICTION_FLUX_LAUNCH="${DEFAULT_FLUX_LAUNCH}"
fi
if [[ -z "${FLUX_FICTION_FLUX_LAUNCH_CORES+x}" && -n "${DEFAULT_FLUX_LAUNCH_CORES}" ]]; then
  export FLUX_FICTION_FLUX_LAUNCH_CORES="${DEFAULT_FLUX_LAUNCH_CORES}"
fi

RESULT_PATH="${CAMPAIGN_ROOT}/batches/${BATCH_ID}/batch_result.json"
# A stale result from an earlier attempt would make this wrapper exit
# immediately without running anything.
rm -f "${RESULT_PATH}"

result_return_code() {
  python3 - "$RESULT_PATH" <<'PY'
import json
import sys
try:
    data = json.load(open(sys.argv[1], encoding='utf-8'))
    print(int(data.get('return_code', 1)))
except Exception:
    print(1)
PY
}

choose_scratch_root() {
  if [[ -n "${FLUX_FICTION_SCRATCH_HOST_ROOT:-}" ]]; then
    printf '%s\n' "${FLUX_FICTION_SCRATCH_HOST_ROOT}"
  elif [[ -d /l/ssd && -w /l/ssd ]]; then
    printf '%s\n' /l/ssd
  else
    printf '%s\n' /var/tmp
  fi
}

write_spack_modprobe_overlay() {
  local overlay="$1"
  mkdir -p "${overlay}/modprobe.d"
  cat > "${overlay}/modprobe.d/zz-spack-fluxion.toml" <<EOF
[[modules]]
name = "sched-fluxion-resource"
module = "${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-resource.so"
args = ["load-allowlist=node,core,gpu"]
ranks = "0"
after = ["resource"]
requires = ["resource"]
priority = 900

[[modules]]
name = "sched-fluxion-feasibility"
module = "${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-feasibility.so"
args = ["load-allowlist=node,core,gpu"]
provides = ["feasibility"]
ranks = "0"
after = ["sched-fluxion-resource"]
requires = ["sched-fluxion-resource"]
needs = ["sched-fluxion-resource"]
priority = 900

[[modules]]
name = "sched-fluxion-qmanager"
module = "${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-qmanager.so"
provides = ["sched"]
ranks = "0"
after = ["job-manager", "sched-fluxion-resource"]
requires = ["sched-fluxion-resource", "job-manager"]
needs = ["sched-fluxion-resource"]
priority = 900
EOF
}

run_container_worker() {
command -v podman >/dev/null 2>&1 || { echo "podman is required for ensemble workers" >&2; exit 127; }

# Rootless podman needs a runtime dir. Under a bare sbatch there is no
# systemd user session, so /run/user/$UID does not exist and runc falls
# back to /run/runc, which it cannot create (OCI permission denied).
# Flux/srun sessions provide one, hence this only bites the Slurm path.
# NB: XDG_RUNTIME_DIR is often exported but pointing at a directory that
# does not exist, so test the path itself rather than whether it is set.
if [[ ! -d "${XDG_RUNTIME_DIR:-/nonexistent}" ]]; then
  export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"
  mkdir -p "${XDG_RUNTIME_DIR}" || true
  chmod 700 "${XDG_RUNTIME_DIR}" 2>/dev/null || true
  echo "Runtime dir missing; using XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR}"
fi
[[ -d "${WORKSPACE_ROOT}" ]] || { echo "workspace root not found: ${WORKSPACE_ROOT}" >&2; exit 4; }
[[ -d "${CONTAINER_INSTALLS}" ]] || { echo "container installs not found: ${CONTAINER_INSTALLS}" >&2; exit 4; }

# Rootless podman spawns a long-lived pause daemon (catatonit) on first use.
# On nodes without a user systemd session it stays in this job's cgroup and
# inherits its stdio, which keeps the Flux job RUNNING long after this
# script exits. Spawn it up front with stdio detached, and kill it (plus
# other podman helpers) in cleanup; the node is exclusively ours.
podman info >/dev/null 2>&1 </dev/null || true

if ! podman image exists "${IMAGE}" </dev/null; then
  [[ -f "${IMAGE_TAR}" ]] || { echo "container image is missing and tar was not found: ${IMAGE_TAR}" >&2; exit 3; }
  podman load -i "${IMAGE_TAR}" </dev/null
fi

# Pick a REAL node-local disk for the broker rundir/KVS. On these
# compute nodes /var/tmp is tmpfs, i.e. RAM -- a 20k-job run puts
# GBs of content.sqlite there and it counts against node memory.
# /l/ssd is node-local NVMe; fall back to /var/tmp where absent.
SCRATCH_ROOT="$(choose_scratch_root)"
echo "Scratch root (broker rundir + KVS): ${SCRATCH_ROOT}"
SCRATCH_HOST="$(mktemp -d "${SCRATCH_ROOT}/ffensemble.XXXXXX")"
CONTAINER_NAME="ffe-${BATCH_ID}-$(date +%s)-$$"
WAIT_PATH="${SCRATCH_HOST}/container.exit"
WAIT_ERR_PATH="${SCRATCH_HOST}/container.wait.err"
cleanup() {
  for pid in "${LOGS_PID:-}" "${WAIT_PID:-}"; do
    [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
  done
  if [[ -n "${CONTAINER_ID:-}" ]]; then
    podman rm -f -t 5 "${CONTAINER_ID}" >/dev/null 2>&1 </dev/null || true
  fi
  for pid in "${LOGS_PID:-}" "${WAIT_PID:-}"; do
    [[ -n "${pid}" ]] && kill -9 "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
  pkill -u "$(id -un)" -x conmon 2>/dev/null || true
  pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
  rm -rf "${SCRATCH_HOST}" 2>/dev/null || true
}
trap cleanup EXIT

CONTAINER_ID="$(podman run -d --rm --name "${CONTAINER_NAME}" --pull=never \
  -e MPLBACKEND=Agg \
  -e PYTHONPATH="${CONTAINER_PYTHONPATH}" \
  -e FLUX_FICTION_FAKETIME_DIR=/dev/shm \
  -e FLUX_FICTION_FLUX_LAUNCH="${FLUX_FICTION_FLUX_LAUNCH:-}" \
  -e FLUX_FICTION_FLUX_LAUNCH_CORES="${FLUX_FICTION_FLUX_LAUNCH_CORES:-}" \
  -e FLUX_FICTION_SCRATCH=/scratch \
  -e TMPDIR=/scratch \
  -e FLUX_FICTION_WORKER_EPOCH="${FLUX_FICTION_WORKER_EPOCH}" \
  --shm-size=1g \
  --stop-timeout=90 \
  -v "${SCRATCH_HOST}:/scratch" \
  -v "${CONTAINER_INSTALLS}:/workspace/container-installs:ro" \
  -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:rw" \
  -v "${CAMPAIGN_ROOT}:${CAMPAIGN_ROOT}:rw" \
  "${IMAGE}" \
  bash -lc 'if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then source /usr/local/bin/flux-dev-env.sh; fi; exec python3 -m flux_fiction_ensemble worker "$@"' _ "${CAMPAIGN_ROOT}" "${BATCH_ID}" </dev/null)"
echo "Started container ${CONTAINER_NAME} (${CONTAINER_ID})"
podman logs -f "${CONTAINER_ID}" &
LOGS_PID="$!"
podman wait "${CONTAINER_ID}" >"${WAIT_PATH}" 2>"${WAIT_ERR_PATH}" &
WAIT_PID="$!"

container_exit_code() {
  tail -n 1 "${WAIT_PATH}" 2>/dev/null | tr -cd '0-9' || true
}

while true; do
  if [[ -s "${RESULT_PATH}" ]]; then
    sleep "${FLUX_FICTION_WORKER_RESULT_GRACE_SECONDS:-5}"
    rc="$(result_return_code)"
    if kill -0 "${WAIT_PID}" 2>/dev/null; then
      podman stop -t "${FLUX_FICTION_WORKER_STOP_SECONDS:-30}" "${CONTAINER_ID}" >/dev/null 2>&1 || true
      wait "${WAIT_PID}" >/dev/null 2>&1 || true
    fi
    echo "Observed ${RESULT_PATH}; exiting worker wrapper rc=${rc}"
    exit "${rc}"
  fi
  if ! kill -0 "${WAIT_PID}" 2>/dev/null; then
    wait "${WAIT_PID}" >/dev/null 2>&1 || true
    if [[ -s "${RESULT_PATH}" ]]; then
      rc="$(result_return_code)"
      echo "Observed ${RESULT_PATH} after container exit; wrapper rc=${rc}"
      exit "${rc}"
    fi
    rc="$(container_exit_code)"
    [[ -n "${rc}" && "${rc}" != "0" ]] || rc=1
    echo "Container ${CONTAINER_NAME} (${CONTAINER_ID}) exited before writing ${RESULT_PATH}; wrapper rc=${rc}" >&2
    if [[ -s "${WAIT_ERR_PATH}" ]]; then cat "${WAIT_ERR_PATH}" >&2; fi
    exit "${rc}"
  fi
  sleep 2
done
}

run_spack_worker() {
  command -v flux >/dev/null 2>&1 || { echo "flux is required for Spack ensemble workers" >&2; exit 127; }
  command -v python3 >/dev/null 2>&1 || { echo "python3 is required for Spack ensemble workers" >&2; exit 127; }
  [[ -d "${SOURCE_ROOT}" ]] || { echo "source root not found: ${SOURCE_ROOT}" >&2; exit 4; }
  [[ -d "${REPO_ROOT}" ]] || { echo "repo root not found: ${REPO_ROOT}" >&2; exit 4; }
  [[ -d "${SPACK_FLUXION_PREFIX}" ]] || { echo "Spack Fluxion prefix not found: ${SPACK_FLUXION_PREFIX}" >&2; exit 4; }
  [[ -d "${SPACK_FAKETIME_PREFIX}" ]] || { echo "Spack faketime prefix not found: ${SPACK_FAKETIME_PREFIX}" >&2; exit 4; }
  [[ -d "${SPACK_GCC_RUNTIME}" ]] || { echo "Spack GCC runtime prefix not found: ${SPACK_GCC_RUNTIME}" >&2; exit 4; }
  [[ -d "${SPACK_VIEW}" ]] || { echo "Spack view not found: ${SPACK_VIEW}" >&2; exit 4; }
  [[ -d "${SPACK_NINJA_PREFIX}" ]] || { echo "Spack ninja prefix not found: ${SPACK_NINJA_PREFIX}" >&2; exit 4; }

  SCRATCH_ROOT="$(choose_scratch_root)"
  echo "Scratch root (host Spack runtime): ${SCRATCH_ROOT}"
  SCRATCH_HOST="$(mktemp -d "${SCRATCH_ROOT}/ffensemble-spack.XXXXXX")"
  cleanup() {
    pkill -9 -f flux-broker 2>/dev/null || true
    pkill -9 -f flux-shell 2>/dev/null || true
    pkill -9 -f flux-imp 2>/dev/null || true
    rm -rf "${SCRATCH_HOST}" 2>/dev/null || true
  }
  trap cleanup EXIT

  export PYTHONPATH="/usr/lib64/flux/python3.6:${SOURCE_ROOT}:${PYTHONPATH:-}"
  export MPLBACKEND=Agg
  export FLUX_FICTION_WORKSPACE_ROOT="${WORKSPACE_ROOT}"
  export FLUX_FICTION_FAKETIME_DIR="${FLUX_FICTION_FAKETIME_DIR:-/dev/shm}"
  if [[ -n "${FLUX_FICTION_HOST_TMPDIR:-}" ]]; then
    export TMPDIR="${FLUX_FICTION_HOST_TMPDIR}"
  elif [[ -n "${DEFAULT_HOST_TMPDIR}" ]]; then
    export TMPDIR="${DEFAULT_HOST_TMPDIR}"
  else
    export TMPDIR="${SCRATCH_HOST}"
  fi
  mkdir -p "${TMPDIR}"
  export FLUX_PREFIX="${FLUX_PREFIX:-/nonexistent-flux-prefix}"
  export FAKETIME_LIB="${FAKETIME_LIB:-${SPACK_FAKETIME_PREFIX}/lib/faketime/libfaketimeMT.so.1}"
  [[ -f "${FAKETIME_LIB}" ]] || { echo "faketime library not found: ${FAKETIME_LIB}" >&2; exit 4; }

  SPACK_MODPROBE_OVERLAY="${SCRATCH_HOST}/spack-modprobe"
  write_spack_modprobe_overlay "${SPACK_MODPROBE_OVERLAY}"
  if [[ -n "${FLUX_MODPROBE_PATH_APPEND:-}" ]]; then
    export FLUX_MODPROBE_PATH_APPEND="${SPACK_MODPROBE_OVERLAY}:${FLUX_MODPROBE_PATH_APPEND}"
  else
    export FLUX_MODPROBE_PATH_APPEND="${SPACK_MODPROBE_OVERLAY}"
  fi
  export FLUX_FICTION_FLUXION_RESOURCE_MODULE="${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-resource.so"
  export FLUX_FICTION_FLUXION_FEASIBILITY_MODULE="${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-feasibility.so"
  export FLUX_FICTION_FLUXION_QMANAGER_MODULE="${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-qmanager.so"
  BASE_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
  export LD_LIBRARY_PATH="${SPACK_GCC_RUNTIME}/lib:${SPACK_FLUXION_PREFIX}/lib64:${SPACK_FLUXION_PREFIX}/lib:${BASE_LD_LIBRARY_PATH}"

  if [[ -z "${FLUX_FICTION_JOBTAP_SO:-}" ]]; then
    BUILD_DIR="${SCRATCH_HOST}/build-host-jobtap"
    INSTALL_DIR="${SCRATCH_HOST}/install-host-jobtap"
    rm -rf "${BUILD_DIR}" "${INSTALL_DIR}"
    mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}"
    env \
      CC="${HOST_JOBTAP_CC}" \
      PYTHONPATH="${MESON_PYTHONPATH}:${PYTHONPATH:-}" \
      PATH="${SPACK_NINJA_PREFIX}/bin:${PATH}" \
      python3 -m mesonbuild.mesonmain setup "${BUILD_DIR}" "${REPO_ROOT}" \
        -Duse_system_flux=true \
        -Dflux_prefix=/usr \
        --prefix "${INSTALL_DIR}"
    env \
      CC="${HOST_JOBTAP_CC}" \
      PYTHONPATH="${MESON_PYTHONPATH}:${PYTHONPATH:-}" \
      PATH="${SPACK_NINJA_PREFIX}/bin:${PATH}" \
      python3 -m mesonbuild.mesonmain compile -C "${BUILD_DIR}"
    export FLUX_FICTION_JOBTAP_SO="${BUILD_DIR}/src/emu-jobtap.so"
  fi
  [[ -f "${FLUX_FICTION_JOBTAP_SO}" ]] || { echo "jobtap plugin not found: ${FLUX_FICTION_JOBTAP_SO}" >&2; exit 4; }

  echo "Worker runtime: spack"
  echo "Spack Fluxion: ${SPACK_FLUXION_PREFIX}"
  echo "Flux pinning: ${FLUX_FICTION_FLUX_LAUNCH:-0} cores=${FLUX_FICTION_FLUX_LAUNCH_CORES:-auto}"
  echo "TMPDIR: ${TMPDIR}"
  # Host workers inherit the system Flux job's FLUX_URI. Clear it so
  # run_ff_parallel creates a node-local flux start instance for pinning
  # instead of submitting the 16 replicas back to the system scheduler.
  unset FLUX_URI FLUX_JOB_ID FLUX_KVS_NAMESPACE FLUX_INSTANCE_LEVEL
  python3 -m flux_fiction_ensemble worker "${CAMPAIGN_ROOT}" "${BATCH_ID}"
}

case "${WORKER_RUNTIME}" in
  container)
    run_container_worker
    ;;
  spack)
    run_spack_worker
    ;;
  *)
    echo "unknown worker runtime: ${WORKER_RUNTIME} (expected container or spack)" >&2
    exit 64
    ;;
esac
