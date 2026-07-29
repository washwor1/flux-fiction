#!/usr/bin/env bash
set -euo pipefail

PROBE_ROOT="${PROBE_ROOT:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-direct-faketime}"
SOURCE_PROBE_ROOT="${SOURCE_PROBE_ROOT:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-probe}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-/usr/WS1/ashworth12/ff-podman}"
REPO="${REPO:-${WORKSPACE_ROOT}/flux-fiction-develop}"
RESOURCE_R="${RESOURCE_R:-${WORKSPACE_ROOT}/resource_graphs/tuolumne.json}"
IMAGE="${FLUX_FICTION_CONTAINER_IMAGE:-localhost/flux-fiction-dev:latest}"
IMAGE_TAR="${FLUX_FICTION_CONTAINER_IMAGE_TAR:-${WORKSPACE_ROOT}/flux-fiction-dev.tar}"
CONTAINER_INSTALLS="${FLUX_FICTION_CONTAINER_INSTALLS:-${WORKSPACE_ROOT}/container-installs}"
JOB_LIMIT="${JOB_LIMIT:-50}"
VANILLA_MATCH_TIMEOUT="${VANILLA_MATCH_TIMEOUT:-120}"
FAKETIME_JUMP_SECONDS="${FAKETIME_JUMP_SECONDS:-3600}"
MATCH_FORMATS="${MATCH_FORMATS:-rv1_shorthand}"
RPC_MODES="${RPC_MODES:-nofake static jump}"
RESOURCE_QUERY_FORMATS="${RESOURCE_QUERY_FORMATS:-rv1 rv1_nosched}"
RESOURCE_QUERY_MODES="${RESOURCE_QUERY_MODES:-nofake static}"
RUN_RPC_PROBES="${RUN_RPC_PROBES:-1}"
RUN_RESOURCE_QUERY_PROBES="${RUN_RESOURCE_QUERY_PROBES:-1}"

ARTIFACTS="${PROBE_ROOT}/artifacts/$(date +%Y%m%d_%H%M%S)"
mkdir -p "${ARTIFACTS}" "${PROBE_ROOT}"
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

copy_probe_inputs() {
  for path in configs jobspecs manifest.json queueb_gaussian_rabbit.csv resource_query_commands.in; do
    if [[ ! -e "${PROBE_ROOT}/${path}" ]]; then
      cp -a "${SOURCE_PROBE_ROOT}/${path}" "${PROBE_ROOT}/${path}"
    fi
  done
}

write_limited_resource_query_commands() {
  local out="${ARTIFACTS}/resource_query_${JOB_LIMIT}.in"
  find "${PROBE_ROOT}/jobspecs" -maxdepth 1 -name 'job_*.json' -print \
    | sort \
    | head -n "${JOB_LIMIT}" \
    | awk '{print "match allocate_orelse_reserve " $0}' > "${out}"
  {
    echo "stat"
    echo "quit"
  } >> "${out}"
  echo "${out}"
}

ensure_container_image() {
  command -v podman >/dev/null 2>&1 || {
    log "podman is required for container probes"
    return 127
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
      return 3
    }
    run_step podman_load podman load -i "${IMAGE_TAR}"
  fi
}

container_run() {
  local name="$1"
  shift
  run_step "${name}" \
    podman run --rm --pull=never \
      -e MPLBACKEND=Agg \
      -e PYTHONPATH="${REPO}/src" \
      -e FLUX_FICTION_FAKETIME_DIR=/dev/shm \
      -e TMPDIR=/scratch \
      --shm-size=1g \
      -v "${SCRATCH_HOST}:/scratch" \
      -v "${CONTAINER_INSTALLS}:/workspace/container-installs:ro" \
      -v "${WORKSPACE_ROOT}:${WORKSPACE_ROOT}:rw" \
      -v "${PROBE_ROOT}:${PROBE_ROOT}:rw" \
      "${IMAGE}" "$@"
}

write_rpc_probe_script() {
  local name="$1"
  local mode="$2"
  local match_format="$3"
  local script="${ARTIFACTS}/container_${name}.sh"
  cat > "${script}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then
  source /usr/local/bin/flux-dev-env.sh
fi
cd "${REPO}"
export PYTHONPATH="${REPO}/src"
faketime_lib="\${FAKETIME_LIB:-/usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1}"
stamp="/dev/shm/${name}.stamp"
time_file="${ARTIFACTS}/${name}.time"
out_json="${ARTIFACTS}/${name}.json"
broker_log="${ARTIFACTS}/${name}_broker.log"
probe_args=(
  util/sched_loop_probe_vanilla.py
  --resource-r "${RESOURCE_R}"
  --scheduler-json "${PROBE_ROOT}/configs/scheduler_${match_format}.json"
  --jobspec-dir "${PROBE_ROOT}/jobspecs"
  --out "\${out_json}"
  --label "${name}"
  --limit "${JOB_LIMIT}"
  --per-match-timeout "${VANILLA_MATCH_TIMEOUT}"
)
if [[ "${mode}" == "jump" ]]; then
  probe_args+=(
    --faketime-stamp-file "\${stamp}"
    --faketime-jump-seconds "${FAKETIME_JUMP_SECONDS}"
  )
fi
if [[ "${mode}" == "nofake" ]]; then
  /usr/bin/time -p -o "\${time_file}" \\
    flux start -s 1 \\
      --setattr="log-filename=\${broker_log}" \\
      -- \\
      flux python "\${probe_args[@]}"
else
  echo "+0.000000000s" > "\${stamp}"
  /usr/bin/time -p -o "\${time_file}" \\
    env \\
      FLUX_LOAD_WITH_DEEPBIND=0 \\
      LD_PRELOAD="\${faketime_lib}" \\
      FAKETIME_TIMESTAMP_FILE="\${stamp}" \\
      FAKETIME_NO_CACHE=1 \\
      FAKETIME_DONT_FAKE_MONOTONIC=1 \\
      flux start -s 1 \\
        --setattr="log-filename=\${broker_log}" \\
        -- \\
        flux python "\${probe_args[@]}"
fi
EOF
  chmod +x "${script}"
  echo "${script}"
}

write_resource_query_script() {
  local name="$1"
  local mode="$2"
  local rq_format="$3"
  local commands_file="$4"
  local script="${ARTIFACTS}/container_${name}.sh"
  cat > "${script}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then
  source /usr/local/bin/flux-dev-env.sh
fi
rq="\${RESOURCE_QUERY:-}"
if [[ -z "\${rq}" ]]; then
  rq="\$(command -v resource-query || true)"
fi
if [[ -z "\${rq}" && -x "${WORKSPACE_ROOT}/flux-sched/build/resource/utilities/resource-query" ]]; then
  rq="${WORKSPACE_ROOT}/flux-sched/build/resource/utilities/resource-query"
fi
if [[ -z "\${rq}" ]]; then
  echo "resource-query not found" >&2
  exit 127
fi
faketime_lib="\${FAKETIME_LIB:-/usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1}"
stamp="/dev/shm/${name}.stamp"
time_file="${ARTIFACTS}/${name}.time"
out_log="${ARTIFACTS}/${name}.resource_query.log"
out_json="${ARTIFACTS}/${name}.resource_query.json"
rq_resource="${ARTIFACTS}/${name}.resource.normalized.json"
python3 - "${RESOURCE_R}" "\${rq_resource}" <<'PY'
import json
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
data = json.loads(src.read_text())
scheduling = data.get("scheduling")
graph = scheduling.get("graph") if isinstance(scheduling, dict) else data.get("graph")
nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
for node in nodes:
    meta = node.get("metadata")
    if isinstance(meta, dict) and meta.get("type") == "ssd" and meta.get("status") == 1:
        meta["status"] = 0
dst.write_text(json.dumps({"graph": graph}, sort_keys=True) + "\\n")
PY
cmd=(
  "\${rq}"
  -L "\${rq_resource}"
  -f jgf
  -S CA
  -P lonodex
  -F "${rq_format}"
  -p ALL:core,ALL:node,ALL:gpu
  -r 200000
  -e
  -d
)
if [[ "${mode}" == "nofake" ]]; then
  /usr/bin/time -p -o "\${time_file}" "\${cmd[@]}" < "${commands_file}" > "\${out_log}" 2>&1
else
  echo "+0.000000000s" > "\${stamp}"
  /usr/bin/time -p -o "\${time_file}" \\
    env \\
      FLUX_LOAD_WITH_DEEPBIND=0 \\
      LD_PRELOAD="\${faketime_lib}" \\
      FAKETIME_TIMESTAMP_FILE="\${stamp}" \\
      FAKETIME_NO_CACHE=1 \\
      FAKETIME_DONT_FAKE_MONOTONIC=1 \\
      "\${cmd[@]}" < "${commands_file}" > "\${out_log}" 2>&1
fi
python3 - "\${out_log}" "\${time_file}" "\${out_json}" "${name}" "${rq_format}" "${mode}" <<'PY'
import json
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
time_path = Path(sys.argv[2])
out_path = Path(sys.argv[3])
label = sys.argv[4]
match_format = sys.argv[5]
mode = sys.argv[6]
text = log_path.read_text(errors="replace")
elapsed = [float(x) for x in re.findall(r"INFO:\\s+ELAPSE=([0-9.]+)", text)]
resources = {
    "allocated": len(re.findall(r"INFO:\\s+RESOURCES=ALLOCATED", text)),
    "reserved": len(re.findall(r"INFO:\\s+RESOURCES=RESERVED", text)),
}
time_data = {}
if time_path.exists():
    for line in time_path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                time_data[parts[0]] = float(parts[1])
            except ValueError:
                pass
payload = {
    "label": label,
    "mode": mode,
    "match_format": match_format,
    "log": str(log_path),
    "time_file": str(time_path),
    "jobs_measured": len(elapsed),
    "resource_counts": resources,
    "external_time": time_data,
    "internal_elapsed_summary": {
        "count": len(elapsed),
        "min_seconds": min(elapsed) if elapsed else None,
        "max_seconds": max(elapsed) if elapsed else None,
        "avg_seconds": sum(elapsed) / len(elapsed) if elapsed else None,
    },
}
out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\\n")
print(json.dumps(payload, indent=2, sort_keys=True))
PY
EOF
  chmod +x "${script}"
  echo "${script}"
}

log "probe root: ${PROBE_ROOT}"
log "artifacts: ${ARTIFACTS}"
log "host: $(hostname)"
log "flux: $(flux --version | tr '\n' '; ')"
copy_probe_inputs
rq_commands="$(write_limited_resource_query_commands)"
run_step env env

ensure_container_image

SCRATCH_ROOT="${FLUX_FICTION_SCRATCH_HOST_ROOT:-}"
if [[ -z "${SCRATCH_ROOT}" ]]; then
  if [[ -d /l/ssd && -w /l/ssd ]]; then
    SCRATCH_ROOT=/l/ssd
  else
    SCRATCH_ROOT=/var/tmp
  fi
fi
SCRATCH_HOST="$(mktemp -d "${SCRATCH_ROOT}/ff-direct-faketime.XXXXXX")"
cleanup() {
  rm -rf "${SCRATCH_HOST}" 2>/dev/null || true
  pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
  pkill -u "$(id -un)" -x conmon 2>/dev/null || true
  pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
}
trap cleanup EXIT

container_run container_capabilities bash -lc "
  set -euo pipefail
  if [[ -f /usr/local/bin/flux-dev-env.sh ]]; then source /usr/local/bin/flux-dev-env.sh; fi
  command -v flux
  flux --version
  command -v resource-query || true
  python3 - <<'PY'
import importlib.util
import json
import sys
mods = ['flux', 'dftracer', 'dftracer.python']
found = {}
for mod in mods:
    try:
        found[mod] = bool(importlib.util.find_spec(mod))
    except ModuleNotFoundError:
        found[mod] = False
print(json.dumps(found, sort_keys=True))
print(sys.version)
PY
"

container_run dftracer_smoke bash -lc "
  set -euo pipefail
  mkdir -p '${ARTIFACTS}/dftracer_smoke'
  export DFTRACER_ENABLE=1
  export DFTRACER_LOG_FILE='${ARTIFACTS}/dftracer_smoke/smoke'
  export DFTRACER_TIME_METRIC=NS
  export DFTRACER_INC_METADATA=1
  python3 - <<'PY'
try:
    from dftracer.python import dftracer
    dftracer.initialize_log()
    log = dftracer.get_instance()
    log.log_metadata_event('probe', 'direct-faketime-smoke')
    log.log_event(name='smoke_event', cat='probe', start_time=1, duration=1)
    log.finalize()
    print('dftracer python smoke ok')
except Exception as exc:
    print('dftracer python smoke failed:', repr(exc))
PY
  find '${ARTIFACTS}/dftracer_smoke' -maxdepth 2 -type f -print | sort
"

if [[ "${RUN_RPC_PROBES}" == "1" ]]; then
  for match_format in ${MATCH_FORMATS}; do
    for mode in ${RPC_MODES}; do
      name="container_rpc_${match_format}_${mode}"
      script="$(write_rpc_probe_script "${name}" "${mode}" "${match_format}")"
      container_run "${name}" bash "${script}"
    done
  done
else
  log "RUN_RPC_PROBES=${RUN_RPC_PROBES}; skipping direct RPC probes"
fi

if [[ "${RUN_RESOURCE_QUERY_PROBES}" == "1" ]]; then
  for rq_format in ${RESOURCE_QUERY_FORMATS}; do
    for mode in ${RESOURCE_QUERY_MODES}; do
      name="resource_query_${rq_format}_${mode}"
      script="$(write_resource_query_script "${name}" "${mode}" "${rq_format}" "${rq_commands}")"
      container_run "${name}" bash "${script}"
    done
  done
else
  log "RUN_RESOURCE_QUERY_PROBES=${RUN_RESOURCE_QUERY_PROBES}; skipping resource-query probes"
fi

run_step artifact_listing find "${ARTIFACTS}" -maxdepth 3 -type f
log "complete: ${ARTIFACTS}"
if [[ "${FAILURES}" -gt 0 ]]; then
  log "probe completed with ${FAILURES} failed step(s)"
  exit 1
fi
