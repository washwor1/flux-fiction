#!/usr/bin/env bash
set -euo pipefail

PROBE_ROOT="${PROBE_ROOT:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-flux-fiction-probe}"
SOURCE_PROBE_ROOT="${SOURCE_PROBE_ROOT:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-probe-20}"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-/usr/WS1/ashworth12/ff-podman}"
REPO="${REPO:-${WORKSPACE_ROOT}/flux-fiction-develop}"
RESOURCE_R="${RESOURCE_R:-${WORKSPACE_ROOT}/resource_graphs/tuolumne.json}"
SPACK_FLUXION_PREFIX="${SPACK_FLUXION_PREFIX:-/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/flux-sched-0.43.0-paxb7uagtwnbepfc2sbslllm7ys2trbl}"
SPACK_FAKETIME_PREFIX="${SPACK_FAKETIME_PREFIX:-/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/faketime-0.9.10-n2lvnlfsett6t3xoh4h4hrfb7ew3bppo}"
SPACK_GCC_RUNTIME="${SPACK_GCC_RUNTIME:-/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/gcc-runtime-12.2.0-w244juoi3w2lwcowcvtkm6t5vqu5wd2t}"
SPACK_VIEW="${SPACK_VIEW:-/usr/WS1/ashworth12/package_managers/spack/var/spack/environments/emulator/.spack-env/view}"
SPACK_NINJA_PREFIX="${SPACK_NINJA_PREFIX:-/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/ninja-1.12.1-buf67bdsc6njmhhy3ipymclhnni74us2}"
MESON_PYTHONPATH="${MESON_PYTHONPATH:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/host-python-deps/meson-1.8.2}"
JOB_LIMIT="${JOB_LIMIT:-20}"
MATCH_FORMATS="${MATCH_FORMATS:-rv1_shorthand}"
RUN_DIRECT_SPACK="${RUN_DIRECT_SPACK:-1}"
RUN_FF_SPACK="${RUN_FF_SPACK:-1}"
RUN_FF_SYSTEM="${RUN_FF_SYSTEM:-1}"
DIRECT_STEP_TIMEOUT="${DIRECT_STEP_TIMEOUT:-420s}"
DIRECT_MATCH_TIMEOUT="${DIRECT_MATCH_TIMEOUT:-120}"
FF_WALLTIME_BUDGET_SECONDS="${FF_WALLTIME_BUDGET_SECONDS:-900}"
FF_FINALIZE_RESERVE_SECONDS="${FF_FINALIZE_RESERVE_SECONDS:-90}"
BASE_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"

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

write_spack_modprobe_overlay() {
  local overlay="${ARTIFACTS}/spack-modprobe"
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
  echo "${overlay}"
}

spack_runtime_env() {
  export FLUX_MODPROBE_PATH_APPEND="${SPACK_MODPROBE_OVERLAY}"
  export FLUX_FICTION_FLUXION_RESOURCE_MODULE="${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-resource.so"
  export FLUX_FICTION_FLUXION_FEASIBILITY_MODULE="${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-feasibility.so"
  export FLUX_FICTION_FLUXION_QMANAGER_MODULE="${SPACK_FLUXION_PREFIX}/lib64/flux/modules/sched-fluxion-qmanager.so"
  export LD_LIBRARY_PATH="${SPACK_GCC_RUNTIME}/lib:${SPACK_FLUXION_PREFIX}/lib64:${SPACK_FLUXION_PREFIX}/lib:${SPACK_VIEW}/lib64:${SPACK_VIEW}/lib:${BASE_LD_LIBRARY_PATH}"
}

host_python_env() {
  export PYTHONPATH="/usr/lib64/flux/python3.6:${REPO}/src:${PYTHONPATH:-}"
  export MPLBACKEND=Agg
  export FLUX_FICTION_WORKSPACE_ROOT="${WORKSPACE_ROOT}"
}

build_host_jobtap() {
  local build_dir="${ARTIFACTS}/build-host-jobtap"
  local install_dir="${ARTIFACTS}/install-host-jobtap"
  rm -rf "${build_dir}" "${install_dir}"
  mkdir -p "${build_dir}" "${install_dir}"
  env \
    CC="${HOST_JOBTAP_CC:-gcc}" \
    PYTHONPATH="${MESON_PYTHONPATH}:${PYTHONPATH:-}" \
    PATH="${SPACK_NINJA_PREFIX}/bin:${PATH}" \
    python3 -m mesonbuild.mesonmain setup "${build_dir}" "${REPO}" \
      -Duse_system_flux=true \
      -Dflux_prefix=/usr \
      --prefix "${install_dir}"
  env \
    CC="${HOST_JOBTAP_CC:-gcc}" \
    PYTHONPATH="${MESON_PYTHONPATH}:${PYTHONPATH:-}" \
    PATH="${SPACK_NINJA_PREFIX}/bin:${PATH}" \
    python3 -m mesonbuild.mesonmain compile -C "${build_dir}"
  printf '%s\n' "${build_dir}/src/emu-jobtap.so" > "${ARTIFACTS}/host_jobtap_path.txt"
}

run_direct_spack_probe() {
  local match_format="$1"
  spack_runtime_env
  host_python_env
  export FLUX_PREFIX=/nonexistent-flux-prefix
  run_step "spack_direct_${match_format}" \
    timeout "${DIRECT_STEP_TIMEOUT}" \
    flux start -s 1 \
      --setattr="log-filename=${ARTIFACTS}/spack_direct_${match_format}_broker.log" \
      -- \
      bash -lc "cd '${REPO}' && exec python3 util/sched_loop_probe_vanilla.py --resource-r '${RESOURCE_R}' --scheduler-json '${PROBE_ROOT}/configs/scheduler_${match_format}.json' --jobspec-dir '${PROBE_ROOT}/jobspecs' --out '${ARTIFACTS}/spack_direct_${match_format}.json' --label 'spack_direct_${match_format}' --limit '${JOB_LIMIT}' --per-match-timeout '${DIRECT_MATCH_TIMEOUT}'"
}

run_ff_probe() {
  local label="$1"
  local match_format="$2"
  local run_dir="${ARTIFACTS}/${label}_${match_format}"
  host_python_env
  export FLUX_FICTION_JOBTAP_SO="$(cat "${ARTIFACTS}/host_jobtap_path.txt")"
  export FLUX_FICTION_FAKETIME_DIR=/dev/shm
  # Flux creates AF_UNIX sockets under TMPDIR; long artifact paths exceed the
  # socket path limit inside deeply nested run directories.
  export TMPDIR="${FLUX_FICTION_HOST_TMPDIR:-/tmp}"

  if [[ "${label}" == "spack_ff" ]]; then
    spack_runtime_env
    export FLUX_PREFIX=/nonexistent-flux-prefix
    local faketime_lib="${SPACK_FAKETIME_PREFIX}/lib/faketime/libfaketimeMT.so.1"
  else
    unset FLUX_MODPROBE_PATH_APPEND
    unset FLUX_FICTION_FLUXION_RESOURCE_MODULE
    unset FLUX_FICTION_FLUXION_FEASIBILITY_MODULE
    unset FLUX_FICTION_FLUXION_QMANAGER_MODULE
    export LD_LIBRARY_PATH="${BASE_LD_LIBRARY_PATH}"
    export FLUX_PREFIX=/nonexistent-flux-prefix
    local faketime_lib="${WORKSPACE_ROOT}/libfaketime/src/libfaketimeMT.so.1"
  fi

  run_step "${label}_${match_format}" \
    python3 -m flux_fiction.cli.run_ff \
      "${PROBE_ROOT}/configs/flux_fiction_${match_format}.toml" \
      --run-dir "${run_dir}" \
      --walltime-budget-seconds "${FF_WALLTIME_BUDGET_SECONDS}" \
      --finalize-reserve-seconds "${FF_FINALIZE_RESERVE_SECONDS}" \
      --broker-log-level 3 \
      --no-broker-log-file \
      --faketime-lib "${faketime_lib}" \
      --faketime-start-lead 30
}

copy_probe_inputs
SPACK_MODPROBE_OVERLAY="$(write_spack_modprobe_overlay)"
export SPACK_MODPROBE_OVERLAY

log "probe root: ${PROBE_ROOT}"
log "source probe root: ${SOURCE_PROBE_ROOT}"
log "artifacts: ${ARTIFACTS}"
log "host: $(hostname)"
log "flux: $(flux --version | tr '\n' '; ')"
log "spack fluxion: ${SPACK_FLUXION_PREFIX}"
log "spack modprobe overlay: ${SPACK_MODPROBE_OVERLAY}"

run_step env env
run_step flux_version flux --version
run_step python_imports bash -lc "cd '${REPO}' && PYTHONPATH='/usr/lib64/flux/python3.6:${REPO}/src' python3 -c 'import flux, pydantic, tqdm, yaml, flux_fiction; print(\"ok\")'"
run_step spack_modprobe_show env FLUX_MODPROBE_PATH_APPEND="${SPACK_MODPROBE_OVERLAY}" flux modprobe show sched
run_step spack_module_paths bash -lc "export LD_LIBRARY_PATH='${SPACK_GCC_RUNTIME}/lib:${SPACK_FLUXION_PREFIX}/lib64:${SPACK_FLUXION_PREFIX}/lib:${SPACK_VIEW}/lib64:${SPACK_VIEW}/lib:'\"\${LD_LIBRARY_PATH:-}\"; export FLUX_MODPROBE_PATH_APPEND='${SPACK_MODPROBE_OVERLAY}'; flux start -s 1 --setattr='log-filename=${ARTIFACTS}/spack_module_paths_broker.log' -- flux python -c 'import flux,json; h=flux.Flux(); print(json.dumps(h.rpc(\"module.list\").get()[\"mods\"], indent=2))'"

run_step build_host_jobtap build_host_jobtap
run_step host_jobtap_ldd ldd "$(cat "${ARTIFACTS}/host_jobtap_path.txt")"

if [[ "${RUN_DIRECT_SPACK}" == "1" ]]; then
  for match_format in ${MATCH_FORMATS}; do
    run_direct_spack_probe "${match_format}"
  done
else
  log "RUN_DIRECT_SPACK=0; skipping direct Spack Fluxion probe"
fi

if [[ "${RUN_FF_SPACK}" == "1" ]]; then
  for match_format in ${MATCH_FORMATS}; do
    run_ff_probe spack_ff "${match_format}"
  done
else
  log "RUN_FF_SPACK=0; skipping Spack Flux Fiction probe"
fi

if [[ "${RUN_FF_SYSTEM}" == "1" ]]; then
  for match_format in ${MATCH_FORMATS}; do
    run_ff_probe system_ff "${match_format}"
  done
else
  log "RUN_FF_SYSTEM=0; skipping system Fluxion Flux Fiction probe"
fi

run_step artifact_listing find "${ARTIFACTS}" -maxdepth 4 -type f
log "complete: ${ARTIFACTS}"
if [[ "${FAILURES}" -gt 0 ]]; then
  log "probe completed with ${FAILURES} failed step(s)"
  exit 1
fi
