#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workspace_root="${WORKSPACE_ROOT:-$(cd "${repo_root}/.." && pwd)}"
libfaketime_root="${LIBFAKETIME_ROOT:-${workspace_root}/libfaketime-shared-clock}"
installs="${CONTAINER_INSTALLS:-${workspace_root}/container-installs}"
image="${CONTAINER_IMAGE:-localhost/flux-fiction-dev:latest}"
repetitions="${REPETITIONS:-3}"
results="${RESULTS:-${repo_root}/benchmarks/shared-clock/e2e_results.csv}"
logs_dir="${repo_root}/benchmarks/shared-clock/logs"

mkdir -p "${logs_dir}"
printf '%s\n' 'mode,repetition,order,wall_seconds,exit_code,jobs' > "${results}"

run_one()
{
    local mode="$1"
    local repetition="$2"
    local order="$3"
    local log_path="${logs_dir}/${mode}-${repetition}.log"
    local start_ns end_ns elapsed_ns exit_code jobs

    start_ns="$(date +%s%N)"
    set +e
    podman run --rm \
        -e FLUX_FICTION_JOBTAP_SO=/workspace/flux-fiction/build/src/emu-jobtap.so \
        -v "${repo_root}:/workspace/flux-fiction" \
        -v "${libfaketime_root}:/workspace/libfaketime" \
        -v "${installs}:/workspace/container-installs" \
        -w /workspace/flux-fiction "${image}" bash -lc \
        'source /usr/local/bin/flux-dev-env.sh &&
         export PYTHONPATH=/workspace/flux-fiction/src:/workspace/container-installs/flux-core/lib/flux/python3.12 &&
         python3 -m flux_fiction.cli.run_ff src/config.toml \
           --tag shared-clock-e2e \
           --faketime-mode '"${mode}"' \
           --faketime-lib /workspace/libfaketime/src/libfaketimeMT.so.1 \
           --run-dir /tmp/flux-fiction-shared-clock-e2e' \
        > "${log_path}" 2>&1
    exit_code=$?
    set -e
    end_ns="$(date +%s%N)"
    elapsed_ns=$((end_ns - start_ns))
    jobs="$(grep -Eo 'Jobs completed: +100%[^0-9]+[0-9]+/[0-9]+' "${log_path}" \
        | tail -1 | grep -Eo '[0-9]+/[0-9]+' | cut -d/ -f2 || true)"
    printf '%s,%s,%s,%s.%09d,%s,%s\n' \
        "${mode}" "${repetition}" "${order}" \
        "$((elapsed_ns / 1000000000))" "$((elapsed_ns % 1000000000))" \
        "${exit_code}" "${jobs:-0}" >> "${results}"
    return "${exit_code}"
}

for repetition in $(seq 1 "${repetitions}"); do
    if ((repetition % 2 == 1)); then
        modes=(legacy shared)
    else
        modes=(shared legacy)
    fi
    order=0
    for mode in "${modes[@]}"; do
        order=$((order + 1))
        run_one "${mode}" "${repetition}" "${order}"
    done
done

printf 'End-to-end results: %s\n' "${results}"
