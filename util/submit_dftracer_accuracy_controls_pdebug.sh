#!/usr/bin/env bash
# Generate and submit six two-way accuracy controls to pdebug.
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/g/g14/ashworth12/workspace/ff-podman}"
FF_SOURCE="${FF_SOURCE:-${WORKSPACE_ROOT}/flux-fiction-shared-clock}"
OUT_ROOT="${OUT_ROOT:-/p/lustre5/ashworth12/dftracer-accuracy-controls-$(date -u +%Y%m%d_%H%M%S)}"

if [[ -e "${OUT_ROOT}" ]]; then
    echo "refusing to reuse output root: ${OUT_ROOT}" >&2
    exit 2
fi
mkdir -p "${OUT_ROOT}"
python3 "${FF_SOURCE}/util/generate_dftracer_accuracy_controls.py" \
    --out "${OUT_ROOT}" \
    --workspace-root "${WORKSPACE_ROOT}"

jobids=()
for policy in easy conservative hybrid; do
    for latency_name in latency-on latency-off; do
        pair="${policy}-${latency_name}"
        jobid="$(flux batch \
            -N1 -n1 --exclusive \
            --queue=pdebug \
            -t 59m \
            --job-name="dft-accuracy-${pair}" \
            --output="${OUT_ROOT}/flux-${pair}.out" \
            --error="${OUT_ROOT}/flux-${pair}.out" \
            "${FF_SOURCE}/util/run_dftracer_accuracy_pair_pdebug.sh" \
            "${OUT_ROOT}" "${policy}" "${latency_name}")"
        jobids+=("${jobid}")
        printf '%s %s\n' "${pair}" "${jobid}" | tee -a "${OUT_ROOT}/submitted_jobs.txt"
    done
done

printf 'OUT_ROOT=%s\n' "${OUT_ROOT}"
printf 'JOBIDS=%s\n' "${jobids[*]}"
