#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:?usage: system_fluxion_probe.sh <output-dir> [job-count]}"
JOB_COUNT="${2:-1000}"
mkdir -p "${OUT_DIR}"

{
  echo "host=$(hostname)"
  echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "job_count=${JOB_COUNT}"
  echo "outer_flux_uri=${FLUX_URI:-}"
  echo "flux_path=$(command -v flux)"
  flux version
} >"${OUT_DIR}/system_fluxion_probe.env"

flux start bash -s "${OUT_DIR}" "${JOB_COUNT}" <<'INNER'
set -euo pipefail
OUT_DIR="$1"
JOB_COUNT="$2"

{
  echo "nested_host=$(hostname)"
  echo "nested_flux_uri=${FLUX_URI:-}"
  echo "nested_started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "=== flux version ==="
  flux version
  echo "=== modules ==="
  flux module list
  echo "=== resource ==="
  flux resource list || true
} >"${OUT_DIR}/nested_fluxion_context.txt" 2>&1

start="$(date +%s.%N)"
flux submit --cc="1-${JOB_COUNT}" -n1 -c1 --quiet --wait /bin/true \
  >"${OUT_DIR}/submit_wait.stdout" 2>"${OUT_DIR}/submit_wait.stderr"
rc="$?"
end="$(date +%s.%N)"

python3 - "${OUT_DIR}" "${JOB_COUNT}" "${start}" "${end}" "${rc}" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
count = int(sys.argv[2])
start = float(sys.argv[3])
end = float(sys.argv[4])
rc = int(sys.argv[5])
elapsed = max(0.0, end - start)
payload = {
    "job_count": count,
    "return_code": rc,
    "start_epoch": start,
    "end_epoch": end,
    "elapsed_seconds": elapsed,
    "jobs_per_second": (count / elapsed) if elapsed else None,
    "jobs_per_minute": (60.0 * count / elapsed) if elapsed else None,
}
(out / "system_fluxion_probe.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(json.dumps(payload, sort_keys=True))
PY

{
  echo "=== flux jobs stats ==="
  flux jobs -a --stats || true
  echo "=== flux queue status ==="
  flux queue status || true
} >"${OUT_DIR}/nested_fluxion_after.txt" 2>&1
INNER
