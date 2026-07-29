#!/usr/bin/env bash
# Does swapping the container-installs directory reach a job that was ALREADY
# QUEUED when the swap happened?
#
# This mimics worker.sh exactly: it hardcodes the literal prefix path rather than
# taking it as an argument, because that is what the generated worker.sh does and
# what the 373 already-submitted tuolumne jobspecs will execute. The prefix is
# resolved when the container starts, not when the job was submitted.
#
# It prints the size of the fluxion module it actually mounted:
#   671016 -> still the -O0 build (swap did NOT reach a queued job)
#   158568 -> the -O3 build       (swap DID reach a queued job)
set -uo pipefail
WORKSPACE_ROOT=/usr/WS1/ashworth12/ff-podman
INSTALLS="${WORKSPACE_ROOT}/container-installs"     # literal, as worker.sh bakes it
IMAGE=localhost/flux-fiction-dev:latest
OUT="${1:?usage: test_queued_swap.sh <outdir>}"
mkdir -p "${OUT}"

if [[ -z "${XDG_RUNTIME_DIR:-}" || ! -d "${XDG_RUNTIME_DIR}" ]]; then
    export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"; mkdir -p "${XDG_RUNTIME_DIR}"
fi
podman info >/dev/null 2>&1 </dev/null || true
cleanup(){ pkill -u "$(id -un)" -x catatonit 2>/dev/null||true; pkill -u "$(id -un)" -x conmon 2>/dev/null||true; pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null||true; }
trap cleanup EXIT
podman image exists "${IMAGE}" </dev/null || podman load -i "${WORKSPACE_ROOT}/flux-fiction-dev.tar" </dev/null

echo "job started at: $(date -u +%FT%TZ)"     | tee -a "${OUT}/verdict.txt"
echo "host-side size: $(stat -c %s "${INSTALLS}/flux-core/lib/flux/modules/sched-fluxion-resource.so")" | tee -a "${OUT}/verdict.txt"

SCRATCH=$(mktemp -d /l/ssd/qswap.XXXXXX 2>/dev/null || mktemp -d /var/tmp/qswap.XXXXXX)
t0=$(date +%s.%N)
podman run --rm --pull=never \
    -e MPLBACKEND=Agg -e FLUX_FICTION_FAKETIME_DIR=/dev/shm -e TMPDIR=/scratch \
    -e PYTHONPATH=/workspace/flux-fiction-develop/src \
    --shm-size=1g \
    -v "${WORKSPACE_ROOT}:/workspace" \
    -v "${INSTALLS}:/workspace/container-installs" \
    -v "${OUT}:/out" -v "${SCRATCH}:/scratch" \
    "${IMAGE}" \
    bash -lc 'source /usr/local/bin/flux-dev-env.sh 2>/dev/null || true
              M=/workspace/container-installs/flux-core/lib/flux/modules/sched-fluxion-resource.so
              echo "in-container module size: $(stat -c %s $M)" >> /out/verdict.txt
              python3 -m flux_fiction.cli.run_ff /workspace/probe5_ff.toml \
                  --tag qswap --run-dir /out/run >/out/run.log 2>&1
              echo "ff rc=$?" >> /out/verdict.txt' </dev/null
t1=$(date +%s.%N)
printf 'wall=%.1fs\n' "$(echo "$t1 - $t0" | bc)" | tee -a "${OUT}/verdict.txt"
python3 -c "
import json,glob
f=glob.glob('${OUT}/**/summary.json',recursive=True)
print('jobs_completed:', json.load(open(f[0])).get('jobs_completed') if f else 'NONE')" | tee -a "${OUT}/verdict.txt"
rm -rf "${SCRATCH}" 2>/dev/null || true
