#!/usr/bin/env bash
# Head-to-head: the container's Fluxion built -O0 vs the same source built -O3.
#
# Both runs happen in one allocation, on one node, back to back, with the SAME
# container image, the SAME 20-job probe trace and the SAME scheduler config
# (conservative, queue-depth 2048, reservation-depth 128, lonodex, full 1153-node
# tuolumne graph). The ONLY difference is which container-installs prefix is
# mounted, i.e. the optimization level of sched-fluxion-*.so.
#
# This deliberately reuses the workload behind the published probe table so the
# wall times line up with:
#   container (-O0)   414 s   2.90 jobs/min
#   Spack 0.53.0      128 s   9.38 jobs/min
#
#   flux run -N1 -n1 --exclusive -q pdebug -t 45m bash util/bench_fluxion_o0_vs_o3.sh
set -uo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/usr/WS1/ashworth12/ff-podman}"
IMAGE="${FLUX_FICTION_IMAGE:-localhost/flux-fiction-dev:latest}"
IMAGE_TAR="${WORKSPACE_ROOT}/flux-fiction-dev.tar"
OUT_ROOT="${OUT_ROOT:-/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/o0-vs-o3-$(date +%Y%m%d_%H%M%S)}"
REPEATS="${REPEATS:-1}"

if [[ -z "${XDG_RUNTIME_DIR:-}" || ! -d "${XDG_RUNTIME_DIR}" ]]; then
    export XDG_RUNTIME_DIR="/tmp/podman-run-$(id -u)"; mkdir -p "${XDG_RUNTIME_DIR}"
fi
podman info >/dev/null 2>&1 </dev/null || true
cleanup() {
    pkill -u "$(id -un)" -x catatonit 2>/dev/null || true
    pkill -u "$(id -un)" -x conmon 2>/dev/null || true
    pkill -u "$(id -un)" -f fuse-overlayfs 2>/dev/null || true
}
trap cleanup EXIT
podman image exists "${IMAGE}" </dev/null || podman load -i "${IMAGE_TAR}" </dev/null

mkdir -p "${OUT_ROOT}"
echo "node: $(hostname)   out: ${OUT_ROOT}"

run_one() {  # $1=label  $2=installs-prefix
    local label="$1" installs="$2" rundir="${OUT_ROOT}/${1}_r${3}"
    mkdir -p "${rundir}"
    local so="${installs}/flux-core/lib/flux/modules/sched-fluxion-resource.so"
    echo "--- ${label} (rep $3): $(stat -c %s "${so}") byte fluxion module"
    local t0 t1
    t0=$(date +%s.%N)
    # Node-local scratch for the child broker's KVS; /tmp in-container is
    # fuse-overlayfs and would tax both runs unevenly.
    podman run --rm --pull=never \
        -e MPLBACKEND=Agg \
        -e FLUX_FICTION_FAKETIME_DIR=/dev/shm \
        -e TMPDIR=/scratch \
        -e PYTHONPATH=/workspace/flux-fiction-develop/src \
        --shm-size=1g \
        -v "${WORKSPACE_ROOT}:/workspace" \
        -v "${installs}:/workspace/container-installs" \
        -v "${rundir}:/out" \
        -v "${SCRATCH_HOST}:/scratch" \
        "${IMAGE}" \
        bash -lc "source /usr/local/bin/flux-dev-env.sh 2>/dev/null || true
                  # flux-fiction-run is not on PATH in this image; the ensemble
                  # worker uses python -m against the live tree, so do the same.
                  # PYTHONPATH arrives via -e BEFORE flux-dev-env.sh runs, which
                  # appends the flux bindings. Exporting it here instead would
                  # clobber them and the inner process could not import flux.
                  python3 -m flux_fiction.cli.run_ff /workspace/probe20_ff.toml \
                      --tag ${label} --run-dir /out/run >/out/run.log 2>&1
                  rc=\$?; echo rc=\$rc > /out/rc.txt; exit \$rc" </dev/null
    t1=$(date +%s.%N)
    local wall; wall=$(echo "${t1} - ${t0}" | bc)
    local jobs; jobs=$(python3 -c "
import json,glob
f=glob.glob('${rundir}/**/summary.json',recursive=True)
print(json.load(open(f[0])).get('jobs_completed',0) if f else 0)" 2>/dev/null || echo 0)
    printf '%s,%s,%.1f,%s\n' "${label}" "$3" "${wall}" "${jobs}" >> "${OUT_ROOT}/results.csv"
    printf '    wall=%.1fs  jobs_completed=%s\n' "${wall}" "${jobs}"
}

SCRATCH_HOST=$(mktemp -d "${FLUX_FICTION_SCRATCH_HOST_ROOT:-/l/ssd}/o0o3.XXXXXX" 2>/dev/null \
             || mktemp -d /var/tmp/o0o3.XXXXXX)
echo "label,rep,wall_s,jobs_completed" > "${OUT_ROOT}/results.csv"

for r in $(seq 1 "${REPEATS}"); do
    # Alternate order so a warm-cache advantage cannot favour one arm.
    if (( r % 2 == 1 )); then
        run_one o0 "${WORKSPACE_ROOT}/container-installs"    "$r"
        run_one o3 "${WORKSPACE_ROOT}/container-installs-o3" "$r"
    else
        run_one o3 "${WORKSPACE_ROOT}/container-installs-o3" "$r"
        run_one o0 "${WORKSPACE_ROOT}/container-installs"    "$r"
    fi
done

rm -rf "${SCRATCH_HOST}" 2>/dev/null || true
echo; echo "=== results (${OUT_ROOT}/results.csv):"; cat "${OUT_ROOT}/results.csv"
python3 - "${OUT_ROOT}/results.csv" <<'PY'
import csv, sys, statistics as st
rows=list(csv.DictReader(open(sys.argv[1])))
g={}
for r in rows: g.setdefault(r['label'],[]).append(float(r['wall_s']))
if 'o0' in g and 'o3' in g:
    a,b=st.median(g['o0']),st.median(g['o3'])
    print(f"\n-O0 median wall {a:.1f}s   -O3 median wall {b:.1f}s   speedup {a/b:.2f}x")
    print(f"jobs/min: -O0 {20/(a/60):.2f}   -O3 {20/(b/60):.2f}")
    print("reference from the published table: container 414s/2.90, Spack 0.53.0 128s/9.38")
PY
