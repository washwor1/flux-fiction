#!/usr/bin/env bash
# One launcher tick for a multi-host campaign, safe to drive from cron.
#
# Why this exists: dane's login node reaps long-lived user processes. Two
# `run-partial` launchers there died at the same instant after a few minutes,
# and `nohup`, `setsid` and `loginctl enable-linger` did not prevent it. A
# persistent launcher is therefore not viable on that host.
#
# A tick is cheap (~10 s) and idempotent. Each one:
#   * submits any batch currently in the queued state,
#   * reconciles batch/task state against the RJMS,
#   * rewrites status.json and appends to progress.csv,
#   * publishes this host's results.csv into the shared context.
#
# It deliberately does NOT retry failed batches -- resubmission stays a manual
# decision, because auto-retrying would happily burn an allocation re-running a
# systematically broken configuration. To retry, run:
#
#   python3 -m flux_fiction_ensemble retry-failed <campaign-root> --dry-run
#   python3 -m flux_fiction_ensemble retry-failed <campaign-root>
#
# and the next tick submits them.
#
# Usage: ensemble_tick.sh <host-name> [spec] [hosts-file]
set -uo pipefail

HOST_NAME="${1:?usage: ensemble_tick.sh <host-name> [spec] [hosts-file]}"
REPO="${FF_REPO:-/g/g14/ashworth12/workspace/ff-podman/flux-fiction-develop}"
SPEC="${2:-${REPO}/test-inputs/ensemble-rabbit-threshold-sweep.toml}"
HOSTS="${3:-${REPO}/test-inputs/hosts-rabbit-threshold.toml}"
# Default python3 on dane/corona is 3.6.8 with no tomllib and no tomli.
PY="${FF_PYTHON:-/collab/usr/gapps/python/toss_4_x86_64_ib/anaconda3-2025.3.1/bin/python3}"
# Lustre mounts are cluster-specific: tuolumne has lustre5, dane/corona have
# lustre1. Pick the first writable one rather than hardcoding, or the tick logs
# nowhere on the host that lacks it.
if [ -z "${FF_TICK_LOG:-}" ]; then
    for d in /p/lustre5/ashworth12 /p/lustre1/ashworth12 "${HOME}"; do
        if [ -w "$d" ]; then FF_TICK_LOG="$d/ensemble-tick-${HOST_NAME}.log"; break; fi
    done
fi
LOG="${FF_TICK_LOG:?could not find a writable location for the tick log}"

# Never let two ticks overlap: a second one could submit a batch the first is
# already submitting. flock exits 0 without running if the lock is held.
LOCK="${TMPDIR:-/tmp}/ensemble-tick-${HOST_NAME}.lock"
exec 9>"${LOCK}" || exit 0
if ! flock -n 9; then
    echo "$(date -u +%FT%TZ) tick skipped: previous tick still running" >> "${LOG}"
    exit 0
fi

cd "${REPO}" || exit 1
{
    echo "=== $(date -u +%FT%TZ) tick on ${HOST_NAME}"
    PYTHONPATH="${REPO}/src" timeout 900 "${PY}" -u -m flux_fiction_ensemble run-partial \
        "${SPEC}" --hosts "${HOSTS}" --host "${HOST_NAME}" --once 2>&1 | tail -6
} >> "${LOG}" 2>&1

# Keep the log from growing without bound on a 15-minute cadence.
if [ "$(wc -l < "${LOG}" 2>/dev/null || echo 0)" -gt 20000 ]; then
    tail -5000 "${LOG}" > "${LOG}.tmp" && mv "${LOG}.tmp" "${LOG}"
fi
