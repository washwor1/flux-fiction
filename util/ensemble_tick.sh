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
#
# Settings come from the environment or from the launcher config file,
# ${XDG_CONFIG_HOME:-~/.config}/flux-fiction/launcher.env (override the path
# with FF_LAUNCHER_CONFIG), which is sourced as shell:
#
#   FF_SPEC            campaign spec, if not given as the 2nd argument
#   FF_HOSTS           multi-host config, if not given as the 3rd argument
#   FF_PYTHON          interpreter, Python >= 3.11 (default: python3)
#   FF_TICK_LOG        log file (default: first writable dir in FF_TICK_LOG_DIRS)
#   FF_TICK_LOG_DIRS   space-separated candidate log dirs (default: $HOME)
#   FF_LAUNCHER_STATE  heartbeat dir; must be mounted on every host
#                      (default: ${XDG_STATE_HOME:-~/.local/state}/flux-fiction/launcher)
#   FF_REPO            flux-fiction checkout (default: the one holding this script)
set -uo pipefail

FF_CONFIG="${FF_LAUNCHER_CONFIG:-${XDG_CONFIG_HOME:-${HOME}/.config}/flux-fiction/launcher.env}"
if [ -f "${FF_CONFIG}" ]; then
    # The environment wins over the file: keep the FF_* variables already
    # exported (e.g. FF_REPO on the cron line) and restore them afterwards.
    _ff_env="$(declare -px 2>/dev/null | grep -E '^declare -x FF_' || true)"
    # shellcheck source=/dev/null
    . "${FF_CONFIG}"
    eval "${_ff_env}"
fi

USAGE="usage: ensemble_tick.sh <host-name> [spec] [hosts-file]"
HOST_NAME="${1:?${USAGE}}"
REPO="${FF_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
SPEC="${2:-${FF_SPEC:?${USAGE} (or set FF_SPEC)}}"
HOSTS="${3:-${FF_HOSTS:?${USAGE} (or set FF_HOSTS)}}"
PY="${FF_PYTHON:-python3}"
# Clusters often mount different scratch filesystems, so take the first
# candidate that is writable here rather than one fixed path.
if [ -z "${FF_TICK_LOG:-}" ]; then
    for d in ${FF_TICK_LOG_DIRS:-${HOME}}; do
        if [ -w "$d" ]; then FF_TICK_LOG="$d/ensemble-tick-${HOST_NAME}.log"; break; fi
    done
fi
LOG="${FF_TICK_LOG:?could not find a writable location for the tick log}"

# The heartbeat must live on a filesystem every host mounts, so "is the
# launcher alive, and where?" can be answered from any cluster without sshing
# to every login node and running `crontab -l` on each.
STATE_DIR="${FF_LAUNCHER_STATE:-${XDG_STATE_HOME:-${HOME}/.local/state}/flux-fiction/launcher}"
mkdir -p "${STATE_DIR}" 2>/dev/null || true
BEAT="${STATE_DIR}/${HOST_NAME}.heartbeat"

# Never let two ticks overlap: a second one could submit a batch the first is
# already submitting. flock exits 0 without running if the lock is held.
#
# NOTE, and it is a real limit: this lock is node-local (/tmp is not shared),
# so it serializes ticks ON ONE LOGIN NODE only. Two crontabs on two different
# login nodes would run concurrently with nothing between them. That is why
# `ensemble_launcher_ctl.sh install` refuses to add a second crontab while
# another node's heartbeat is fresh, and why `status` reports duplicates
# loudly. Prevention is at install time; there is no cross-node mutex here.
LOCK="${TMPDIR:-/tmp}/ensemble-tick-${HOST_NAME}.lock"
exec 9>"${LOCK}" || exit 0
if ! flock -n 9; then
    echo "$(date -u +%FT%TZ) tick skipped: previous tick still running" >> "${LOG}"
    exit 0
fi

cd "${REPO}" || exit 1
{
    # Log the LOGIN NODE, not just the cluster name. `ssh <cluster>` round-robins
    # login nodes and crontabs are per-node, so `crontab -l` from a shell that
    # landed elsewhere reports "no crontab" while the tick is running fine. That
    # produced false "no cron" alarms during the 2026-07 campaign. This line is
    # the authoritative answer to "where is the tick installed?".
    echo "=== $(date -u +%FT%TZ) tick on ${HOST_NAME} (login node $(hostname -s), pid $$)"
    PYTHONPATH="${REPO}/src" timeout 900 "${PY}" -u -m flux_fiction_ensemble run-partial \
        "${SPEC}" --hosts "${HOSTS}" --host "${HOST_NAME}" --once 2>&1 | tail -6
} >> "${LOG}" 2>&1

# Stamp the heartbeat AFTER the tick, so a hung run-partial does not look
# alive. Written to a temp file and renamed, because a reader on another
# cluster must never see a half-written record.
{
    printf '{"host":"%s","login_node":"%s","pid":%s,"stamped_at":"%s","stamped_epoch":%s,' \
        "${HOST_NAME}" "$(hostname -s)" "$$" "$(date -u +%FT%TZ)" "$(date +%s)"
    printf '"repo":"%s","spec":"%s","hosts":"%s","log":"%s","last":"%s"}\n' \
        "${REPO}" "${SPEC}" "${HOSTS}" "${LOG}" \
        "$(grep -E '^\[[0-9]{2}:[0-9]{2}:[0-9]{2}\]' "${LOG}" 2>/dev/null | tail -1 | tr '"' "'" )"
} > "${BEAT}.tmp" 2>/dev/null && mv "${BEAT}.tmp" "${BEAT}" 2>/dev/null || true

# Keep the log from growing without bound on a 15-minute cadence.
if [ "$(wc -l < "${LOG}" 2>/dev/null || echo 0)" -gt 20000 ]; then
    tail -5000 "${LOG}" > "${LOG}.tmp" && mv "${LOG}.tmp" "${LOG}"
fi
