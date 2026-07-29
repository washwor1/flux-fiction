#!/usr/bin/env bash
# Install, find, and check the campaign launcher.
#
# WHY THIS EXISTS
#
# Long-lived launchers get reaped on LC login nodes. Measured: dane's died
# within minutes, corona's after ~8.4 h, tuolumne's after ~13 h. `nohup`,
# `setsid` and `loginctl enable-linger` were all tried and all failed.
#
# Short flock-guarded cron ticks are the answer, and that is not a guess: a
# tick installed for the 2026-07 campaign was still firing every 15 minutes on
# 2026-09-01, five weeks later, across a Tuolumne maintenance window. Cron also
# survives a login-node reboot, which a self-resubmitting batch job does not.
#
# So the MECHANISM was never the problem. The problem was that nobody could
# tell whether it was running, or where:
#
#   * `ssh <cluster>` round-robins login nodes and crontabs are per-node, so
#     `crontab -l` reports "no crontab" from seven nodes out of eight. This
#     produced false "no cron" alarms during the campaign, and on 2026-09-01
#     finding the stale entry took a sweep of all 8 tuolumne login nodes. It
#     was on tuolumne1002.
#   * Nothing noticed when a tick stopped. A campaign could stall silently.
#   * Nothing stopped a second crontab going onto a second login node. The
#     tick's flock is on /tmp, which is node-local, so two of them would run
#     concurrently with no mutual exclusion at all.
#
# This script fixes those three. It does not replace the tick; it wraps it.
#
#   install    add the cron entry on THIS login node, refusing if a tick is
#              already alive elsewhere
#   status     where is it, when did it last fire, is it stale, are there
#              duplicates
#   uninstall  remove this node's entry
#   beat       print the raw heartbeat records
#
# The heartbeat directory must be mounted on every host, or a heartbeat would
# be invisible from the host that needs to read it. Settings come from the
# environment or the launcher config file; see util/ensemble_tick.sh.

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

STATE_DIR="${FF_LAUNCHER_STATE:-${XDG_STATE_HOME:-${HOME}/.local/state}/flux-fiction/launcher}"
INTERVAL_MIN="${FF_TICK_INTERVAL_MIN:-15}"
# Stale after two missed ticks plus a margin. A tick can legitimately take up
# to its 900 s timeout, so this must not be tighter than that.
STALE_SECONDS="${FF_TICK_STALE_SECONDS:-$(( INTERVAL_MIN * 60 * 2 + 900 ))}"

die() { echo "$*" >&2; exit 1; }
now_epoch() { date +%s; }

beat_field() {  # beat_field <file> <key>
    sed -n "s/.*\"$2\":\"\{0,1\}\([^,\"}]*\)\"\{0,1\}.*/\1/p" "$1" 2>/dev/null | head -1
}

# --- status ---------------------------------------------------------------
cmd_status() {
    [ -d "${STATE_DIR}" ] || { echo "no launcher state at ${STATE_DIR} — nothing has ever ticked"; return 3; }
    local found=0 alive=0 rc=0
    shopt -s nullglob
    for beat in "${STATE_DIR}"/*.heartbeat; do
        found=$(( found + 1 ))
        local host node stamped epoch age spec log state
        host="$(basename "${beat}" .heartbeat)"
        node="$(beat_field "${beat}" login_node)"
        stamped="$(beat_field "${beat}" stamped_at)"
        epoch="$(beat_field "${beat}" stamped_epoch)"
        spec="$(beat_field "${beat}" spec)"
        log="$(beat_field "${beat}" log)"
        age=$(( $(now_epoch) - ${epoch:-0} ))
        if [ "${age}" -le "${STALE_SECONDS}" ]; then state="ALIVE"; alive=$(( alive + 1 ));
        else state="STALE"; rc=1; fi
        printf '%-10s %-8s login node %-14s last tick %s (%dm ago)\n' \
            "${host}" "${state}" "${node:-?}" "${stamped:-?}" "$(( age / 60 ))"
        printf '           spec %s\n           log  %s\n' "${spec:-?}" "${log:-?}"
    done
    shopt -u nullglob
    [ "${found}" -eq 0 ] && { echo "no heartbeats in ${STATE_DIR} — no tick has fired yet"; return 3; }

    # Duplicates are the dangerous case: the tick's flock is node-local, so two
    # live ticks for the same host on different login nodes are NOT serialized
    # against each other and can both advance campaign state.
    local dupes
    dupes="$(for beat in "${STATE_DIR}"/*.heartbeat; do
                 [ -e "${beat}" ] || continue
                 e="$(beat_field "${beat}" stamped_epoch)"
                 [ $(( $(now_epoch) - ${e:-0} )) -le "${STALE_SECONDS}" ] \
                     && basename "${beat}" .heartbeat
             done | sort | uniq -d)"
    if [ -n "${dupes}" ]; then
        echo
        echo "WARNING: more than one live tick for: ${dupes}"
        echo "  The tick lock is node-local. Two ticks on two login nodes are not"
        echo "  mutually excluded. Uninstall one before the next submit."
        rc=2
    fi
    [ "${alive}" -eq 0 ] && echo && echo "Nothing is alive. Re-install with: $0 install <host> <spec> <hosts>"
    return "${rc}"
}

# --- install --------------------------------------------------------------
cmd_install() {
    local host="${1:?usage: $0 install <host> <spec> <hosts> [--force] [--dry-run]}"
    local spec="${2:?spec TOML required}"
    local hosts="${3:?hosts TOML required}"
    shift 3
    local force=0 dry=0
    for a in "$@"; do
        case "$a" in
            --force) force=1 ;;
            --dry-run) dry=1 ;;
            *) die "unknown option: $a" ;;
        esac
    done

    [ -f "${spec}" ]  || die "spec not found: ${spec}"
    [ -f "${hosts}" ] || die "hosts file not found: ${hosts}"
    spec="$(readlink -f "${spec}")"; hosts="$(readlink -f "${hosts}")"
    local repo tick
    repo="${FF_REPO:-$(readlink -f "$(dirname "${BASH_SOURCE[0]}")/..")}"
    tick="${repo}/util/ensemble_tick.sh"
    [ -x "${tick}" ] || [ -f "${tick}" ] || die "tick script not found: ${tick}"

    # Refuse to create the second crontab. This is the only thing standing
    # between the campaign and two unsynchronized launchers, because the tick's
    # own lock cannot see across login nodes.
    local beat="${STATE_DIR}/${host}.heartbeat"
    if [ -f "${beat}" ] && [ "${force}" -eq 0 ]; then
        local e age node
        e="$(beat_field "${beat}" stamped_epoch)"; node="$(beat_field "${beat}" login_node)"
        age=$(( $(now_epoch) - ${e:-0} ))
        if [ "${age}" -le "${STALE_SECONDS}" ]; then
            echo "A tick for '${host}' is already alive on login node ${node} (last fired $(( age / 60 ))m ago)." >&2
            echo "Installing a second one would run two unsynchronized launchers." >&2
            echo "If that node is gone and you are sure, re-run with --force." >&2
            return 1
        fi
        echo "Found a STALE heartbeat for '${host}' (login node ${node}, $(( age / 60 ))m ago) — taking over."
    fi

    local line
    line="*/${INTERVAL_MIN} * * * * FF_REPO=${repo} ${tick} ${host} ${spec} ${hosts} >/dev/null 2>&1"
    local marker="# flux-fiction ensemble launcher: ${host}"

    if [ "${dry}" -eq 1 ]; then
        echo "would install on $(hostname -s):"; echo "  ${marker}"; echo "  ${line}"; return 0
    fi

    # Idempotent: drop any previous entry for this host on this node first.
    local current new
    current="$(crontab -l 2>/dev/null | grep -v "ensemble_tick.sh ${host} " | grep -vF "${marker}")"
    new="$(printf '%s\n%s\n%s\n' "${current}" "${marker}" "${line}" | sed '/^$/d')"
    printf '%s\n' "${new}" | crontab - || die "crontab install failed"

    mkdir -p "${STATE_DIR}" || die "cannot create ${STATE_DIR}"
    echo "Installed on $(hostname -s), every ${INTERVAL_MIN} min:"
    echo "  ${line}"
    echo
    echo "It will not appear in \`crontab -l\` from another login node. Use:"
    echo "  $0 status"
    echo "The first heartbeat lands within ${INTERVAL_MIN} minutes."
}

# --- uninstall ------------------------------------------------------------
cmd_uninstall() {
    local host="${1:?usage: $0 uninstall <host>}"
    local before after
    before="$(crontab -l 2>/dev/null | wc -l)"
    after="$(crontab -l 2>/dev/null | grep -v "ensemble_tick.sh ${host} " | grep -v "# flux-fiction ensemble launcher: ${host}")"
    if [ -z "${after//[[:space:]]/}" ]; then crontab -r 2>/dev/null
    else printf '%s\n' "${after}" | crontab -; fi
    echo "Removed launcher entries for '${host}' on $(hostname -s) ($(( before - $(crontab -l 2>/dev/null | wc -l) )) line(s))."
    echo "The heartbeat at ${STATE_DIR}/${host}.heartbeat is left in place as a record;"
    echo "it will go stale on its own. Delete it if you want status to forget this host."
}

cmd_beat() {
    shopt -s nullglob
    for b in "${STATE_DIR}"/*.heartbeat; do echo "--- ${b}"; cat "${b}"; echo; done
    shopt -u nullglob
}

case "${1:-status}" in
    install)   shift; cmd_install "$@" ;;
    uninstall) shift; cmd_uninstall "$@" ;;
    status)    shift; cmd_status "$@" ;;
    beat)      shift; cmd_beat "$@" ;;
    *) die "usage: $0 {install <host> <spec> <hosts> [--force|--dry-run] | status | uninstall <host> | beat}" ;;
esac
