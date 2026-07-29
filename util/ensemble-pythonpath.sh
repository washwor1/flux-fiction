#!/usr/bin/env bash
# Source this from the repository root before running source-tree ensemble commands:
#   source util/ensemble-pythonpath.sh
#
# Or use it as a one-command wrapper:
#   util/ensemble-pythonpath.sh python3 -m flux_fiction_ensemble status "$ROOT"

_ff_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${_ff_repo_root}/src:${PYTHONPATH:-}"

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -gt 0 ]]; then
    exec "$@"
  fi
  printf 'PYTHONPATH=%s\n' "${PYTHONPATH}"
  printf 'Use: source %s\n' "$0"
fi
