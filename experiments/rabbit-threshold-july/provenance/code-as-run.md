# Code as run

The campaign recorded no source revision, and `worker.sh` imported a live working tree whose ensemble
code was first committed on 2026-07-29, after the runs. The code was identified afterwards.

## Method

Containers ran Python 3.12 and left `.cpython-312.pyc` files in the mounted tree. Each records the mtime
and size of the source it was compiled from (`source-fingerprint.tsv`, 50 files). Candidates from git
were compiled with Python 3.12 and compared function by function, covering bytecode, constants and names.
Files whose source had not changed compared identical, which validates the method.

## Result

The closest commit is `fe3c524`. Compared with it, the code that ran differs as follows:

| File | Source size as run | Difference |
|---|---|---|
| `_core/engine.py` | 57,011 | Matches `40226f1` in 45 of 46 functions; `run()` also calls `get_scheduler_metrics`. Lacks the module-stats reporting added in `817ecf4` |
| `cli/run_ff.py` | 37,989 | Lacks `submit_novalidate` and the `config_overrides` argument (3 of 44 code objects differ) |
| `_adapters/flux/adapter.py` | 14,441 | Lacks `get_flux_module_stats` |

The other imported files match `fe3c524`. `_paths.py` and `cli/main.py` differ, but their bytecode dates
from June, so these runs did not import them under 3.12.

No exact source for the three files exists in git, so they are described here rather than reproduced.

## Changes during the campaign

The bytecode shows only the last compile of each file, so earlier edits may not be visible.

- `ensemble/campaign.py` was edited on 2026-07-27 at 04:38 UTC, after dane and corona started their
  second attempts and before tuolumne started.
- `api/status.py` was edited on 2026-07-27 at 16:19 UTC, after tuolumne started and before dane's
  second wave. The earlier version is not preserved.

## Build

The flux-core and flux-sched revisions were not recorded. The container image config digest in
`batch.log` is `sha256:71fb6a05f123a7d9f86bd9190350ba302602e76450dc5317dfc6a8b65266d7d2`. The image
itself is no longer available. `scripts/podman_containers/` is the version in `fe3c524`, which was
edited after the image was built.
