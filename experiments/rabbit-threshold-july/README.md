# Rabbit threshold sweep, July 2026: run configuration

Scripts and configuration for the 12,060-task rabbit threshold sweep launched on 2026-07-26 across
tuolumne, dane and corona. The sweep asks where rabbit storage requests become the dominant factor in
Tuolumne utilization, varying request frequency and request size.

This directory is a record of what was run. It cannot be rerun as it stands: the input trace is not
included, and paths are placeholders.

## Grid

3 queue policies × 2 match policies × 2 rabbit distributions × 10 job percentages × 10 ceiling
percentages, plus one 0% baseline per policy pair: 1,206 cells × 10 seeds = 12,060 tasks.

| Host | Backend | Tasks | Batches | Runs per node |
|---|---|---|---|---|
| tuolumne | flux | 5,964 | 373 | 16 |
| dane | slurm | 5,969 | 374 | 16 |
| corona | flux | 127 | 16 | 8 |

Tasks were split round-robin on a 95-task cycle (1 corona, 47 dane, 47 tuolumne). Replicas of a cell
therefore ran on different machines. Only 1,343 tasks reached all 10,000 jobs, so the sweep is partial.

## Contents

| Path | What it is |
|---|---|
| `spec/` | Ensemble spec and host split |
| `configs/` | Base flux-fiction and scheduler configuration |
| `resource_graphs/tuolumne.json` | The synthetic 1,153-node graph |
| `campaign/<host>/` | `worker.sh`, resolved spec, task list, batches and assignment for each host |
| `campaign/example-batch/` | One batch manifest and one task's generated configuration |
| `scripts/` | Trace slicing, cron tick and container build files, as committed in `fe3c524` |
| `provenance/` | What code ran, and a hash of every original file |

## Not included

- **Traces.** Neither `tuo_queueB_10k_sat.csv` nor the per-task `trace.csv` files are here, because they
  hold production job data. `scripts/make_queueb_slice.py` shows how the slice was selected.
- **The container image and flux-core/flux-sched build.** See `provenance/code-as-run.md`.
- **Results.**

## Changes from the originals

Paths specific to one user were replaced, so these files are not byte-identical to what ran.
`provenance/file-manifest.tsv` gives the SHA-256 of each original and the number of replacements. No
other edits were made. Paths are otherwise unchanged and many are site-specific.

| Placeholder | Meaning |
|---|---|
| `__WORKSPACE_ROOT__` | Workspace holding the source trees and inputs |
| `__WORKSPACE_PARENT__` | Parent of that workspace |
| `__SPACK_ROOT__` | Spack installation |
| `__LUSTRE5_ROOT__`, `__LUSTRE1_ROOT__` | Per-user Lustre output roots |
| `__HOME__` | Home directory |
| `__USER__` | User name |

The spec names `resource_graphs/tuolumne.json` under the workspace. That path later held a different,
1,144-node graph; the file here is the 1,153-node graph. The match is by node count, since no hash of
the input was recorded. The base configs moved after the campaign, so their spec paths are also stale.

## Known limitations

- **Graph.** The synthetic graph has since been retired in favour of the live one.
- **Build type.** The container flux-sched had no `CMAKE_BUILD_TYPE`, effectively `-O0`.
- **Early finalize.** Runs are asked to finalize 900 s before the wall, but the engine checks for the
  request only every 10th loop iteration. Saturated runs can miss the wall and lose their per-job output.
- **Dane.** 33 batches were cancelled about 19 hours into the 24-hour limit.
