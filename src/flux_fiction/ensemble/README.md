# Flux Fiction ensemble launcher

Runs a grid of Flux Fiction simulations as a *campaign*: generate a task grid
from one TOML, batch it, submit each batch as a cluster job, track progress, and
collect results — across one cluster or several at once.

Invoke it as `python3 -m flux_fiction_ensemble <command>` (the
`flux_fiction_ensemble` package is a thin standalone wrapper around
`flux_fiction.ensemble`).

---

## 1. Mental model

```
campaign spec (TOML)
  └─ tasks          one per grid cell (policy x policy x rabbit x duplicate)
       └─ batches   tasks chunked by batch_size; ONE cluster job per batch
            └─ runs batch_size simulations run concurrently inside one container
```

* **Task** — one simulation configuration. Its `task_id` encodes every grid
  factor, e.g. `t000005__s91001__qhybrid__mfirstnodex__rj000__rc000`.
* **Batch** — the unit of scheduling. One batch = one `flux`/`sbatch` job on one
  exclusive node. `batch_size` tasks run *concurrently inside that node* via
  `flux_fiction.cli.run_ff_parallel`.
* **Campaign root** — `<output_root>/<campaign name>/`, holding all manifests,
  state and outputs.

Everything is **deterministic from the spec**: task ids, shake seeds and rabbit
seeds are derived by hashing the grid coordinates. The same spec regenerates
byte-identical inputs on any host, any time. This is what makes multi-host
splitting safe (§7) and reruns reproducible.

### The process chain

```
launcher (login node)            submits one job per batch
  └─ worker.sh (compute node)    generated per campaign; runs podman
       └─ container              run_worker -> run_ff_parallel
            └─ run_ff (xN)       one per concurrent task
                 └─ flux start   a private Flux instance = the simulated cluster
                      └─ engine  the simulation itself
```

Two facts about this chain drive much of the design:

1. **The simulation runs under `libfaketime`.** Everything below `flux start`
   sees *simulated* time. Its clocks, and therefore its log timestamps and
   `summary.json:generated_at`, are useless for measuring real elapsed time.
   Only the launcher and `run_ff`'s parent process have a true clock.
2. **The container is ephemeral.** Results only exist because they are written
   to the shared filesystem as the run proceeds.

---

## 2. Commands

| command | purpose |
|---|---|
| `example <path>` | write a starter campaign TOML |
| `init <spec>` | create task/batch manifests only |
| `plan <spec> [--limit N]` | init + print the task grid and collapse savings |
| `launch <spec> [--once] [--dry-run] [--poll]` | run launcher ticks |
| `run <spec> [--dry-run] [--poll]` | submit and track until the campaign drains |
| `resume <root\|state.json\|status.json>` | continue an existing campaign |
| `status <root\|spec> [--json]` | queue-aware status (queries the RJMS) |
| `progress <root\|spec> [--json]` | per-policy completion, filesystem-only |
| `results <root\|spec>` | write `results.csv` |
| `run-partial <spec> --hosts <hosts.toml> [--host H]` | run this host's share (§7) |
| `merge --hosts <hosts.toml> [--out P]` | combine all hosts' results |
| `serve <root> [--host H] [--port P]` | HTTP status endpoint |
| `reset <root\|spec>` | **archive** the root to `<name>-archived-<UTC>` |
| `delete <root\|spec>` | permanently remove the root |
| `worker`, `materialize-task` | internal; invoked inside the container |

Most commands accept **either** a campaign spec TOML **or** a campaign root
path; a `.toml` is resolved to its root automatically.

Notable options:

* `--dry-run` — print the submit commands without touching the RJMS. Safe.
* `--poll` — use fixed-interval polling instead of Flux event-watch wakeups.
  Forced automatically on the Slurm backend (Slurm has no event stream).
* `--once` — a single tick, useful under an external scheduler.
* `reset` vs `delete` — `reset` never destroys data; it renames. Both refuse to
  touch a directory lacking `campaign_spec.json`/`tasks.jsonl`/`state.json`.

### Typical session

```bash
python3 -m flux_fiction_ensemble plan   spec.toml          # inspect the grid
python3 -m flux_fiction_ensemble run    spec.toml          # submit + track
python3 -m flux_fiction_ensemble progress spec.toml        # any time, anywhere
python3 -m flux_fiction_ensemble results  spec.toml        # tidy results.csv
```

`run` is idempotent: re-running an existing campaign continues it (it prints a
banner saying so) rather than resubmitting. To start over, `reset` first.

---

## 3. Campaign spec reference

### `[campaign]`

| key | default | notes |
|---|---|---|
| `name` | required | campaign directory name |
| `output_root` | required | parent dir; root is `<output_root>/<name>` |
| `batch_size` | 16 | tasks per job = **concurrent sims per node** |
| `poll_interval_seconds` | 60 | launcher tick / event-wait timeout |
| `walltime_minutes` | 58 | per-batch job walltime |
| `keep_generated_traces` | false | keep per-task `trace.csv` (large) |
| `no_faketime` | false | disable libfaketime (much slower; debug only) |
| `broker_log_level` | 6 | Flux broker verbosity (lower = quieter) |
| `no_broker_log_file` | false | **stop writing `broker.log` entirely** |
| `finalize_reserve_seconds` | 120 | time held back to write partial results (§6) |

> `broker.log` reached **2.3 GB per replica** at level 6 on a 20k-job run, and
> that I/O competes with the simulation you are timing. For throughput work set
> `no_broker_log_file = true` and `broker_log_level = 3`. Trade-off: the
> fatal-error emergency dump keys off scanning `broker.log`, so it will not
> trigger — re-run a single task with logging on to debug a real failure.

### `[input]`

| key | default | notes |
|---|---|---|
| `trace` | required | source CSV |
| `format` | `auto` | `sacct` or `tuolumne` |
| `slice_start` / `slice_count` | 0 / all | positional slice of the CSV |
| `cpus_per_node` | 1 | used to synthesise `NCPUS` for tuolumne traces |

The `tuolumne` normalizer always emits an `NGPUS` column (the sim's trace reader
requires it when the modeled machine has GPUs). Rows with an unparseable submit
time are silently dropped, so `slice_count` is an upper bound.

### `[shake]`

Perturbs the workload to create statistically distinct replicas.

| key | default | notes |
|---|---|---|
| `duplicates` | 100 | replicas; **multiplies the whole grid** |
| `seed` | 1 | base seed; replica *i* uses `seed + i` |
| `job_percentage` | 10 | share of jobs perturbed, **per attribute** |
| `attributes` | `["interarrival"]` | any subset of `interarrival`, `runtime`, `nodes` |
| `degree_seconds` | 60 | max magnitude, `interarrival` only |
| `relative_cap_percent` | none | cap as a share of the local interarrival gap |
| `relative_degree_percent` | 10 | max magnitude for `runtime`/`nodes`, as a share of each job's own value |
| `max_nodes` | inferred | cap on a shaken node count; from the resource graph |
| `attribute` | — | the older scalar spelling of `attributes` |

Set `job_percentage = 0` (or the relevant magnitude to `0`) to replay verbatim.

**Why the magnitude splits in two.** Interarrival gaps have a meaningful
absolute scale, so `degree_seconds` works. Runtimes do not: a trace spans
one-second jobs and day-long ones, and ±60 s either rewrites the former or
vanishes into the latter. Node counts are worse — small integers where any
percentage rounds to zero. So `runtime` and `nodes` draw from
`±relative_degree_percent%` of each job's own value, and the node step is at
least ±1.

Consequences worth knowing:

* Each attribute gets its **own RNG stream**, derived from the replica seed, so
  adding `nodes` does not reshuffle which jobs `interarrival` moved. Attribute
  order in the TOML does not matter. Interarrival keeps the bare replica seed,
  so interarrival-only specs reproduce byte-identically to before this existed.
* A single-node job drawing a downward step **stays at one** — it cannot ask
  for less. That biases the smallest jobs upward: measured on the 10k queueB
  slice at 10%/10%, total node demand rose **1.0%**. It is the same bias in
  every replica, so cross-replica comparison is unaffected.
* Invariants are maintained: `NCPUS`/`NGPUS` are rescaled at the job's own
  per-node ratio, node counts stay within `[1, max_nodes]`, and a job shaken
  past its old `Timelimit` carries the limit up with it.

### `[rabbit]`

| key | default | notes |
|---|---|---|
| `capacity_gib` | inferred | total cluster rabbit capacity |
| `per_rabbit_gib` | inferred | capacity of one rabbit node |
| `ceiling_basis` | `cluster` | what `rabbit_ceiling_percentages` is a % **of** |
| `distribution` | `uniform` | `uniform`/`triangular`/`loguniform`/`gaussian`/`pareto` |
| `field_name` | `RabbitGiB` | trace column written |
| `mean_fraction` | 0.5 | gaussian centre, as a fraction of the ceiling |
| `sigma_fraction` | 1/6 | gaussian spread (±3σ spans the support) |
| `tail_alpha` | 1.5 | pareto tail weight; **smaller = heavier tail** |
| `tail_min_gib` | 1.0 | pareto floor; the scale the bulk clusters near |

**`ceiling_basis` is the single most misunderstood setting.** The ceiling is the
*upper bound* of each job's draw, not the amount it requests, and the
distribution is **not centred on it**:

```
ceiling = ceiling_percent% x (cluster capacity | one rabbit node)
each selected job draws independently from [0, ceiling]
```

On the tuolumne graph one rabbit = 16,308 GiB = **1.389%** of the cluster's
1,174,176 GiB — so the two bases differ by ~72x. With `basis = "cluster"`,
`rc = 10` means the average job wants ~5% of *all* cluster rabbit and only ~20
such jobs can hold storage at once; a 0→100 sweep then explores "rabbit is a
hard global bottleneck", not "light → heavy usage". Use
`ceiling_basis = "rabbit_node"` if you mean "a job may request at most N% of one
rabbit".

Distribution shapes at a fixed ceiling (measured, 20k draws):

| distribution | median | mean | top 1% share |
|---|---|---|---|
| `uniform` | 0.50·ceiling | 0.50·ceiling | 2% |
| `gaussian` (defaults) | 0.50·ceiling | 0.50·ceiling | 1.8% (tighter) |
| `triangular` | 0.42·ceiling | 0.44·ceiling | 2.1% |
| `pareto` α=1.5, floor 1k | 1,578 GiB | 2,860 GiB | 17.8% |
| `pareto` α=1.0, floor 1k | 1,980 GiB | 6,309 GiB | 29.4% |
| `loguniform` | ~0.001·ceiling | 0.08·ceiling | 12.4% |

`pareto` is sampled by exact inverse-CDF over `[tail_min_gib, ceiling]`, so it
never clamps or rejects. As `tail_alpha → 0` it converges to `loguniform`.
**`tail_min_gib` matters as much as `tail_alpha`**: with the default 1 GiB floor
against a multi-TiB ceiling the bulk collapses to ~2 GiB, which is not a
meaningful allocation.

### `[grid]`

| key | default | notes |
|---|---|---|
| `queue_policies` | `["easy"]` | fluxion qmanager policies |
| `match_policies` | `["lonodex"]` | fluxion resource match policies |
| `rabbit_job_percentages` | `[0.0]` | % of jobs requesting rabbit |
| `rabbit_ceiling_percentages` | `[0.0]` | see `ceiling_basis` above |
| `rabbit_distributions` | `[rabbit.distribution]` | request-size distributions to sweep |
| `collapse_zero_rabbit` | true | drop redundant zero-rabbit cells |

Task count = `duplicates × |queue| × |match| × |rj| × |rc| × |dist|`, minus the
collapse.

`rabbit_distributions` sweeps the draw shape as a grid axis, so comparing
gaussian against long-tail is **one campaign** rather than two — one partition
across hosts, one `results.csv`, and a `rabbit_distribution` column to group by.
The task id gains a `__d<name>` tag only when more than one is listed, so
single-distribution campaigns keep the ids and seeds they always had.

**Zero-rabbit collapse.** Rabbit is only written when *both* the job percentage
and the ceiling are non-zero (and capacity is known). Every pairing where either
is zero produces a **byte-identical** trace, so the whole `rj=0` row and `rc=0`
column are the same control run repeated. The launcher keeps **one control per
policy cell** (per queue/match combination — the scheduler config still differs)
and skips the rest. The control is shared across `rabbit_distributions` too: a
trace with no requests in it cannot depend on the shape they were drawn from. On an 11×11 rabbit sweep that removes 20 of 21 degenerate
cells per policy cell, ~17% of the grid, with provably identical coverage. Each
task carries `rabbit_active`; a collapsed control has `rabbit_active = false`
even if one percentage in its name is non-zero.

**Verified backfill reservation depths** (read from the flux-sched source — the
`reservation-depth` policy param does *not* mean what its name suggests):

| policy | effective depth |
|---|---|
| `fcfs` | no backfill at all |
| `easy` | **1**, hard-coded in the constructor; ignores `reservation-depth` |
| `hybrid` | `reservation-depth` (default 64); the **only** policy that reads it |
| `conservative` | `MAX_RESERVATION_DEPTH` (100000) clamped to `queue-depth` |

So setting `reservation-depth` globally in the scheduler JSON affects hybrid
only. Hybrid silently ignores a value ≥ `max-reservation-depth` (the else-branch
is a self-assignment no-op), so it only works because the default max is 100000.

### `[flux]`

| key | notes |
|---|---|
| `base_config` | required; the base `flux_fiction` TOML |
| `base_config_json` | scheduler JSON; policies are patched per task |
| `resource_file` / `resource_R` | the simulated machine (mutually exclusive) |
| `config_overrides` | extra `flux_fiction` keys merged into every task |

A useful override for large campaigns is `make_plots = false`, which skips the
per-run matplotlib render of `resource_utilization.png`.
`resource_usage_timeseries.csv` and `resource_allocations.csv` are still
written, so nothing is lost and the plot can be reproduced later.

### `[submission]`

| key | default | notes |
|---|---|---|
| `queues` | `["pbatch"]` | queue/partition names, tried in order |
| `job_name_prefix` | `ffe` | job-name prefix |
| `backend` | `flux` | `flux`, `slurm`, or `auto` |
| `[submission.queue_limits.<q>] max_active` | none | concurrent jobs cap |
| `[submission.queue_limits.<q>] max_submit_per_hour` | none | rate cap |

An **empty queue name** (`queues = [""]`) submits with no queue attribute — for
a personal Flux instance, which has no named queues. `auto` picks `slurm` when
there is no reachable Flux instance but `sbatch` exists.

---

## 4. Backends

Both submit **one single-node job per batch**; only the verbs differ.

| | flux | slurm |
|---|---|---|
| submit | `flux.job.submit`, `-N1`, exclusive | `sbatch --parsable -N1 --exclusive` |
| list active | `job_list` by name prefix | `squeue -u $USER` |
| list finished | `job_list` incl. inactive | `sacct -X -P --starttime now-7days` |
| cancel | `flux.job.cancel` | `scancel` |
| wakeup | event-watch reactor | polling |

**Slurm specifics learned the hard way:**

* The job is wrapped in `srun -N1 -n1 --cpu-bind=none`. A bare `sbatch --wrap`
  has **no per-task session**, so `/run/user/$UID` does not exist and rootless
  podman fails with `mkdir /run/runc: permission denied`. `srun` provides it.
* `--cpu-bind=none` is explicit because a 1-task step could otherwise be pinned
  to a single CPU depending on site `TaskPlugin` config. On an `--exclusive`
  allocation it was measured permissive (224/224 CPUs), but do not rely on it.
* `worker.sh` also sets `XDG_RUNTIME_DIR` if the current value does not point at
  an existing directory — it is frequently *exported but stale*, so test the
  path, not whether the variable is set.
* Slurm states map onto the Flux row schema; `sacct` decorates cancellations as
  `CANCELLED by <uid>`, which is normalised.

**Flux specifics:** the launcher shells out to `flux python -c` for all bindings
work because `flux python` is a different interpreter (3.6 on TOSS) from the one
running the launcher (3.13) — `import flux` in-process will not work.

---

## 5. Outputs

Per campaign root:

| path | contents |
|---|---|
| `tasks.jsonl` / `batches.jsonl` | the generated grid |
| `campaign_spec.json` | frozen spec snapshot (workers read this) |
| `state.json` | run_id, submitted map, submission history |
| `status.json` | counts, active jobs, per-task progress |
| `progress.csv` | **wall-clock throughput timeseries** (append-only) |
| `results.csv` | one tidy row per task |
| `worker.sh` | generated container launcher |
| `batches/<b>/batch.log` | the job's stdout |
| `batches/<b>/batch_result.json` | terminal state + return code |
| `batches/<b>/parallel/<ts>_manifest/runs/<n>_<task>/child/` | per-simulation output |

Per simulation (`child/`): `summary.json` (makespan, queue waits, utilization,
KVS growth, start-lag percentiles), `output/eventlog.csv` (per-job state
timestamps), `output/resource_allocations.csv` (per-job placement),
`job_transitions.csv`, `resource_usage_timeseries.csv`, `pernode.json` (Chrome
trace), `resource_utilization.png`, plus `reproduce.sh`.

**`progress.csv`** exists because `status.json` is rewritten in place every
time-step, so the *history* is otherwise lost. One short row per task per tick,
stamped from the launcher's real clock — ~890 KB for 24 h at 4 tasks/30 s. This
is the only source for a wall-clock throughput curve.

**`results.csv`** joins grid factors to measured metrics and is written
automatically when the campaign drains, or on demand. It falls back to each
child's `summary.json` when `parallel_summary.json` is missing (see §6), so
truncated runs still report metrics.

---

## 6. Timeouts and partial results

A walltime kill used to leave `output/` **completely empty** — the per-job data
is written only in post-sim analysis. That is now handled:

1. `run_ff`'s parent (not faketime-preloaded, so it has a real clock) watches a
   budget and writes `output/.finalize_now` at `budget − reserve`.
2. The engine stats that sentinel every 10 time-steps and, on seeing it, takes
   the **normal completion path** — full outputs, well-formed `summary.json`.
3. The ensemble derives the budget from the batch walltime automatically.

Measured finalization cost (three real runs): **6 s @ 79 jobs, 37 s @ 6,006,
39 s @ 6,275** — i.e. roughly **5 s fixed + 5.3 s per 1,000 completed jobs**. It
is strongly fixed-cost dominated, so naive linear extrapolation from a small run
over-estimates badly (the 79-job point predicts 456 s at 10k; the truth is ~60 s).

| completed jobs | finalize | suggested reserve |
|---|---|---|
| 1,000 | ~11 s | 60 s |
| 10,000 | ~60 s | **240 s** |
| 20,000 | ~110 s | 360 s |

Scale with the jobs a run is expected to *complete*, not the slice size, and
allow ~3x headroom plus a margin for slower hosts.

Two subtleties:

* The budget is measured from `FLUX_FICTION_WORKER_EPOCH`, stamped by
  `worker.sh` **before** the container image load. Measuring from inside the
  container over-estimates the remaining time by the image-load duration and the
  wall then lands mid-finalization.
* An early finalize leaves hundreds of simulated jobs in flight, and the child
  broker's shutdown must cancel them all (`cleanup.2: flux-cancel: Canceled 331
  jobs`). That teardown can outlast the allocation, so `parallel_summary.json`
  may never appear. **Do not depend on it** — `results.csv` reads the child
  `summary.json` instead.

Timeout reporting: a batch killed at the wall may be recorded either as a
`TIMEOUT` job result *or* as a worker-reported negative return code (`rc=-7` =
killed by signal). Both are reported as `[timeout]`, not `[failed]`.

---

## 7. Splitting one experiment across clusters

`run-partial` runs this host's share of a shared campaign. Because task
generation is deterministic, every host derives identical task definitions with
no coordination — verified by materialising the same task under three host
configs and comparing trace SHA-256s.

`hosts.toml`:

```toml
version = 1
shared_root = "/usr/WS1/.../ensemble-shared"   # must be mounted on EVERY host

[hosts.tuolumne]
backend = "flux"          # flux | slurm | auto
queues = ["pdebug"]
output_root = "/p/lustre5/..."   # this cluster's own filesystem
runs_per_node = 16        # concurrent sims per node -> batch_size
max_active = 8
max_submit_per_hour = 30
walltime_minutes = 55
share = 2                 # relative weight when dividing the grid
hostname_match = ["tuolumne"]
```

```bash
python3 -m flux_fiction_ensemble run-partial spec.toml --hosts hosts.toml            # auto-detects host
python3 -m flux_fiction_ensemble run-partial spec.toml --hosts hosts.toml --host corona --plan-only
python3 -m flux_fiction_ensemble merge --hosts hosts.toml                            # -> shared_root/results_all.csv
```

* Tasks are divided by **weighted round-robin over sorted host names** —
  deterministic, and *interleaved* so each host gets a representative mix of the
  policy grid rather than a contiguous block of one policy.
* Each host's campaign root holds only its own tasks; the campaign name gets a
  `-<host>` suffix so two clusters sharing a filesystem cannot collide.
* `shared_root` must be globally mounted. **Lustre mounts are cluster-specific**
  (tuolumne has lustre5; corona/dane have lustre1) — put the shared context on
  the workspace filesystem and heavy outputs on each cluster's own lustre.
* Changing `share` weights on an existing root is **refused**: it would silently
  re-shuffle tasks under already-submitted work. Use `--force` on a fresh root.

---

## 8. Environment variables

| variable | effect |
|---|---|
| `FLUX_FICTION_WALLTIME_BUDGET_SECONDS` | real seconds before early finalize |
| `FLUX_FICTION_FINALIZE_RESERVE_SECONDS` | seconds reserved to write results |
| `FLUX_FICTION_WORKER_EPOCH` | job start stamp; set by `worker.sh` |
| `FLUX_FICTION_NO_BROKER_LOG_FILE` | suppress `broker.log` |
| `FLUX_FICTION_SCRATCH_HOST_ROOT` | override the node-local scratch root |
| `FLUX_FICTION_FAKETIME_DIR` | libfaketime stamp dir (default `/dev/shm`) |
| `FLUX_FICTION_TERMINAL_RELEASE_GRACE_SECONDS` | retry delay for releasing finished allocations (120) |
| `FLUX_FICTION_WORKSPACE_ROOT`, `_CONTAINER_IMAGE`, `_CONTAINER_IMAGE_TAR`, `_CONTAINER_INSTALLS`, `_CONTAINER_PYTHONPATH` | container plumbing, forwarded to workers |
| `FLUX_FICTION_JOBTAP_SO` | override the compiled jobtap plugin |
| `FLUX_FICTION_WORKER_STOP_SECONDS`, `_WORKER_RESULT_GRACE_SECONDS` | worker shutdown tuning |

---

## 9. Performance and sizing

Measured on the 1153-node tuolumne graph, 20k-job pbatch trace:

| quantity | value |
|---|---|
| RAM per simulation | **~4.5 GB**, flat with job count (4.2 GB is the flux broker) |
| KVS `content.sqlite` | **~378 KB per completed job** (34 KB on a 17-node graph) |
| KVS at 10k jobs | ~3.8 GB per replica |
| simulator throughput | 4–16k jobs in 55 min depending on policy |
| flux instance startup | ~11.5 s per replica (fluxion graph build) |

**Where scratch lives matters.** The child broker derives its rundir — and hence
`content.sqlite` — from `TMPDIR`. `worker.sh` sets it to a bind mount of a
node-local directory chosen as: `FLUX_FICTION_SCRATCH_HOST_ROOT`, else `/l/ssd`
if writable (real NVMe), else `/var/tmp`. This matters because **on tuolumne
compute nodes `/var/tmp` is `tmpfs`, i.e. RAM** — leaving the KVS there charges
GBs against node memory. The container's own overlay is also RAM-backed *and*
routed through fuse-overlayfs, so it is the worst of the options.

The faketime stamp file lives on `/dev/shm` deliberately: with
`FAKETIME_NO_CACHE=1`, libfaketime does a full open/read/close **per clock call**
(~0.02 µs vDSO vs ~20 µs tmpfs vs ~311 µs NFS), and under podman `/tmp` is
fuse-overlayfs.

Sizing checklist before a large campaign:

* `duplicates` multiplies everything — do the arithmetic first.
  `tasks / (runs_per_node × max_active)` × `walltime` = wall-clock per host.
* RAM: `runs_per_node × 4.5 GB` must fit comfortably (16/node ≈ 72 GB).
* Disk: `runs_per_node × 378 KB × jobs_completed` on the scratch root.
* pdebug caps at 1 h everywhere; use pbatch for real runs (24 h on tuolumne).
* Raise `finalize_reserve_seconds` with expected completed jobs.

---

## 10. Gotchas

* **Nothing finishes by default.** A 20k-job slice does not complete in 55 min
  on any tested host; every replica hits the wall. That is fine if you are
  measuring throughput, fatal if you need makespans.
* **`podman` leaves a `catatonit` pause daemon** that keeps the job in `RUN`
  after the task exits (no user systemd session on compute nodes). `worker.sh`
  kills `catatonit`/`conmon`/`fuse-overlayfs` in its EXIT trap; without this,
  jobs hang until walltime.
* **`flux jobs` on a busy login node occasionally returns empty.** The launcher's
  binding-based view is authoritative; do not conclude jobs vanished.
* **A detached launcher survives a disconnected session.** Check whether
  `progress.csv` is still advancing before restarting one — two launchers on one
  campaign will fight.
* **Child timestamps are simulated.** `summary.json:generated_at` reads
  `2020-01-01`. Use `results.csv`'s `wall_seconds` / `startup_seconds` /
  `sim_wall_seconds`, which the parallel runner stamps from a real clock.
* **The jobtap `.so` is resolved from the repo `build/` tree** (or
  `FLUX_FICTION_JOBTAP_SO`). It is architecture-specific in principle; an
  AMD-built plugin was verified to load fine on Intel.
* **Job names carry a `run_id`.** Matching batches to jobs is fluxid-first;
  name-matching is restricted to *active* jobs of the current run to avoid
  adopting a previous generation's dead jobs.

---

## 11. Testing

```bash
PYTHONPATH=src python3 -m pytest src/tests/test_ensemble_generator.py \
    src/tests/test_ensemble_multihost.py src/tests/test_parallel_runner.py -q
```

Coverage includes: task-grid generation and batching, the zero-rabbit collapse
(including a proof that no distinct run is lost), trace normalization, launcher
tick/state reconciliation, terminal-state marking and allocation release, the
Slurm submit shape, distribution bounds/shape/determinism, multi-host
partitioning and merging, and truncated-run metric recovery.

Prefer `--dry-run` and `plan` for exercising configuration; they never touch a
scheduler.
