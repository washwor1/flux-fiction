# Session findings — 2026-07-26

Rabbit threshold sweep: build-out, launch, and the bugs found along the way.
Companion to `FLUXION_PERFORMANCE_FINDINGS.md`, which established the
container-vs-Spack performance gap; this document identifies its cause and
records everything else learned while getting the campaign running.

---

## 1. The headline: container Fluxion was built unoptimized

`podman_containers/scripts/build-flux-sched.sh` ran `cmake` with **no
`-DCMAKE_BUILD_TYPE`**. With that variable empty CMake adds *no optimization
flags at all*, so Fluxion was effectively `-O0`. `flux-sched`'s own
`CMakeLists.txt` does not supply a default either. Spack's package records
`build_type=Release` (`-O3 -DNDEBUG`), which is the entire reason the Spack path
looked faster.

Evidence — same source (`v0.51.0-53-g815a88eb`), same image, only flags differ:

| build | `sched-fluxion-resource.so` |
|---|---|
| no build type (as shipped) | **671,016 bytes** |
| `-DCMAKE_BUILD_TYPE=Release` | **158,568 bytes** |

flux-core is unaffected — it is autotools and `./configure` defaults `CFLAGS` to
`-g -O2`. This was specific to flux-sched, which is the hot path.

### Measured effect

Controlled A/B, one pdebug node, back-to-back, order alternated, same 20-job
probe trace and scheduler config (conservative, queue-depth 2048,
reservation-depth 128, lonodex, full 1153-node graph). All runs completed all 20
jobs:

| build | wall times | median | throughput |
|---|---|---|---|
| `-O0` | 538.5, 452.3 s | **495.4 s** | 2.42 jobs/min |
| `-O3` | 185.1, 179.4 s | **182.2 s** | 6.58 jobs/min |

**2.72×.** For reference the published table had container 414 s / 2.90 jobs/min
and Spack 0.53.0 128 s / 9.38, i.e. 3.23× — so the build flag recovers ~84% of
the Spack ratio, and my `-O0` baseline reproduced the published container number
closely (2.42 vs 2.90 jobs/min).

At **campaign scale the sustained gain is 3–4×** (dane ~1,091 jobs/h vs a ~352
jobs/h `-O0` baseline; corona ~777 vs ~150–259). Larger than the probe because
the campaign runs deep queues where match cost dominates, which is exactly what
`-O3` attacks.

Fixed in `build-flux-sched.sh`:
`-DCMAKE_BUILD_TYPE="${FLUX_SCHED_BUILD_TYPE:-Release}"`, with the numbers in a
comment so the reason survives.

---

## 2. How to change what an ALREADY-QUEUED job will use

This was the most valuable non-obvious mechanism of the session, and the first
approach was wrong.

* **`FLUX_FICTION_CONTAINER_INSTALLS` does NOT reach queued jobs.**
  `_submit_batch` bakes the environment into the jobspec **at submit time**.
  Inspecting a queued tuolumne jobspec showed
  `container-related env in jobspec: NONE` — those jobs were submitted before the
  variable existed anywhere, so setting it later changes nothing for them.

* **`worker.sh` DOES reach them.** The jobspec command is
  `bash <root>/worker.sh <root> <batch-id>`, and worker.sh is read from disk when
  the job *starts*.

* **But do not hand-edit worker.sh.** `ensure_worker_script` ends in an
  unconditional `path.write_text(script)` (campaign.py:1835) and is called from
  `_submit_batch`, so the next submission on that host silently regenerates the
  file and reverts the edit.

* **The robust mechanism is to swap the directory.** worker.sh bakes the literal
  path `/usr/WS1/ashworth12/ff-podman/container-installs`, resolved at container
  start. So:

      mv container-installs    container-installs-o0
      mv container-installs-o3 container-installs

  applies to every job on every host — queued, requeued or future — with no env
  vars, no file edits, and nothing for `ensure_worker_script` to clobber.
  Rollback is two renames.

### Verified, not assumed

Test: submit a `sleep 200`; submit a short flux-fiction run with
`--dependency=afterany:<sleeper>` so it sits in `D` state; swap the directories
while it is queued; let it run. The probe hardcoded the literal prefix path
exactly as worker.sh does. Result:

    host-side size:            158568
    in-container module size:  158568
    ff rc=0 ; jobs_completed: 5

The queued job picked up `-O3` and ran clean. That is why tuolumne's 373 queued
batches needed no further action.

Related: running containers are unaffected by the swap — their bind mounts were
established against the old directory and follow the inode.

---

## 3. Bugs found and fixed

### 3.1 Early finalize never produced summary.json

`Job.queue_wait` was assigned only in `start_job`, never initialized in
`Job.__init__`. The summary walk in `engine.run()` iterates **every** job in the
map and reads it. At natural completion every job has started, so the bug was
**reachable only through the truncation path** — an early finalize ends a run
with hundreds of jobs still queued.

Observed on the calibration: 18/18 sentinels written, 14/18 wrote the complete
per-job CSVs, **0/18 wrote summary.json**, every child dying with

    AttributeError: 'Job' object has no attribute 'queue_wait'

Unfixed, a timed-out cell yields per-job CSVs and an empty `results.csv` — the
same visible symptom as the pre-finalize campaigns, an entirely different cause.
Do not confuse the two when reading old results.

Fixed (`self.queue_wait = None` in `Job.__init__`) and verified: a 4-task rerun
produced **4/4 summaries** and `results.csv` recovered makespan, avg/max queue
wait and job counts for all four timed-out tasks (16 of 21 columns populated,
versus 0 metrics before).

### 3.2 A transient Lustre stat killed whole batches

`_read_json_dict` in `parallel/runner.py` had `if not path.exists()` **outside**
its try block. `Path.exists()` only swallows a fixed set of errnos, so a
transient filesystem error under load (Lustre returning EIO/ESTALE) escaped
`os.stat`, unwound out of the parent's status-refresh loop, and killed the batch.

**117 of dane's 374 batches died this way after 3–4 hours of healthy running**,
each taking 16 live child runs with it. A representative one died at 58,838 of
160,000 jobs with all 16 replicas alive. Total loss: **0 summary.json across all
1,872 tasks** in those batches, roughly 6 M simulated jobs of compute.

Why dane and not corona: dane ran 374 batches × 16 replicas ≈ 5,984 concurrent
simulations against lustre1 versus corona's 128 — about 47× the I/O pressure.

Fixed two ways: dropped the unguarded `exists()` pre-check (opening directly is
simpler *and* safe — a missing file raises `FileNotFoundError`, already handled),
and wrapped `refresh_parent_status` so purely observational bookkeeping can never
abort a run. Also added the module `logger` that the new handler needs.

### 3.3 Unfixed: `t_submit` is timezone-dependent

`shake_interarrival` writes the `t_submit` column with `datetime.timestamp()` on
a **naive** datetime, so the epoch is interpreted in the generating host's local
timezone. Materializing the same task on the tuolumne login node (PST for a
January trace date) versus inside the UTC container gives traces differing by
exactly 28,800 s, on precisely the 10% of rows the interarrival shake touched.

Not corrupting current results — within one host every row is consistent, and a
uniform epoch shift just translates the timeline — but it breaks the
cross-host byte-identical determinism the multi-host design rests on, and means
`reproduce.sh` outside the container does not reproduce a campaign's inputs.
Fix is to attach `timezone.utc` before `.timestamp()`; note that changing it
alters every generated trace, so do not land it mid-campaign.

---

## 4. Features added

| feature | switch | outcome |
|---|---|---|
| async job submission | `async_submit` (default off) | **no throughput gain** |
| shaking runtime + node count | `shake.attributes` | works |
| rabbit distribution as a grid axis | `grid.rabbit_distributions` | works |
| skip the utilization plot | `make_plots = false` | works |
| requeue failed batches | `retry-failed` subcommand | works |

### Async submission — correct but pointless

`flux.job.submit()` is `submit_async()` immediately followed by
`submit_get_id()`, one blocking round-trip per job. The new path splits them and
drains every future in `_drain_pending_submits()`, called at the top of
`_send_quiescence_probe()` — the single sufficient barrier, since
`_flush_pending_quiescent_expect` (which issues `accumulate_quiescent`) is
reached only from inside the probe.

Verified equivalent on `fcfs_gaussian`, a deterministic case: makespan
41837.75627183914 s and both queue-wait figures **bit-identical**;
`job_transitions.csv` identical in SUBMIT/START/FINISH/FLUX_OBSERVED_START/
NODELIST with only the REAL\_\* wall-clock columns differing;
`resource_allocations.csv` identical in all 11 substantive columns;
`resource_usage_timeseries.csv` byte-identical. Only Flux-assigned jobids
(70/500, FLUID embeds a ms timestamp) and KVS byte counts (0.18%) differ.

**No throughput difference** (0.99–1.03× across 18 tasks). Submission was never
the bottleneck — a few hundred submits over five minutes, versus simulated-clock
advance and scheduler work. Leave it off absent a profile showing otherwise.

**Note on choosing a verification case:** `easy_gaussian` is the *worst* case in
the Salishan suite for this purpose — 333× spread in `notebook_util_error`
across four runs with no code change. A single run there cannot distinguish a
real bug from the noise floor. Use a deterministic case (`fcfs_gaussian`) for
equivalence checks.

### Shaking runtime and nodes

Interarrival keeps absolute `degree_seconds`; runtime and nodes use
`relative_degree_percent` (±% of each job's own value), because runtimes span
1 s to a day and node counts are small integers where any percentage rounds to
zero. Each attribute gets its own RNG stream so adding one does not reshuffle
the others; interarrival keeps the bare replica seed so old specs reproduce
byte-identically.

**Honest artifact:** a single-node job drawing a downward step stays at one — it
cannot request less. Measured on the 10k slice at 10%/10%, total node demand
rose **1.0%**. Identical in every replica, so cross-replica comparison is
unaffected.

---

## 5. Trace preparation

The raw export (`tuo_data_chrono_clean.csv`) needed three fixes, each a real
correctness problem:

1. **Queue mixing.** Only `queueB` is pbatch-representative — 230,052 of 289,850
   records and essentially all the load. queueA/C together offer ~1%.
2. **Ordering.** The file is sorted by `@timestamp` (job *end*), and only ~75% of
   adjacent rows ascend by `job.submittime`. The interarrival shake computes each
   gap from adjacent rows, so out-of-order rows give negative interarrivals.
3. **Straggler head.** A positional slice picks up jobs that merely *ended* in
   the window but were submitted weeks earlier. Under 1% of rows prepended
   **753 hours** of simulated dead time, which would dominate makespan and
   utilization — the very quantities under study.

Selecting a contiguous *submit-time* window fixes 2 and 3 together.

Window selection needs three criteria, not just size (`util/make_queueb_slice.py`):

* **offered load near saturation** — below ~90% the cluster is rarely full,
  makespan tracks the arrival span regardless of policy, and the sweep reads flat
* **no multi-hour submission gap** — an idle gap drains the queue and resets
  accumulated fragmentation
* **not a job-array burst** — the trace contains several bursts of ~10k
  *single-node* jobs inside an hour. They score beautifully on load (470%) and
  gap (0.1 min) and are useless: identical 1-node jobs cannot fragment anything.

Chosen: rows 174500:184500, 2026-01-16 09:44 → 01-22 13:09 — **96.8% offered
load, 60.1 min worst gap, 7.7% busiest-hour share**, 6.14 days.

Workload shape worth knowing: **1-node jobs are 69.9% of jobs but 25% of the
node-hours; jobs of 9+ nodes are 6.7% of jobs and 48% of node-hours.** The wide
jobs are the ones fragmentation can block.

---

## 6. Simulator performance characteristics

**The job rate is arithmetic, not pathology.** The trace holds 10,000
completions across 147.4 simulated hours = 68 completions per simulated hour, so

    jobs/min (wall) = 68 × (real-time speedup) / 60

Measured speedups ranged 1.8× (rabbit-saturated corner) to 69× (easy/firstnodex
control) on the `-O0` build. The "2 jobs/min" that looked alarming is exactly
68 × 1.8 / 60. Nothing was stuck.

24 hours buys 147.4 simulated hours only at **≥6.1× real time**.

### What actually determines scheduling performance

Per the Fluxion paper's own measurements and confirmed against this campaign,
**per-job scheduling time falls sharply over roughly the first 50 jobs and then
holds a steady state**, with occasional spikes. It does not trend over a run.
The determinants of that steady state are:

1. **queue policy** — how many match operations each scheduling loop performs
2. **match policy** — the traversal strategy
3. **number of jobs in the queue** — more candidates, more matching
4. **raw per-match compute time** — hardware and environment (this is what the
   `-O0` → `-O3` fix moved, and nothing else)
5. **allocation size** — this is graph scheduling, so a wide job traverses many
   more vertices before finding a full match, while a 1-node request only walks
   to tree depth and matches. Job size distribution is therefore a first-order
   input to scheduling cost, not a detail.

That model retro-explains the numbers measured here, which had been described
only loosely:

* **firstnodex vs lonodex differing ~10×** — match policy (3).
* **easy → hybrid → conservative** tracking effective reservation depth
  (`easy` 1, hard-coded and ignoring the param; `hybrid` = `reservation-depth`
  = 128; `conservative` clamped to `queue-depth` = 2048) — queue policy × queue
  depth (1, 3) driving the *number* of matches per loop. Conservative at 2048
  against a deep queue is a deliberately expensive corner, not Fluxion's normal
  operating point.
* **Cost per timestep varying ~16×** (0.5 s to 8.8 s) on identical hardware —
  a composite of (1), (3) and (5).
* **The rabbit-saturated corner being slow.** Earlier attributed to capacity
  limits; the better mechanism is (5) — an unsatisfiable job must traverse the
  whole search space *before* failing, and rabbit requests add storage vertices
  to walk. It is expensive because it fails, not merely because it is blocked.
  Consistent with that corner gaining only 1.2–1.3× from the faster build.

### Correction: two wrong claims made earlier in this session

* **"Rate rises over a run (S-curve)."** Wrong. Corona's heavy cells showed
  89 jobs/h over their first 6 h and 252 over the full 8.2 h, implying ~696
  jobs/h in the final 2.2 h. Under a steady per-match cost that cannot be a
  scheduler effect — it is the **workload replay**. The slice has a 4,941 s
  median runtime at 96.8% offered load, so few jobs have retired early and
  completions then bunch as long-running jobs finish together; the trace also
  carries real diurnal structure. Add the genuine first-~50-job warmup and the
  apparent S-curve is fully accounted for without the scheduler changing speed.
* **"Extrapolating from an early window understates final throughput."** Not a
  general rule — it followed from the artifact above. Wall-clock completion rate
  is simply not a stationary quantity, so extrapolate from it in either
  direction only with the workload's structure in hand.

### Consequence for this campaign's design

Node-count shaking perturbs **match cost directly** via (5), unlike interarrival
shaking which does not. Replica variance in scheduling time is therefore
expected, and it is not neutral noise. Relatedly, cells differ in match cost by
workload composition: this slice is 69.9% single-node jobs (cheap, tree-depth
matches) yet 48% of its node-hours sit in jobs of 9+ nodes (expensive, wide
traversals).
* **Do not model wall-clock completion rate as a property of the scheduler.**
  It mixes three unrelated things: per-match scheduler cost, the trace's own
  completion structure, and queue-depth evolution. Two claims made earlier in
  this session were wrong and are corrected below.
* **The rabbit-saturated corner is capacity-limited, not match-limited.** It
  gained only 1.2–1.3× from the host/Spack path while other cells gained 9–17×.
  No build optimization will rescue it. At `rc=100` gaussian the median request
  is ~8,154 GiB — half a rabbit node — and with 1.17 PiB total only ~140 jobs can
  hold storage at once.
* **16-way packing costs nothing.** The identical task run solo vs alongside 15
  siblings: **1.005×** end-to-end, 0.99–1.05× throughout 55 minutes. Both curves
  plateau *together*, showing the plateau is a property of the simulated
  timeline, not contention.
* **Pinning (`flux_launch`) is net-negative.** With it on, every
  easy/firstnodex cell regressed — one by 6.7× — while slow cells gained 15–17×.
  An exclusive core block stops a fast replica borrowing capacity its idle
  siblings leave free. Left **off**.

### Finalize reserve sizing

Post-sim analysis took **17–257 s** per task at 16-way packing, and 4 of 18 never
reached it inside a 240 s reserve. `EARLY_FINALIZE_CHECK_EVERY = 10` timesteps
means noticing the sentinel alone can cost ~80 s at 8 s/timestep. Campaign now
uses **900 s**.

Even then, after summary.json is written the child broker must cancel the
in-flight simulated jobs, and that teardown can outlast the allocation. The
worker then never writes `batch_result.json` and the launcher marks the batch
`failed` **despite every task having produced complete data**. `results.csv`
normalizes such a batch to `timeout` and recovers the metrics — so **trust
`results.csv` over `status`**.

---

## 7. Operational lessons (LC-specific)

* **Long-lived launchers get reaped.** Dane's died repeatedly, twice at the same
  instant, after a few minutes; `nohup`, `setsid` and `loginctl enable-linger`
  all failed to prevent it (though one earlier launcher survived 7.5 h, so it is
  intermittent). Corona's died after ~8.4 h, tuolumne's after ~13 h.
  **Solution: short cron ticks instead of a daemon** —
  `util/ensemble_tick.sh <host>`, one `run-partial --once` (~10 s), flock-guarded
  so ticks cannot overlap. It deliberately does **not** auto-retry failures,
  since that would happily burn an allocation re-running a broken configuration.
* **Never add cron on a host that still has a live launcher** — two tickers can
  double-submit a batch.
* **`ssh <cluster>` round-robins across login nodes, and crontabs are
  per-node.** `crontab -l` from a different node shows nothing even when cron is
  running fine — this produced a false "corona has no cron" alarm. **Check the
  tick log on shared Lustre, not `crontab -l`.** A stale log is the real signal.
* **Liveness checks are treacherous.** `ps -eo args | grep "... run-partial"`
  gives false **negatives** because `ps` truncates args; a plain
  `pgrep -f flux_fiction_ensemble` gives false **positives** by matching your own
  ssh command line. Use `pgrep -f 'flux_fiction[_]ensemble'`, or better, check
  whether the log is still gaining lines.
* **Lustre mounts are cluster-specific** — tuolumne has lustre5, dane/corona
  have lustre1. Each host must run its own `run-partial`; a campaign root on the
  wrong filesystem is simply unwritable. `ensemble_tick.sh` now picks the first
  writable log location rather than hardcoding.
* **Default `python3` on dane/corona is 3.6.8** with no `tomllib` and no `tomli`.
  Use `/collab/usr/gapps/python/toss_4_x86_64_ib/anaconda3-2025.3.1/bin/python3`
  (3.13.2, present on all three).
* **Detaching over ssh makes the ssh call hang** until its timeout, because the
  nohup'd process holds stdout. The launcher survives; a timeout there is not a
  reason to relaunch — check the queue.
* **Compute-node scratch differs per cluster** (decides where the child broker's
  `content.sqlite` lands; login nodes differ from compute nodes on all three):
  tuolumne `/l/ssd` 200 GB disk; corona `/l/ssd` 3.5 TB NVMe; **dane has no
  local disk at all** — `/var/tmp` is a symlink to `/tmp`, both tmpfs — so dane
  charges ~60 GB of KVS against RAM on top of ~72 GB of simulator, ~132 GB of
  251 GB. It fits, without headroom.
* **`cp -a` fails with spurious ENOENT** on the setgid NFS `container-installs`
  prefix, leaving a partial copy. Use `tar` piped through a subshell.

---

## 8. Mistakes made this session (worth not repeating)

* **Asserted a fix was applied when it was not.** Both the Spack runtime and the
  `-O3` fix were absent while the campaign ran on `-O0`; I had flagged the
  decision at launch but buried it, and did not re-verify before reporting.
  Verify with `stat`/config resolution, not memory.
* **Proposed an env-var mechanism that could not work** for already-queued jobs,
  and would have failed silently. Caught only by inspecting a real jobspec.
* **Called a live launcher dead, and a dead one alive**, from unreliable
  `ps`/`pgrep` patterns (see above).
* **Segment-detection error produced a false "0.88× — slower" reading** when
  comparing `-O0` to `-O3`: taking each task's *last* reset picked up an earlier
  requeue that was also `-O0`. Caught because the reported window (60.5 min) was
  impossible for batches 20 min old. Anchor comparisons on an explicit cutoff
  timestamp.
* **Quoted a 9× speedup from a 4-minute window.** That measures startup latency
  (fluxion graph build), not sustained throughput; the per-task spread
  (p25 4.2×, median 17×, p75 69.7×) was the giveaway. Sustained is 3–4×.
* **Generalized a confounded metric into a scheduler property.** Reported that
  simulated-job completion rate "rises over a run", when that metric mixes
  per-match cost with the trace's completion structure and queue depth. Corrected
  in §6 — per-job scheduling time reaches a steady state after ~50 jobs and does
  not trend. Measure the thing you want to claim about: per-match time, not
  jobs/hour over wall clock.
* **Four false starts in the benchmark harness**, each producing a plausible-
  looking but meaningless number: `flux-fiction-run` is not on PATH in the image
  (use `python -m` against the live tree); the flag is `--run-dir`, not
  `--run-root`, and it must not pre-exist; exporting `PYTHONPATH` *inside* the
  container clobbers the flux bindings that `flux-dev-env.sh` appends (pass it
  via `-e` instead); and a stray pair of double quotes in a comment inside a
  `bash -lc "…"` string terminated the command early. **A suspiciously fast run
  is a failed run** — always check `jobs_completed`.
* **Over-stated a caution:** claimed swapping the build mid-flight would make
  batches "non-comparable". Wrong for the science — `-O3` vs unoptimized is the
  same deterministic algorithm, so only wall-clock throughput differs. The real
  hazard was narrower: `cmake --install` overwrites `.so` files in place under
  live brokers.

---

## 9. Campaign as launched

12,060 tasks / 763 batches, 24 h walltime each, all submitted upfront with no
concurrency cap on tuolumne or dane.

| host | backend | tasks | batches | packing |
|---|---|---|---|---|
| tuolumne | flux | 5,964 | 373 | 16/node |
| dane | slurm | 5,969 | 374 | 16/node |
| corona | flux | 127 | 16 | 8/node |

Spec `test-inputs/ensemble-rabbit-threshold-sweep.toml`, shares 47/47/1 in
`test-inputs/hosts-rabbit-threshold.toml`, shared context
`/usr/WS1/ashworth12/ff-podman/ensemble-shared-rabbit-threshold`.

Grid: 10 replicas × 3 queue policies × 2 match policies × 11 rabbit-job-% × 11
rabbit-ceiling-% × 2 distributions, minus 2,460 collapsed zero-rabbit cells
(60 controls survive — one per policy cell per replica, shared across both
distributions).

`ceiling_basis = "rabbit_node"` so rc=100 means one full rabbit (16,308 GiB);
long-tail floor `tail_min_gib = 100` (the 1 GiB default collapses the bulk to
~2 GiB against a 16 TiB ceiling).

**User decision:** cells that cannot finish inside 24 h are excluded from the
analysis rather than resized — a cell that cannot drain is itself evidence of
poor utilization.

### Reading the results

Makespan is only comparable between cells that got through similar job counts.
For truncated cells use jobs-completed-in-fixed-budget, achieved utilization (a
rate), or makespan truncated to a common job count from `output/eventlog.csv`.

### Early scientific signal (n=1, extreme corner, truncated — a hint only)

| cell | completed | avg queue wait |
|---|---|---|
| easy/firstnodex, control | 212 | **1.0 s** |
| easy/firstnodex, rj=100/rc=100 | 107 | **124.0 s** |
| easy/lonodex, control | 204 | 1.0 s |
| easy/lonodex, rj=100/rc=100 | 83 | 5.8 s |

A ~124× jump in average queue wait from adding rabbit demand, and a large
match-policy interaction (lonodex 5.8 s where firstnodex shows 124 s under the
same load) — the sort of effect the sweep is designed to map.

---

## 10. Files touched

**Fixes**
* `podman_containers/scripts/build-flux-sched.sh` — `CMAKE_BUILD_TYPE=Release`
* `src/flux_fiction/_core/models.py` — `Job.queue_wait` initialized
* `src/flux_fiction/parallel/runner.py` — `_read_json_dict` hardened,
  `refresh_parent_status` guarded, module `logger` added

**Features**
* `src/flux_fiction/_adapters/{base,flux/adapter,mock/adapter}.py` — async submit
* `src/flux_fiction/_core/engine.py` — async submit wiring, `make_plots`
* `src/flux_fiction/api/config.py` — `async_submit`, `make_plots`
* `src/flux_fiction/_outputs/vis.py` — honor `make_plots`
* `src/flux_fiction/ensemble/trace.py` — `apply_shake`, runtime/node shaking
* `src/flux_fiction/ensemble/config.py` — shake attributes, distribution axis
* `src/flux_fiction/ensemble/campaign.py` — distribution axis, `retry-failed`
* `src/flux_fiction/ensemble/cli.py` — `retry-failed` subcommand

**Tools**
* `util/make_queueb_slice.py` — trace window selection
* `util/ensemble_tick.sh` — cron-driven single tick
* `util/build_fluxion_o3.sh` — Release build into a separate prefix
* `util/bench_fluxion_o0_vs_o3.sh` — the controlled A/B
* `util/test_queued_swap.sh` — the queued-job swap test

**Tests** — `src/tests/test_async_submit.py` (new, 7 cases),
`src/tests/test_ensemble_generator.py` (+12 cases)

**Writeup** — `../rabbit-threshold-writeup/rabbit-threshold-experiment.ipynb`
(executed, 9 figures) plus `build_notebook.py` to regenerate

### Known pre-existing test breakage

* `test_status_reporting.py::test_engine_writes_summary_file` — `engine.py` calls
  `adapter.get_scheduler_metrics()`, which `FluxAdapter` has but `MockAdapter`
  does not, and which is not declared on the base `Adapter`. Real runs
  unaffected.
* `test_benchmark_utility.py::test_benchmark_utility_compare_mode` — flaky;
  asserts a wall-clock delta between two subprocess runs is positive, so noise
  flips the sign (passes ~2 of 3).
