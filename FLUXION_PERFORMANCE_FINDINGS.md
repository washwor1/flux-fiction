# Fluxion / Flux Fiction Performance Findings

Date: 2026-07-26

This is a handoff note for continuing the Flux Fiction / Fluxion performance investigation on Tuolumne.

## Executive Summary

The dominant slowdown is the container Fluxion path, not Flux Fiction Python itself. Running the same Flux Fiction workflow against host/Spack Fluxion modules improves end-to-end throughput from about 2.90 jobs/min to 9-11 jobs/min on the 20-job probe.

The latest Fluxion release package tested here is `flux-sched@0.53.0` plus only the personal-repo `sched.quiescent` RPC patch. It built and benchmarked successfully without the ERANGE-to-EBUSY planner patch, so the EBUSY patch was not added.

The newest tested `0.53.0 + quiescent RPC` build is still good compared with the container path, but it is modestly slower than the earlier Spack `0.43.0` build:

| Path | Full FF wall time | Throughput | Fluxion match avg | Matches | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Container optimized | 414s | 2.90 jobs/min | 1.1009s | 332 | `submit_novalidate`; container Fluxion modules |
| Spack Fluxion 0.43.0 | 109s | 11.01 jobs/min | 0.2139s | 358 | host modules, quiescent-capable local checkout |
| Spack Fluxion 0.53.0 + quiescent RPC | 128s | 9.38 jobs/min | 0.2452s | 358 | latest release plus quiescent RPC only |

All three full-workflow rows use the same 20-job probe class and the full Tuolumne synthetic resource graph, not a pdebug-sized graph. The pdebug queue was only used to run benchmark jobs on a compute node.

## Spack Package Change

Package file:

`/usr/WS1/ashworth12/package_managers/spack/var/spack/repos/builtin/packages/flux-sched/package.py`

Patch file:

`/usr/WS1/ashworth12/package_managers/spack/var/spack/repos/builtin/packages/flux-sched/qmanager-quiescent-rpc.patch`

Changes made:

- Added `version("0.53.0", sha256="21726fcaf589cbc2f13b3339e04f668a197e2642d1a8484e5c45819864cc712d")`.
- Added `patch("qmanager-quiescent-rpc.patch", when="@0.53.0")`.
- The patch is from personal repo commit `367c8e21fbdc3289c8d44f8bf327e0227b5f9921` (`qmanager: add sched.quiescent RPC`).
- Did not include personal repo commit `4a32e1711987122668d1dd62bdeb631429eef380` (`resource/planner/c/planner_c_interface.cpp`: ERANGE to EBUSY).

The package file already had local Python build wiring before this change:

- `depends_on('python', type=('build'))`
- `depends_on('py-jsonschema', type=('build'))`
- `Python3_EXECUTABLE` CMake argument

I preserved those existing dirty changes.

Installed prefix:

`/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/flux-sched-0.53.0-rben2bzg2txikxftd7w34mcio3u6mzdc`

Build ran under pdebug as job `f3NURfasDCL7` and completed successfully. Spack applied `qmanager-quiescent-rpc.patch`; build+install took 46.42s inside the job.

The `emulator` Spack environment still has:

```yaml
develop:
  flux-sched:
    spec: flux-sched@0.43.0
    path: /g/g14/ashworth12/workspace/flux-sched
```

So an explicit `spack -e emulator spec flux-sched@0.53.0` fails because the develop entry pins `0.43.0`. For the 0.53.0 build I used the emulator config as a config scope instead:

```bash
/usr/WS1/ashworth12/package_managers/spack/bin/spack \
  -C /usr/WS1/ashworth12/package_managers/spack/var/spack/environments/emulator \
  install --fail-fast flux-sched@0.53.0%gcc@12.2.0 ^flux-core@0.78.0
```

## Benchmark Setup

Primary benchmark wrapper:

`/g/g14/ashworth12/workspace/ff-podman/flux-fiction-develop/util/run_spack_flux_fiction_probe_pdebug.sh`

0.53.0 run command:

```bash
flux batch -q pdebug -N1 -n1 -t 45m \
  --job-name=ff-fluxion053-qrpc \
  --cwd=/g/g14/ashworth12/workspace/ff-podman/flux-fiction-develop \
  --env=SPACK_FLUXION_PREFIX=/usr/WS1/ashworth12/package_managers/spack/opt/spack/linux-rhel8-x86_64/gcc-12.2.0/flux-sched-0.53.0-rben2bzg2txikxftd7w34mcio3u6mzdc \
  --env=RUN_FF_SYSTEM=0 \
  --env=PROBE_ROOT=/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-fluxion053-quiescent-probe \
  --env=JOB_LIMIT=20 \
  util/run_spack_flux_fiction_probe_pdebug.sh
```

Benchmark job:

`f3NUSeErKMo5`

Artifacts:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-fluxion053-quiescent-probe/artifacts/20260726_021430`

Inputs:

- Resource graph: `/usr/WS1/ashworth12/ff-podman/resource_graphs/tuolumne.json`
- Scheduler config: `pbatch`, conservative policy, `queue-depth=2048`, `reservation-depth=128`
- Graph size in scheduler stats: `V=119042`, `E=119041`
- Probe trace: `/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-probe-20/queueb_gaussian_rabbit.csv`

## Latest 0.53.0 Results

Direct resource-query probe:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-fluxion053-quiescent-probe/artifacts/20260726_021430/spack_direct_rv1_shorthand.json`

- 20 measured matches
- total wall: `5.958s`
- avg match: `0.295348s`
- p50 match: `0.286274s`
- p95 match: `0.434464s`
- matches/sec: `3.3568`
- errors: none

Full Flux Fiction probe:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-fluxion053-quiescent-probe/artifacts/20260726_021430/spack_ff_rv1_shorthand/summary.json`

- status: succeeded
- full wall from status file: `128s` (`2026-07-26T09:15:01Z` to `2026-07-26T09:17:09Z`)
- throughput: `9.375 jobs/min`
- jobs completed: `20/20`
- quiescence epochs: `40`
- stale quiescence replies: `0`
- successful resource matches: `358`
- failed resource matches: `0`
- resource match avg: `0.245206s`
- resource match max: `0.438780s`
- resource load time: `22.448s`
- reserved actions: `318`
- Flux observed start lag avg: `1.249s`
- KVS growth: `286,924.8 bytes/completed job`

I searched the 0.53.0 build and benchmark artifacts for `ERANGE` and `EBUSY`; there were no hits. Therefore the EBUSY patch should stay out unless a future workload reproduces an ERANGE failure.

## Comparison Against Earlier Runs

Spack 0.43.0 host run:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-flux-fiction-probe/artifacts/20260726_013803`

- Full FF wall: `109s`
- Throughput: `11.009 jobs/min`
- Full FF resource match avg: `0.213855s`
- Direct resource-query avg: `0.215999s`
- Matches: `358`
- Quiescence epochs: `40`
- Stale quiescence replies: `0`

Container optimized run:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-probe-novalidate-20/artifacts/20260726_001615/ff_rv1_shorthand`

- Full FF wall: `414s`
- Throughput: `2.899 jobs/min`
- Full FF resource match avg: `1.100863s`
- Matches: `332`
- Quiescence epochs: `40`
- Stale quiescence replies: `0`

Container direct Fluxion RPC probe:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-direct-faketime/artifacts/20260726_003934/container_rpc_rv1_shorthand_nofake.json`

- Direct resource-query avg: `1.193096s`
- 50 measured matches

System/vanilla direct Fluxion probe:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/sched-loop-probe/artifacts/20260725_233659/vanilla_rv1_shorthand.json`

- Direct resource-query avg: `0.419108s`
- 50 measured matches

Important context: the system/vanilla direct number was from a child Flux instance loaded with the same kind of synthetic Tuolumne resource graph and scheduler config. It was not a cheaper pdebug-sized queue query. The pdebug queue was only the allocation used to run the benchmark.

System Fluxion full Flux Fiction attempt:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/codex-perf/spack-flux-fiction-probe/artifacts/20260726_014257/system_ff_rv1_shorthand`

- Not a valid throughput comparison.
- It failed/stalled because system Fluxion does not implement `sched.quiescent`.
- This is why the quiescent RPC patch is required for a full Flux Fiction run against a vanilla/newer Fluxion.

## Optimizations / Fixes Found

Use host/Spack Fluxion modules instead of the container Fluxion modules.

- This is the main win.
- Container full path: about `2.90 jobs/min`.
- Host Spack path: about `9-11 jobs/min`.
- Direct match loop cost drops from about `1.19s` in the container to `0.216-0.295s` on host/Spack.

Add module path overrides to Flux Fiction.

- File: `/g/g14/ashworth12/workspace/ff-podman/flux-fiction-develop/src/flux_fiction/_adapters/flux/modules.py`
- Environment variables added earlier:
  - `FLUX_FICTION_RESOURCE_MODULE`
  - `FLUX_FICTION_FLUXION_RESOURCE_MODULE`
  - `FLUX_FICTION_FLUXION_FEASIBILITY_MODULE`
  - `FLUX_FICTION_FLUXION_QMANAGER_MODULE`
- These allow Flux Fiction to reload scheduler/resource modules from a specific Spack prefix while still using system Flux core.

Build the Flux Fiction jobtap plugin against the host Flux core.

- The pdebug wrapper builds `src/emu-jobtap.so` with `-Duse_system_flux=true -Dflux_prefix=/usr`.
- This avoids dragging the container Flux core/sched stack into the host benchmark.

Use `TMPDIR=/tmp` for nested Flux runs.

- Flux creates AF_UNIX sockets under `TMPDIR`.
- Deep Lustre artifact paths can exceed the socket path limit.
- The wrapper sets `TMPDIR=/tmp` for full Flux Fiction runs to avoid that failure mode.

Keep libfaketime stamp files off Lustre.

- The wrapper sets `FLUX_FICTION_FAKETIME_DIR=/dev/shm`.
- The large progress-bar times in logs are simulated time, not wall time.
- libfaketime was not the root cause of the container slowdown; direct no-faketime container probing was still about `1.19s/match`.

Use `--flags=novalidate` in the ensemble launcher path where appropriate.

- The optimized container artifact used the novalidate path and succeeded.
- This helps submission overhead, but it does not remove the main container Fluxion match-loop cost.

Use the quiescent RPC for full Flux Fiction.

- Full Flux Fiction needs `sched.quiescent`.
- System Fluxion without that RPC cannot complete the same full workflow.
- The 0.53.0 package with `qmanager-quiescent-rpc.patch` returned 40 quiescent epochs and zero stale replies.

Do not add the ERANGE-to-EBUSY patch by default.

- The latest 0.53.0 + quiescent RPC package ran direct and full probes without ERANGE.
- Only add the one-line planner patch from `origin/debug_erange` if a future workload actually reproduces ERANGE.

## Practical Next Steps

For production-like Flux Fiction runs on Tuolumne, prefer a host/Spack Fluxion prefix and explicit module overrides over the container Fluxion stack.

If another agent continues this:

1. Keep the Spack package at `0.53.0 + qmanager-quiescent-rpc.patch` first.
2. Do not include the ERANGE-to-EBUSY patch unless an ERANGE failure is observed in artifacts.
3. Benchmark on pdebug, but keep the synthetic input graph/config pbatch-sized when comparing scheduler cost.
4. Compare both direct resource-query loop cost and full Flux Fiction wall throughput; direct match cost alone does not include all wrapper overhead.
5. Treat system Fluxion full-run failures as missing `sched.quiescent` unless logs prove otherwise.

## Ensemble Spack Runtime and Pinning Follow-Up

A Spack runtime path was added to the ensemble launcher after the initial
Fluxion probe work. The campaign spec now supports:

- `campaign.worker_runtime = "spack"` to run workers directly on the host with
  system Flux core and Spack Fluxion modules instead of Podman.
- `campaign.flux_launch = true` to turn on the existing node-local `flux start`
  / `flux run --cores-per-task=... -o cpu-affinity=default` replica pinning.
- Optional Spack path overrides for Fluxion, faketime, GCC runtime, Spack view,
  ninja, Meson, host jobtap compiler, and host TMPDIR.

Important implementation details:

- Host/Spack workers build `src/emu-jobtap.so` on the compute node with
  `-Duse_system_flux=true -Dflux_prefix=/usr`.
- Host/Spack workers write a modprobe overlay pointing
  `sched-fluxion-resource`, `sched-fluxion-feasibility`, and
  `sched-fluxion-qmanager` at the Spack Fluxion prefix.
- The worker must clear inherited system Flux variables before invoking
  `run_ff_parallel`: `FLUX_URI`, `FLUX_JOB_ID`, `FLUX_KVS_NAMESPACE`, and
  `FLUX_INSTANCE_LEVEL`. Without this, `flux run` submits the 16 replicas back
  to the system Flux instance, where they appear as unintended `pbatch` jobs.
- Do not put the whole Spack view in `LD_LIBRARY_PATH` for the host worker. It
  can make system Flux shell load Spack ncurses/tinfo under
  `/usr/lib64/lua/5.3/posix.so`, causing `undefined symbol: luaopen_posix`.
  The working runtime path is only:
  `SPACK_GCC_RUNTIME/lib:SPACK_FLUXION_PREFIX/lib64:SPACK_FLUXION_PREFIX/lib`.
- Pinning must size from the process CPU affinity/cpuset, not whole-node sysfs.
  The pdebug allocation exposed 80 physical cores to the worker; whole-node
  sysfs reported 96. Affinity-aware sizing produced `--cores-per-task=5`, so
  all 16 replicas started. Whole-node sizing produced `--cores-per-task=6`, and
  only 14/16 replicas could start.

16-pack throughput artifact:

`/p/lustre5/ashworth12/flux-fiction-ensemble-tuo/rabbit-threshold-calib-async-spack`

- Spec: `test-inputs/ensemble-rabbit-threshold-calib-async-spack.toml`
- Worker runtime: host/Spack
- Fluxion: `0.53.0 + qmanager-quiescent-rpc.patch`
- Pinning: enabled, affinity-aware auto sizing, `--cores-per-task=5`
- Faketime: enabled. The worker exports Spack `FAKETIME_LIB`, every replica has
  `no_faketime: False`, and launch records include `/dev/shm/.../faketime_stamp`.
- System-visible jobs during the valid run: one `pdebug` worker only; no replica
  leakage to system `pbatch`.
- Result: `37,631` simulated jobs completed across the 16 packed replicas.
- Allocation-level throughput: about `684 jobs/min` (`37,631 / 55 min`).
- Runner log last progress line before allocation kill: `35,995/160,000` jobs
  at `2738.5s`, about `789 jobs/min` while the parallel runner was still alive.
- `results.csv` was written after reconciliation and contains 18 rows; the first
  16 rows are the measured 16-pack batch and the final 2-task batch remained
  queued.

Per-task completions from the 16-pack:

| task | queue | match | rabbit | dist | completed |
| --- | --- | --- | --- | --- | ---: |
| t000001 | easy | firstnodex | 0% | gaussian | 4974 |
| t000002 | easy | firstnodex | 100% | gaussian | 70 |
| t000003 | easy | firstnodex | 100% | pareto | 177 |
| t000004 | easy | lonodex | 0% | gaussian | 3082 |
| t000005 | easy | lonodex | 100% | gaussian | 136 |
| t000006 | easy | lonodex | 100% | pareto | 2117 |
| t000007 | hybrid | firstnodex | 0% | gaussian | 3839 |
| t000008 | hybrid | firstnodex | 100% | gaussian | 140 |
| t000009 | hybrid | firstnodex | 100% | pareto | 3928 |
| t000010 | hybrid | lonodex | 0% | gaussian | 3655 |
| t000011 | hybrid | lonodex | 100% | gaussian | 130 |
| t000012 | hybrid | lonodex | 100% | pareto | 3974 |
| t000013 | conservative | firstnodex | 0% | gaussian | 3654 |
| t000014 | conservative | firstnodex | 100% | gaussian | 128 |
| t000015 | conservative | firstnodex | 100% | pareto | 3970 |
| t000016 | conservative | lonodex | 0% | gaussian | 3657 |

The 16-pack still timed out at the pdebug wall before writing a clean
`batch_result.json`; `status` reconciled it as `TIMEOUT` and preserved per-task
progress. The logs show early-finalize sentinels were written, but
`finalize_reserve_seconds = 240` was not enough for teardown/results under this
16-way workload. Future pdebug calibration specs should use a larger reserve
(for example 600s) if a clean batch result is more important than maximizing
run time.
