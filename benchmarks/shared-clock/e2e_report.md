# Flux Fiction shared-clock smoke comparison

## Workload

This comparison runs the repository's `src/config.toml` case in a fresh Flux
instance inside `localhost/flux-fiction-dev:latest`. The trace contains 10 jobs
on a 30-node, 16-core/node resource set. Both modes use the same modified
`libfaketimeMT.so.1`; only `--faketime-mode` changes. Runs alternate mode order
across three repetitions, and host `date +%s%N` measures wall time outside the
faketime-preloaded container process.

Exact command:

```bash
REPETITIONS=3 benchmarks/shared-clock/run_flux_smoke_comparison.sh
```

The prepared container does not contain the `dftracer` Python package, so this
is a clock-advance and Flux scheduling smoke case, not a reproduction of the
previous trace-annotation workload or its reported 3.6× slowdown.

## Results

| Mode | Raw wall times | Median | Range | Completed jobs |
|---|---|---:|---:|---:|
| Legacy timestamp file | 14.529, 16.438, 13.631 s | 14.529 s | 13.631–16.438 s | 10/10 each |
| Shared-memory clock | 18.530, 19.136, 18.474 s | 18.530 s | 18.474–19.136 s | 10/10 each |

The shared mode was 0.784× as fast as legacy in this tiny end-to-end case
(27.5% longer by the medians). Container and Flux startup dominate these short
runs, so this result neither demonstrates nor contradicts the 429.5×
intercepted-read improvement measured by the C microbenchmark. It does show
that the optimization needs validation on the actual DFTracer annotation
campaign before claiming whole-workload speedup.

All six runs produced the same simulated makespan (0.4 hours), 83.33% average
core utilization, 432-second average queue wait, and completed all 10 jobs.
Five runs reported identical KVS growth (10,649.6 bytes/job); one shared run
used one additional SQLite page (11,059.2 bytes/job), without a simulation or
scheduler-output difference.

Machine-readable timings are in `e2e_results.csv`. Complete per-run console
logs are in `logs/`.
