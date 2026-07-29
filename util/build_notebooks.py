#!/usr/bin/env python3
"""Generate the two analysis notebooks. Rerun to regenerate them from source."""
import ast
from pathlib import Path

import nbformat as nbf

OUT = Path("/usr/WS1/ashworth12/ff-podman/rabbit-threshold-analysis")
SETUP = '''\
import sys, warnings
sys.path.insert(0, "/usr/WS1/ashworth12/ff-podman/flux-fiction-develop/util")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from plotnine import *
import ff_analysis as A

# Set REFRESH = True to re-harvest every cluster before analysing. The harvest
# is incremental and auto-discovering: it globs each campaign root, so a rerun
# picks up whatever finished since, and only re-parses runs whose files changed.
REFRESH = False

runs, traj = A.load(refresh_first=REFRESH)
pd.set_option("display.width", 160, "display.max_columns", 40)
print(f"{len(runs):,} runs   {int(runs.finalized.sum()):,} finalized   "
      f"{int(runs.complete.sum()):,} reached all 10,000 jobs   "
      f"{runs.config.nunique():,} configs   hosts: {sorted(runs.host.unique())}")
'''

CAVEAT = '''\
> **The one thing to keep in mind throughout.** Every batch ran under a 24-hour
> walltime, and most runs hit it before simulating all 10,000 jobs. A run cut
> short is *not* a shorter version of the same experiment — its makespan is
> whatever fraction of the trace it got through, and its utilisation is measured
> over that fraction. So makespan and utilisation are only comparable between
> runs that simulated the **same number of jobs**. Wherever this notebook
> aggregates them, it either restricts to complete runs or says explicitly what
> the cohort is. `jobs_per_makespan_h` is the truncation-safe alternative.
'''


def md(text):
    return nbf.v4.new_markdown_cell(text)


def code(src):
    return nbf.v4.new_code_cell(src)


# ─────────────────────────────────────────────────────────── performance
def performance():
    c = [
        md("# Flux Fiction performance across scheduler configurations\n\n"
           "How much simulation each policy/machine/rabbit combination actually got "
           "through in its 24-hour budget, what governs that rate, and what the "
           "runs that finished look like over wall clock.\n\n"
           "Every figure is driven by `metrics_all.csv`, harvested from all three "
           "clusters by `util/ff_harvest.py`. Re-run with `REFRESH = True` to pull "
           "in whatever has completed since."),
        code(SETUP),
        md("## What is in the dataset\n\n" + CAVEAT),
        code('''\
inv = (runs.groupby("host")
       .agg(runs=("task_id", "size"), finalized=("finalized", "sum"),
            complete=("complete", "sum"),
            median_jobs=("jobs_completed", "median"),
            max_jobs=("jobs_completed", "max"))
       .reset_index())
display(inv)
display(runs.tier.value_counts().rename_axis("tier").reset_index(name="runs"))'''),

        md("## 1. How many jobs finished in 24 h, by policy and machine\n\n"
           "The blunt question: given a fixed budget, how far does each scheduler "
           "configuration get? Every run had the same 10,000-job trace and the same "
           "24-hour ceiling, so this is a like-for-like comparison of scheduling cost."),
        code('''\
p = (ggplot(runs, aes("policy", "jobs_completed", fill="queue_policy"))
     + geom_boxplot(outlier_size=0.5, outlier_alpha=0.25, width=0.62, size=0.4)
     + geom_hline(yintercept=10000, color=A.INK2, size=0.5, linetype="dashed")
     + scale_fill_manual(values=A.PAL[:3], name="queue policy")
     + facet_wrap("host")
     + coord_flip()
     + labs(title="Jobs simulated within the 24-hour budget",
            subtitle="Each box is every run of that policy pair on that cluster; "
                     "the dashed line is the whole 10,000-job trace",
            x="", y="simulated jobs completed",
            caption="Source: metrics_all.csv. Runs still in flight are included at "
                    "their current count, so these are lower bounds for tuolumne.")
     + A.theme_ff() + theme(figure_size=(11, 4.5)))
p'''),
        code('''\
summary = (runs.groupby(["host", "policy"])
           .agg(runs=("task_id", "size"), median_jobs=("jobs_completed", "median"),
                complete=("complete", "sum"))
           .reset_index()
           .pivot(index="policy", columns="host", values="median_jobs"))
print("Median jobs completed, by policy and cluster")
display(summary.round(0))'''),

        md("## 2. Does asking for rabbits cost throughput?\n\n"
           "Two knobs: what fraction of jobs request a rabbit allocation "
           "(`rabbit_job_pct`), and how large those requests are as a share of a "
           "rabbit node (`rabbit_ceiling_pct`). `rj = 0` is the control."),
        code('''\
p = (ggplot(runs, aes("factor(rabbit_job_pct)", "jobs_completed"))
     + geom_boxplot(fill=A.PAL[0], alpha=0.8, outlier_size=0.4,
                    outlier_alpha=0.2, size=0.35, width=0.68)
     + facet_wrap("queue_policy", ncol=3)
     + labs(title="Throughput against the share of jobs requesting rabbits",
            subtitle="0% is the control arm; all three queue policies shown",
            x="% of jobs requesting a rabbit allocation", y="simulated jobs completed")
     + A.theme_ff() + theme(figure_size=(11, 4.2)))
p'''),
        code('''\
grid = (runs.groupby(["rabbit_job_pct", "rabbit_ceiling_pct"])
        .agg(median_jobs=("jobs_completed", "median"), n=("task_id", "size"))
        .reset_index())
p = (ggplot(grid, aes("factor(rabbit_job_pct)", "factor(rabbit_ceiling_pct)",
                      fill="median_jobs"))
     + geom_tile(color="white", size=0.6)
     + scale_fill_gradient(low="#eaf1fb", high=A.PAL[0], name="median\\njobs done")
     + labs(title="Throughput across the full rabbit grid",
            subtitle="Median jobs completed for every (frequency, size) pair, "
                     "pooled over policies, machines and shakes",
            x="% of jobs requesting rabbits", y="request size, % of a rabbit node")
     + A.theme_ff() + theme(figure_size=(9, 5)))
p'''),

        md("## 3. Performance against queue depth, per policy\n\n"
           "Scheduling cost is not a constant: it is a product of the queue policy, "
           "the match policy, how many jobs are sitting in the queue, and the raw "
           "compute time of a single match. Queue depth here is the *time-weighted "
           "mean* number of jobs waiting, computed from the per-job event table, so "
           "it is a measurement rather than the configured `queue-depth` parameter.\n\n"
           "Correlations are computed below rather than asserted, and split by policy "
           "— two of these relationships **reverse sign within policy**, so the "
           "pooled number says the opposite of every subgroup."),
        code('''\
fin = runs[runs.finalized].copy()
for x, y in [("queue_depth_mean", "bsld_mean"),
             ("queue_depth_mean", "match_ok_avg_s"),
             ("queue_depth_mean", "match_fail_ratio")]:
    print(f"{x}  vs  {y}")
    print(A.rho(fin, x, y, "queue_policy").to_string(index=False), "\\n")'''),
        code('''\
p = (ggplot(fin, aes("queue_depth_mean", "bsld_mean", color="queue_policy"))
     + geom_point(size=1.5, alpha=0.5, stroke=0)
     + scale_color_manual(values=A.PAL[:3], name="queue policy")
     + scale_x_log10() + scale_y_log10()
     + labs(title="Deeper queues mean much worse bounded slowdown",
            subtitle="The clearest queue-depth effect in the data "
                     "(Spearman +0.77 overall, and +0.69 to +0.85 within every policy)",
            x="mean jobs waiting in queue (log)", y="mean bounded slowdown (log)")
     + A.theme_ff())
p'''),
        code('''\
fin = runs[runs.finalized].copy()
p = (ggplot(fin, aes("queue_depth_mean", "match_ok_avg_s", color="match_policy"))
     + geom_point(size=1.5, alpha=0.55, stroke=0)
     + geom_smooth(method="lowess", se=False, size=1.0)
     + scale_color_manual(values=A.PAL[:2], name="match policy")
     + scale_x_log10() + scale_y_log10()
     + facet_wrap("queue_policy", ncol=3)
     + labs(title="Cost of a successful match rises with queue depth",
            subtitle="One point per finalized run; both axes log scale",
            x="mean jobs waiting in queue", y="seconds per successful match",
            caption="Fluxion's own match counters, from each run's summary.json.")
     + A.theme_ff() + theme(figure_size=(11, 4.2)))
p'''),
        code('''\
p = (ggplot(fin, aes("queue_depth_mean", "jobs_per_makespan_h", color="queue_policy"))
     + geom_point(size=1.5, alpha=0.5, stroke=0)
     + scale_color_manual(values=A.PAL[:3], name="queue policy")
     + scale_x_log10()
     + labs(title="Simulated work per simulated hour is flat across queue depths",
            subtitle="Spearman +0.03 -- essentially no relationship. The dense band "
                     "near 53 jobs/h is a property of the REPLAYED TRACE, not of the "
                     "scheduler: arrivals are fixed, so what a costly policy spends is "
                     "wall clock, not simulated throughput.",
            x="mean jobs waiting in queue (log)",
            y="jobs per hour of simulated makespan")
     + A.theme_ff())
p'''),

        md("## 4. Throughput over wall clock, for the runs that finished\n\n"
           "Only runs that reached a finalize appear here — they are the ones with a "
           "full progress trace. The curve is cumulative simulated jobs against hours "
           "of real time since that run started."),
        code('''\
tj = traj.merge(runs[["host", "task_id", "queue_policy", "match_policy",
                      "policy", "complete"]],
                on=["host", "task_id"], how="left")
tj = tj[tj.queue_policy.notna()]
p = (ggplot(tj, aes("elapsed_h", "jobs_completed", group="task_id",
                    color="queue_policy"))
     + geom_line(alpha=0.16, size=0.4)
     + geom_hline(yintercept=10000, color=A.INK2, size=0.5, linetype="dashed")
     + scale_color_manual(values=A.PAL[:3], name="queue policy")
     + facet_wrap("host")
     + labs(title="Every finalized run's progress against wall clock",
            subtitle=f"{tj.task_id.nunique():,} runs; the dashed line is the full trace",
            x="hours since the run started", y="cumulative simulated jobs")
     + A.theme_ff() + theme(figure_size=(11, 4.5)))
p'''),
        code('''\
# Median trajectory per policy, so the shape is readable rather than a hairball.
band = (tj.assign(h=(tj.elapsed_h * 2).round() / 2)
        .groupby(["policy", "h"])["jobs_completed"]
        .agg(["median", "count"]).reset_index())
band = band[band["count"] >= 5]
p = (ggplot(band, aes("h", "median", color="policy"))
     + geom_line(size=1.0)
     + scale_color_manual(values=A.PAL[:6], name="queue / match")
     + labs(title="Median progress curve by policy pair",
            subtitle="Half-hour bins, only bins backed by 5+ runs",
            x="hours since the run started", y="median cumulative simulated jobs")
     + A.theme_ff())
p'''),

        md("## 5. Where the wall clock actually goes\n\n"
           "Fluxion reports how long it spent on matches that succeeded and on "
           "attempts that failed. Failed attempts are individually cheap but there "
           "are millions of them, so the two can be comparable in total."),
        code('''\
mm = fin.melt(id_vars=["policy", "queue_policy", "match_policy"],
              value_vars=["match_ok_n", "match_fail_n"],
              var_name="kind", value_name="count")
mm["kind"] = mm["kind"].map({"match_ok_n": "succeeded", "match_fail_n": "failed"})
p = (ggplot(mm, aes("policy", "count", fill="kind"))
     + geom_boxplot(outlier_size=0.4, outlier_alpha=0.2, size=0.35)
     + scale_fill_manual(values=[A.PAL[1], A.PAL[2]], name="match attempts")
     + scale_y_log10() + coord_flip()
     + labs(title="Match attempts per run, succeeded versus failed",
            subtitle="Log scale — failed attempts outnumber successful ones by "
                     "two orders of magnitude",
            x="", y="attempts per run (log)")
     + A.theme_ff())
p'''),
        code('''\
p = (ggplot(fin, aes("factor(rabbit_job_pct)", "match_ok_avg_s", fill="queue_policy"))
     + geom_boxplot(outlier_size=0.4, outlier_alpha=0.2, size=0.35, width=0.7)
     + scale_fill_manual(values=A.PAL[:3], name="queue policy")
     + facet_wrap("queue_policy", ncol=3, scales="free_y")
     + labs(title="Rabbit requests make each successful match more expensive",
            subtitle="Strongly for conservative (+0.70) and hybrid (+0.73), only "
                     "weakly for easy (+0.12) -- easy holds one reservation, so it "
                     "traverses far less either way",
            x="% of jobs requesting rabbits", y="seconds per successful match")
     + A.theme_ff() + theme(figure_size=(11, 4.2), legend_position="none"))
p'''),
        code('''\
p = (ggplot(fin, aes("match_s_per_job", "jobs_completed", color="queue_policy"))
     + geom_point(size=1.5, alpha=0.5, stroke=0)
     + scale_color_manual(values=A.PAL[:3], name="queue policy")
     + scale_x_log10()
     + labs(title="Scheduling cost per job almost entirely determines how far a run got",
            subtitle="Spearman -0.99. Total Fluxion match time divided by jobs "
                     "simulated, against the count reached in 24 h",
            x="seconds of match time per simulated job (log)",
            y="simulated jobs completed")
     + A.theme_ff())
p'''),

        md("## Takeaways\n\n"
           "Read these off the figures above rather than as settled results — the "
           "campaign is still running and the cohort is censored by the walltime.\n\n"
           "1. **Policy dominates throughput.** The spread between the cheapest and "
           "most expensive policy pair is far larger than the spread between "
           "machines.\n"
           "2. **Queue depth is the mechanism.** Per-match cost climbs with the "
           "number of jobs waiting, and the queue policies differ mainly in how many "
           "reservations they maintain.\n"
           "3. **Rabbit requests raise per-match cost** by deepening the traversal, "
           "which is visible as a monotone rise in seconds-per-match with "
           "`rabbit_job_pct`.\n"
           "4. **Failed match attempts dominate by count** but not always by total "
           "time — worth watching, because it is the part a rabbit-aware policy "
           "could avoid."),
    ]
    return c


# ────────────────────────────────────────────────────────────────── data
def data_notebook():
    c = [
        md("# What the simulated schedules actually did\n\n"
           "Quality-of-schedule metrics for the runs that produced them: makespan, "
           "utilisation, bounded slowdown and fairness, averaged across the shake "
           "replicas, plus how much of the grid produced usable data at all.\n\n"
           "**Bounded slowdown** is `max(1, (wait + run) / max(run, 10s))` — the "
           "standard bound stops very short jobs from dominating. **Fairness** is "
           "Jain's index over per-job bounded slowdown: 1.0 means every job was "
           "delayed proportionally the same, 1/n means one job absorbed everything. "
           "Both are computed from the per-job event table of each run."),
        code(SETUP),
        md("## 1. How much of the grid produced usable data\n\n"
           "Each config is one point in the policy x rabbit grid, run with 10 shake "
           "replicas. This is the coverage question: how many configs cleared each "
           "bar?"),
        code('''\
tiers = A.tier_table(runs)
cov = A.coverage_summary(tiers)
display(cov)
print(f"{len(tiers):,} configs, {int(tiers.shakes.sum()):,} runs")'''),
        code('''\
p = (ggplot(cov, aes("reorder(criterion, configs)", "configs"))
     + geom_col(fill=A.PAL[0], width=0.62)
     + geom_text(aes(label="configs"), ha="left", nudge_y=8, size=8, color=A.INK)
     + coord_flip()
     + labs(title="How many configurations cleared each completion bar",
            subtitle=f"Out of {len(tiers):,} configurations, each with up to 10 shake replicas",
            x="", y="configurations")
     + A.theme_ff())
p'''),
        code('''\
p = (ggplot(runs, aes("tier", fill="tier"))
     + geom_bar(width=0.65)
     + geom_text(aes(label=after_stat("count")), stat="count", ha="left",
                 nudge_y=60, size=8, color=A.INK)
     + scale_fill_manual(values=[A.PAL[2], A.PAL[0], A.PAL[3], A.PAL[1], "#9aa3b2"])
     + coord_flip()
     + labs(title="Where individual runs stopped",
            subtitle="All runs across all three clusters",
            x="", y="runs")
     + A.theme_ff() + theme(legend_position="none"))
p'''),

        md("## 2. Schedule quality for the runs that completed the whole trace\n\n" + CAVEAT
           + "\n### Survivorship: read this before the numbers\n\n"
             "Finishing the trace inside 24 hours is itself a *policy* outcome, so "
             "filtering on completion filters on policy. In the current data **every "
             "single complete run is `easy / firstnodex`** — the cheapest pairing. "
             "There is therefore no policy contrast to draw inside this cohort, and "
             "any apparent difference between this cohort and a wider one is at least "
             "partly a difference in policy mix rather than in schedule quality.\n\n"
             "What *does* vary within the complete cohort is rabbit demand, so that is "
             "what the figures below are cut by."),
        code('''\
COMPLETE = runs[runs.complete].copy()
print(f"{len(COMPLETE)} complete runs across {COMPLETE.config.nunique()} configs\\n")
print("Policy composition of the complete cohort -- note there is only one:")
print(COMPLETE.policy.value_counts().to_string())
print("\\nRabbit demand DOES vary within it:")
print(COMPLETE.rabbit_job_pct.value_counts().sort_index().to_string())

def summarise(df, by):
    return (df.groupby(by)
            .agg(runs=("task_id", "size"),
                 makespan_h=("makespan_s", lambda s: s.mean() / 3600),
                 util_node=("util_node_pct", "mean"),
                 bsld_mean=("bsld_mean", "mean"),
                 bsld_p95=("bsld_p95", "mean"),
                 fairness=("fairness_jain", "mean"),
                 queue_depth=("queue_depth_mean", "mean"),
                 wait_h=("avg_queue_wait_s", lambda s: s.mean() / 3600))
            .round(3).reset_index())

display(summarise(COMPLETE, ["queue_policy", "match_policy"]))'''),
        code('''\
long = COMPLETE.melt(id_vars=["policy", "rabbit_job_pct"],
                     value_vars=["makespan_s", "util_node_pct", "bsld_mean", "fairness_jain"],
                     var_name="metric", value_name="value")
long["value"] = np.where(long.metric == "makespan_s", long.value / 3600, long.value)
long["metric"] = long.metric.map({
    "makespan_s": "makespan (hours)", "util_node_pct": "node utilisation (%)",
    "bsld_mean": "mean bounded slowdown", "fairness_jain": "fairness (Jain, 1 = equal)"})
p = (ggplot(long, aes("factor(rabbit_job_pct)", "value"))
     + geom_boxplot(fill=A.PAL[0], alpha=0.85, outlier_size=0.6, size=0.35, width=0.65)
     + facet_wrap("metric", scales="free_y", ncol=2)
     + labs(title="Schedule quality across rabbit demand, complete runs only",
            subtitle=f"{len(COMPLETE)} runs that simulated all 10,000 jobs — all of "
                     "them easy / firstnodex, so this is a rabbit contrast, "
                     "not a policy one",
            x="% of jobs requesting a rabbit allocation", y="")
     + A.theme_ff() + theme(figure_size=(11, 6)))
p'''),

        md("## 3. The same metrics for every run that reached 5,000 jobs\n\n"
           "A far larger cohort, at the cost of comparability: these runs stopped at "
           "different points in the trace, so makespan differences partly reflect "
           "*where* each run stopped rather than how well it scheduled. Utilisation "
           "and the per-job metrics (slowdown, fairness) are more robust to this, "
           "since they are averages over whatever was simulated."),
        code('''\
GE5000 = runs[(runs.jobs_completed >= 5000) & runs.finalized].copy()
print(f"{len(GE5000)} runs >= 5,000 jobs across {GE5000.config.nunique()} configs "
      f"(vs {len(COMPLETE)} complete)\\n")
print("Policy composition -- wider, but still nothing like balanced:")
print(GE5000.policy.value_counts().to_string())
print("\\nSo the cohort comparison below is partly a POLICY-MIX comparison: the "
      "5,000+ cohort\\nadmits policies the complete cohort excludes entirely.")
display(summarise(GE5000, ["queue_policy", "match_policy"]))'''),
        code('''\
comp = pd.concat([
    summarise(COMPLETE, ["queue_policy", "match_policy"]).assign(cohort="complete (10k)"),
    summarise(GE5000, ["queue_policy", "match_policy"]).assign(cohort="5,000+ jobs")])
comp["policy"] = comp.queue_policy + " / " + comp.match_policy
long2 = comp.melt(id_vars=["policy", "cohort"],
                  value_vars=["util_node", "bsld_mean", "fairness", "queue_depth"],
                  var_name="metric", value_name="value")
p = (ggplot(long2, aes("policy", "value", fill="cohort"))
     + geom_col(position=position_dodge(width=0.72), width=0.68)
     + scale_fill_manual(values=[A.PAL[0], A.PAL[1]], name="cohort")
     + facet_wrap("metric", scales="free_x", ncol=2)
     + coord_flip()
     + labs(title="Does widening the cohort change the ranking?",
            subtitle="Complete runs versus every run that reached 5,000 jobs. Bars "
                     "present in only one cohort are policies that never completed "
                     "the trace, not a change in their behaviour.",
            x="", y="")
     + A.theme_ff() + theme(figure_size=(11, 6)))
p'''),

        md("## 4. Variance between shake replicas of a single configuration\n\n"
           "Each configuration was run with 10 shake seeds, which perturb job "
           "runtime, node count and interarrival by 10%. If replica-to-replica "
           "spread is comparable to the differences between configurations, then "
           "single-run comparisons are not meaningful."),
        code('''\
# Pick the configuration with the best replica coverage, preferring complete runs.
cand = tiers.sort_values(["complete_10k", "ge_5000"], ascending=False).iloc[0]
PICK = cand.config
sel = runs[(runs.config == PICK) & runs.finalized].sort_values("shake_seed")
print(f"Configuration: {PICK}")
print(f"  {int(cand.shakes)} shakes, {int(cand.complete_10k)} complete, "
      f"{int(cand.ge_5000)} reached 5,000\\n")
display(sel[["shake_seed", "host", "jobs_completed", "makespan_s", "util_node_pct",
             "bsld_mean", "fairness_jain", "queue_depth_mean"]]
        .assign(makespan_h=lambda d: (d.makespan_s / 3600).round(1))
        .drop(columns="makespan_s").round(3))'''),
        code('''\
metrics = ["util_node_pct", "bsld_mean", "fairness_jain", "jobs_completed"]
LABEL = {"util_node_pct": "node utilisation (%)", "bsld_mean": "mean bounded slowdown",
         "fairness_jain": "fairness (Jain)", "jobs_completed": "jobs completed"}
sl = sel.melt(id_vars=["shake_seed"], value_vars=metrics,
              var_name="metric", value_name="value")
sl["metric"] = sl.metric.map(LABEL)
sl = sl.merge(sl.groupby("metric")["value"].mean().reset_index(name="mean"), on="metric")
# Points against a mean line, not bars from zero: a bar anchored at zero makes a
# 2% spread invisible, and the spread is the entire question this plot asks.
p = (ggplot(sl, aes("shake_seed", "value"))
     + geom_hline(aes(yintercept="mean"), color=A.PAL[1], size=0.8, linetype="dashed")
     + geom_segment(aes(xend="shake_seed", y="mean", yend="value"),
                    color=A.PAL[0], size=0.5)
     + geom_point(color=A.PAL[0], size=3.0)
     + facet_wrap("metric", scales="free_y", ncol=2)
     + labs(title="Replica-to-replica spread within one configuration",
            subtitle=f"{PICK} — dashed line is the mean across replicas; "
                     "y-axes zoom to the data rather than anchoring at zero",
            x="shake seed", y="")
     + A.theme_ff()
     + theme(figure_size=(11, 6), axis_text_x=element_text(rotation=45, ha="right")))
p'''),
        code('''\
stat = (sel[metrics].agg(["mean", "std", "min", "max"]).T
        .assign(cv_pct=lambda d: (100 * d["std"] / d["mean"]).round(1)).round(3))
print("Spread across shake replicas of this one configuration")
display(stat)
print("\\nFor comparison, spread ACROSS configurations (complete runs, same metrics):")
display(COMPLETE[metrics].agg(["mean", "std"]).T
        .assign(cv_pct=lambda d: (100 * d["std"] / d["mean"]).round(1)).round(3))'''),

        md("## 5. Utilisation against rabbit demand — the actual hypothesis\n\n"
           "The premise of the campaign is that rabbit requests, being invisible to "
           "the scheduling policy, fragment the machine and can *lower* utilisation "
           "even at small request rates."),
        code('''\
cohort = GE5000.copy()
p = (ggplot(cohort, aes("factor(rabbit_job_pct)", "util_node_pct"))
     + geom_boxplot(fill=A.PAL[2], alpha=0.85, outlier_size=0.4,
                    outlier_alpha=0.25, size=0.35, width=0.68)
     + facet_wrap("queue_policy", ncol=3)
     + labs(title="Node utilisation against the share of jobs requesting rabbits",
            subtitle="Runs that reached 5,000+ jobs; 0% is the control arm",
            x="% of jobs requesting a rabbit allocation", y="node utilisation (%)")
     + A.theme_ff() + theme(figure_size=(11, 4.2)))
p'''),
        code('''\
agg = (cohort.groupby(["rabbit_job_pct", "queue_policy"])
       .agg(util=("util_node_pct", "mean"), bsld=("bsld_mean", "mean"),
            n=("task_id", "size")).reset_index())
agg = agg[agg.n >= 5]
p = (ggplot(agg, aes("rabbit_job_pct", "util", color="queue_policy"))
     + geom_line(size=1.0) + geom_point(size=2.2, stroke=0)
     + scale_color_manual(values=A.PAL[:3], name="queue policy")
     + labs(title="Utilisation drifts down as more jobs ask for rabbits",
            subtitle="Spearman -0.35; roughly 75.5% at the control down to 73.4% at "
                     "100%. Real and near-monotone, but small, and confounded by "
                     "which runs survived to 5,000 jobs.",
            x="% of jobs requesting a rabbit allocation", y="mean node utilisation (%)")
     + A.theme_ff())
p'''),

        md("## 6. Who absorbs the delay\n\n"
           "Read the fairness panel with care: pooled across policies fairness looks "
           "like it falls with queue depth, but **within every individual policy it "
           "rises**. That is Simpson's paradox -- the policies sit at systematically "
           "different depths. Trust the per-policy numbers.\n\n"
           "Fairness as a single index hides *which* jobs got hurt. Bounded slowdown "
           "split by allocation size answers that directly, and it matters here "
           "because graph scheduling costs more for larger allocations."),
        code('''\
sz = cohort.melt(id_vars=["policy", "queue_policy"],
                 value_vars=["bsld_mean_single", "bsld_mean_multi"],
                 var_name="size", value_name="bsld").dropna()
sz["size"] = sz["size"].map({"bsld_mean_single": "single-node jobs",
                             "bsld_mean_multi": "multi-node jobs"})
p = (ggplot(sz, aes("policy", "bsld", fill="size"))
     + geom_boxplot(outlier_size=0.4, outlier_alpha=0.2, size=0.35)
     + scale_fill_manual(values=[A.PAL[0], A.PAL[1]], name="job size")
     + scale_y_log10() + coord_flip()
     + labs(title="Bounded slowdown, single-node versus multi-node jobs",
            subtitle="Runs that reached 5,000+ jobs; log scale",
            x="", y="mean bounded slowdown (log)")
     + A.theme_ff())
p'''),
        code('''\
print("fairness vs queue depth -- pooled, then within each policy")
print(A.rho(cohort, "queue_depth_mean", "fairness_jain", "queue_policy").to_string(index=False))
print("\\nA sign reversal: pooled the correlation is negative, but inside every single"
      "\\npolicy it is positive. The pooled figure is an artefact of the policies "
      "sitting at\\ndifferent queue depths, not a within-policy effect.")'''),
        code('''\
p = (ggplot(cohort, aes("queue_depth_mean", "fairness_jain", color="queue_policy"))
     + geom_point(size=1.5, alpha=0.5, stroke=0)
     + geom_smooth(method="lowess", se=False, size=0.9)
     + scale_color_manual(values=A.PAL[:3], name="queue policy")
     + scale_x_log10()
     + labs(title="Fairness is low everywhere, and does not fall with queue depth",
            subtitle="Jain's index over per-job bounded slowdown; 1.0 = every job "
                     "delayed proportionally the same. Within each policy the trend "
                     "is flat to slightly rising.",
            x="mean jobs waiting in queue (log)", y="fairness (Jain)")
     + A.theme_ff())
p'''),
        code('''\
p = (ggplot(cohort, aes("util_node_pct", "bsld_mean", color="factor(rabbit_active)"))
     + geom_point(size=1.5, alpha=0.5, stroke=0)
     + scale_color_manual(values=[A.PAL[0], A.PAL[1]],
                          labels=["control (no rabbits)", "rabbit jobs present"],
                          name="")
     + scale_y_log10()
     + labs(title="The utilisation / responsiveness trade-off",
            subtitle="Each point is one run that reached 5,000+ jobs",
            x="node utilisation (%)", y="mean bounded slowdown (log)")
     + A.theme_ff())
p'''),

        md("## Takeaways\n\n"
           "1. **Survivorship dominates everything here.** Every complete run is "
           "`easy / firstnodex`, and no configuration got all ten shake replicas "
           "through the full trace inside 24 hours. Completion is a policy outcome, "
           "so conditioning on it silently conditions on policy — the single most "
           "important thing to hold in mind when reading any average below.\n"
           "2. **Replica spread is not negligible.** Compare the coefficient of "
           "variation within one configuration against the spread across "
           "configurations before believing any single-run difference.\n"
           "3. **Truncation is the biggest threat to validity.** Makespan comparisons "
           "between cohorts of different length are not meaningful; utilisation and "
           "the per-job metrics degrade more gracefully.\n"
           "4. **The hypothesis is directionally supported but weak so far.** "
           "Utilisation does fall as rabbit demand rises (Spearman -0.35, ~75.5% to "
           "~73.4%), but the control arm has few surviving runs and sits inside the "
           "spread of the low-rabbit arms. The cleaner result is on *scheduling "
           "cost*: rabbit requests raise per-match time substantially for "
           "conservative and hybrid.\n"
           "5. **Watch for sign reversals.** Two relationships here point one way "
           "pooled and the other way within policy. Read the per-policy correlation "
           "table before believing any scatter."),
    ]
    return c


def write(cells, path, title):
    # Parse every code cell before writing. These sources are triple-quoted
    # strings inside triple-quoted strings, so an escape level is easy to lose,
    # and a broken cell would otherwise only surface at execution time.
    bad = []
    for i, cell in enumerate(cells):
        if cell.cell_type == "code":
            try:
                ast.parse(cell.source)
            except SyntaxError as exc:
                bad.append(f"  cell {i}: {exc}")
    if bad:
        raise SystemExit(f"{path.name}: generated cells do not parse\n" + "\n".join(bad))

    nb = nbf.v4.new_notebook(cells=cells)
    nb.metadata = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
        "title": title,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, str(path))
    print(f"wrote {path} ({len(cells)} cells)")


if __name__ == "__main__":
    write(performance(), OUT / "01-performance.ipynb", "Flux Fiction performance")
    write(data_notebook(), OUT / "02-schedule-data.ipynb", "Schedule quality")
