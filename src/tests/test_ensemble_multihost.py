from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from flux_fiction.ensemble.campaign import (
    generate_tasks,
    initialize_partial_campaign,
    materialize_task_inputs,
    merge_host_results,
    publish_host_results,
    read_jsonl,
    results_csv_path,
    submission_backend,
    tasks_path,
)
from flux_fiction.ensemble.config import EnsembleConfigError, load_campaign_spec
from flux_fiction.ensemble.hosts import (
    assignment_cycle,
    host_adjusted_spec,
    load_host_config,
    partition_tasks,
)
from tests.test_ensemble_generator import _write_base_files, _write_spec


def _write_hosts(tmp_path: Path, *, shares=(2, 1, 2)) -> Path:
    a, b, c = shares
    path = tmp_path / "hosts.toml"
    path.write_text(
        f"""
version = 1
shared_root = "{tmp_path / 'shared'}"

[hosts.alpha]
backend = "flux"
queues = ["pdebug"]
output_root = "{tmp_path / 'alpha'}"
runs_per_node = 16
max_active = 8
walltime_minutes = 55
share = {a}

[hosts.beta]
backend = "flux"
queues = ["pdebug"]
output_root = "{tmp_path / 'beta'}"
runs_per_node = 8
max_active = 4
walltime_minutes = 55
share = {b}

[hosts.gamma]
backend = "slurm"
queues = ["pdebug"]
output_root = "{tmp_path / 'gamma'}"
runs_per_node = 16
max_active = 8
walltime_minutes = 55
share = {c}
""",
        encoding="utf-8",
    )
    return path


def test_partition_is_deterministic_complete_and_disjoint(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    hosts = load_host_config(_write_hosts(tmp_path))
    tasks = generate_tasks(spec)

    first = partition_tasks(tasks, hosts)
    second = partition_tasks(tasks, hosts)

    assert {h: [t["task_id"] for t in v] for h, v in first.items()} == {
        h: [t["task_id"] for t in v] for h, v in second.items()
    }
    assigned = [t["task_id"] for group in first.values() for t in group]
    assert len(assigned) == len(tasks)            # complete
    assert len(set(assigned)) == len(tasks)       # disjoint
    # Weights respected within rounding. The final pass through the cycle is
    # usually partial, and a host's slots may all fall in the truncated tail,
    # so the deviation is bounded by that host's share rather than by 1.
    total_share = sum(h.share for h in hosts.hosts.values())
    for name, host in hosts.hosts.items():
        ideal = len(tasks) * host.share / total_share
        assert abs(len(first[name]) - ideal) <= host.share, (name, len(first[name]), ideal)
    assert len(first["beta"]) < len(first["alpha"])  # share 1 vs share 2
    # interleaved, not blocked: each host sees more than one queue policy
    policies = {t["queue_policy"] for t in first["alpha"]}
    assert len(policies) > 1


def test_host_overlay_changes_placement_not_the_experiment(tmp_path: Path):
    """Per-host settings must not perturb the workload definition."""
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    hosts = load_host_config(_write_hosts(tmp_path))

    alpha = host_adjusted_spec(spec, hosts.hosts["alpha"])
    gamma = host_adjusted_spec(spec, hosts.hosts["gamma"])

    # placement/packing differ
    assert alpha.campaign.batch_size == 16 and hosts.hosts["beta"].runs_per_node == 8
    assert submission_backend(alpha) == "flux"
    assert submission_backend(gamma) == "slurm"
    assert alpha.campaign.name.endswith("-alpha") and gamma.campaign.name.endswith("-gamma")
    assert alpha.campaign.output_root != gamma.campaign.output_root

    # the experiment definition does not
    for other in (alpha, gamma):
        assert other.input == spec.input
        assert other.shake == spec.shake
        assert other.grid == spec.grid
        assert other.rabbit == spec.rabbit


def test_same_task_yields_identical_trace_on_every_host(tmp_path: Path):
    """The property the whole split rests on: matched shaken inputs."""
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    hosts = load_host_config(_write_hosts(tmp_path))
    task = generate_tasks(spec)[0]

    digests = set()
    for name, host in hosts.hosts.items():
        adjusted = host_adjusted_spec(spec, host)
        out = materialize_task_inputs(adjusted, task, tmp_path / f"mat-{name}")
        digests.add(hashlib.sha256(out["trace"].read_bytes()).hexdigest())

    assert len(digests) == 1


def test_partial_campaign_writes_only_its_own_slice(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    hosts = load_host_config(_write_hosts(tmp_path))
    all_tasks = generate_tasks(spec)

    seen: set[str] = set()
    for name in hosts.hosts:
        root, assignment = initialize_partial_campaign(spec, name, hosts)
        own = [t["task_id"] for t in read_jsonl(tasks_path(root))]
        assert own == assignment["task_ids"]
        assert assignment["total_tasks"] == len(all_tasks)
        assert not (seen & set(own)), "hosts must not overlap"
        seen |= set(own)
        # batches respect this host's packing
        batches = read_jsonl(root / "batches.jsonl")
        assert max(b["task_count"] for b in batches) <= hosts.hosts[name].runs_per_node

    assert seen == {t["task_id"] for t in all_tasks}
    # shared context is published where every cluster can read it
    shared = Path(hosts.shared_root)
    assert (shared / "tasks_all.jsonl").exists()
    assert len(read_jsonl(shared / "tasks_all.jsonl")) == len(all_tasks)
    for name in hosts.hosts:
        assert (shared / "hosts" / name / "assignment.json").exists()


def test_changing_shares_on_existing_root_is_refused(tmp_path: Path):
    """Re-partitioning under already-submitted work would silently move tasks."""
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    initialize_partial_campaign(spec, "alpha", load_host_config(_write_hosts(tmp_path)))

    reweighted = load_host_config(_write_hosts(tmp_path, shares=(5, 1, 2)))
    with pytest.raises(RuntimeError, match="shares changed"):
        initialize_partial_campaign(spec, "alpha", reweighted)


def test_merge_tags_rows_with_their_host(tmp_path: Path):
    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    hosts = load_host_config(_write_hosts(tmp_path))

    for name in hosts.hosts:
        root, _ = initialize_partial_campaign(spec, name, hosts)
        with results_csv_path(root).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["task_id", "state", "jobs_completed"])
            writer.writeheader()
            writer.writerow({"task_id": f"t-{name}", "state": "succeeded", "jobs_completed": "5"})
        publish_host_results(root, hosts, name)

    out, per_host = merge_host_results(hosts)
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert per_host == {"alpha": 1, "beta": 1, "gamma": 1}
    assert {r["host"] for r in rows} == {"alpha", "beta", "gamma"}
    assert {r["task_id"] for r in rows} == {"t-alpha", "t-beta", "t-gamma"}


# --- zero-rabbit collapse -------------------------------------------------

def _rabbit_spec(tmp_path: Path, *, collapse: bool):
    from dataclasses import replace

    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    grid = replace(
        spec.grid,
        queue_policies=["fcfs"],
        match_policies=["lonodex"],
        rabbit_job_percentages=[0.0, 50.0, 100.0],
        rabbit_ceiling_percentages=[0.0, 10.0, 50.0],
        collapse_zero_rabbit=collapse,
    )
    return replace(
        spec,
        grid=grid,
        shake=replace(spec.shake, duplicates=1),
        rabbit=replace(spec.rabbit, capacity_gib=1000.0),
    )


def test_zero_rabbit_combinations_collapse_to_one_control(tmp_path: Path):
    """rj=0 and rc=0 both mean 'no rabbit', so their cross product is redundant."""
    full = generate_tasks(_rabbit_spec(tmp_path, collapse=False))
    collapsed = generate_tasks(_rabbit_spec(tmp_path, collapse=True))

    # 3x3 grid: 5 degenerate pairings (rj=0 row + rc=0 column) -> 1 control
    assert len(full) == 9
    assert len(collapsed) == 5
    controls = [t for t in collapsed if not t["rabbit_active"]]
    assert len(controls) == 1
    assert controls[0]["rabbit_job_percentage"] == 0.0
    assert controls[0]["rabbit_ceiling_percentage"] == 0.0
    # everything else genuinely requests rabbit
    assert all(
        t["rabbit_job_percentage"] > 0 and t["rabbit_ceiling_percentage"] > 0
        for t in collapsed
        if t["rabbit_active"]
    )


def test_collapse_keeps_one_control_per_policy_cell(tmp_path: Path):
    """The control is per policy cell -- the scheduler config still differs."""
    from dataclasses import replace

    spec = _rabbit_spec(tmp_path, collapse=True)
    spec = replace(
        spec,
        grid=replace(spec.grid, queue_policies=["fcfs", "conservative"], match_policies=["lonodex", "firstnodex"]),
    )
    tasks = generate_tasks(spec)
    controls = [t for t in tasks if not t["rabbit_active"]]
    assert len(controls) == 4  # 2 queue x 2 match
    assert {(t["queue_policy"], t["match_policy"]) for t in controls} == {
        ("fcfs", "lonodex"),
        ("fcfs", "firstnodex"),
        ("conservative", "lonodex"),
        ("conservative", "firstnodex"),
    }


def test_collapse_preserves_every_distinct_run(tmp_path: Path):
    """No configuration may be lost: a run is (trace, scheduler config)."""
    full_spec = _rabbit_spec(tmp_path, collapse=False)
    collapsed_spec = _rabbit_spec(tmp_path, collapse=True)

    def identities(spec, tag):
        out = []
        for task in generate_tasks(spec):
            gen = materialize_task_inputs(spec, task, tmp_path / f"{tag}-{task['task_id']}")
            out.append(
                (
                    hashlib.sha256(gen["trace"].read_bytes()).hexdigest(),
                    hashlib.sha256(gen["scheduler"].read_bytes()).hexdigest(),
                )
            )
        return out

    full = identities(full_spec, "full")
    collapsed = identities(collapsed_spec, "coll")

    assert set(full) == set(collapsed)                 # nothing dropped
    assert len(collapsed) == len(set(collapsed))       # nothing redundant left
    assert len(full) > len(collapsed)                  # waste actually removed


def test_zero_capacity_makes_every_rabbit_pairing_a_control(tmp_path: Path):
    """With no rabbit capacity the knobs are inert regardless of their values."""
    from dataclasses import replace

    spec = _rabbit_spec(tmp_path, collapse=True)
    spec = replace(spec, rabbit=replace(spec.rabbit, capacity_gib=None))
    tasks = generate_tasks(spec)
    assert len(tasks) == 1
    assert tasks[0]["rabbit_active"] is False


# --- rabbit request distributions ----------------------------------------

def _draw(dist, n=4000, ceiling_percent=50.0, **kw):
    from flux_fiction.ensemble.trace import apply_rabbit_requests

    rows = [{"JobID": str(i)} for i in range(n)]
    out = apply_rabbit_requests(
        rows, seed=7, job_percentage=100, capacity_gib=1_000_000.0,
        ceiling_percent=ceiling_percent, distribution=dist, **kw
    )
    return [float(r["RabbitGiB"]) for r in out]


def test_every_distribution_stays_inside_the_ceiling():
    """The ceiling is a hard upper bound for every shape, including the tails."""
    ceiling = 1_000_000.0 * 0.5
    for dist, kw in (
        ("uniform", {}),
        ("triangular", {}),
        ("loguniform", {}),
        ("gaussian", {}),
        ("gaussian", {"mean_fraction": 1.0}),
        ("pareto", {"tail_alpha": 0.5}),
        ("pareto", {"tail_alpha": 1.5, "tail_min_gib": 1000}),
    ):
        values = _draw(dist, **kw)
        assert min(values) >= 1, (dist, kw)
        assert max(values) <= ceiling, (dist, kw)


def test_pareto_is_long_tailed_and_alpha_controls_the_weight():
    light = _draw("pareto", tail_alpha=2.5, tail_min_gib=1000)
    heavy = _draw("pareto", tail_alpha=1.0, tail_min_gib=1000)

    def top_share(v):
        v = sorted(v)
        return sum(v[int(len(v) * 0.99):]) / sum(v)

    # Long tail: the bulk sits near the floor while the mean is dragged up.
    for v in (light, heavy):
        assert sorted(v)[len(v) // 2] < sum(v) / len(v)
    # Smaller alpha => heavier tail => top percentile owns more of the total.
    assert top_share(heavy) > top_share(light)
    assert sum(heavy) / len(heavy) > sum(light) / len(light)


def test_pareto_floor_sets_the_bulk_scale():
    low = _draw("pareto", tail_alpha=1.5, tail_min_gib=1)
    high = _draw("pareto", tail_alpha=1.5, tail_min_gib=1000)
    assert min(high) >= 1000
    assert sorted(high)[len(high) // 2] > 100 * sorted(low)[len(low) // 2]


def test_gaussian_defaults_match_uniform_mean_but_concentrate():
    import statistics

    uni = _draw("uniform")
    gauss = _draw("gaussian")
    # Same centre ...
    assert abs(statistics.mean(uni) - statistics.mean(gauss)) / statistics.mean(uni) < 0.05
    # ... but tighter, which is the point of choosing it over uniform.
    assert statistics.stdev(gauss) < statistics.stdev(uni)


def test_distributions_are_deterministic_for_a_seed():
    for dist in ("uniform", "triangular", "loguniform", "gaussian", "pareto"):
        assert _draw(dist, n=200) == _draw(dist, n=200), dist


def test_results_recover_metrics_when_run_is_cut_short(tmp_path: Path):
    """A walltime-truncated run still reports its metrics.

    The child writes summary.json the moment it finalizes, but
    parallel_summary.json needs the whole flux instance to tear down first --
    and that teardown must cancel every still-running simulated job, which can
    outlast the allocation. results.csv must not depend on it.
    """
    from flux_fiction.ensemble.campaign import initialize_campaign, write_results_csv

    trace, config, scheduler = _write_base_files(tmp_path)
    spec = load_campaign_spec(_write_spec(tmp_path, trace=trace, config=config, scheduler=scheduler))
    root = initialize_campaign(spec)
    task_id = read_jsonl(tasks_path(root))[0]["task_id"]

    child = root / "batches" / "b000001" / "parallel" / "20260101_000000_manifest" / "runs" / f"0001_{task_id}" / "child"
    child.mkdir(parents=True)
    (child / "status.json").write_text(
        json.dumps({"state": "running", "jobs_completed": 79, "jobs_total": 3997}),
        encoding="utf-8",
    )
    (child / "summary.json").write_text(
        json.dumps(
            {
                "jobs_completed": 79,
                "jobs_total": 3997,
                "makespan_seconds": 361929.0,
                "avg_queue_wait_seconds": 3.5,
                "max_queue_wait_seconds": 703.4,
            }
        ),
        encoding="utf-8",
    )
    # deliberately NO parallel_summary.json, as when the wall lands in teardown
    assert not list((root / "batches" / "b000001" / "parallel").glob("*/parallel_summary.json"))

    out = write_results_csv(root)
    row = next(r for r in csv.DictReader(out.open(encoding="utf-8")) if r["task_id"] == task_id)
    assert row["jobs_completed"] == "79"
    assert float(row["makespan_seconds"]) == 361929.0
    assert float(row["avg_queue_wait_seconds"]) == 3.5
    assert float(row["max_queue_wait_seconds"]) == 703.4
