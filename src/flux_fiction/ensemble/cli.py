from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
from urllib.parse import urlparse

from flux_fiction.ensemble.campaign import (
    batches_path,
    campaign_root,
    copy_example_spec,
    delete_campaign,
    failed_batch_report,
    initialize_campaign,
    initialize_partial_campaign,
    merge_host_results,
    publish_host_results,
    load_campaign_snapshot,
    materialize_task_inputs,
    progress_csv_path,
    read_json,
    read_jsonl,
    refresh_status,
    reset_campaign,
    retry_failed_batches,
    run_launcher,
    run_worker,
    status_path,
    tasks_path,
    update_status,
    write_results_csv,
)
from flux_fiction.ensemble.config import EnsembleConfigError, load_campaign_spec
from flux_fiction.ensemble.hosts import detect_host, load_host_config


def _root_from_resume_arg(value: str) -> Path:
    path = Path(value).expanduser().resolve()
    if path.is_file():
        if path.name in {"state.json", "status.json", "campaign_spec.json"}:
            return path.parent
        if path.suffix.lower() == ".toml":
            return campaign_root(load_campaign_spec(path))
    return path


def _print_plan(root: Path, *, limit: int = 10) -> None:
    tasks = read_jsonl(tasks_path(root))
    batches = read_jsonl(batches_path(root))
    spec = load_campaign_snapshot(root)
    print(f"Campaign:       {spec.campaign.name}")
    print(f"Root:           {root}")
    print(f"Tasks:          {len(tasks)}")
    print(f"Batches:        {len(batches)}")
    print(f"Batch size:     {spec.campaign.batch_size}")
    print(f"Queues:         {', '.join(spec.submission.queues)}")
    print(f"Poll interval:  {spec.campaign.poll_interval_seconds:.1f}s")
    print(f"Keep traces:    {spec.campaign.keep_generated_traces}")
    distributions = list(
        getattr(spec.grid, "rabbit_distributions", None) or [spec.rabbit.distribution]
    )
    print(f"Shake:          {', '.join(spec.shake.attributes)} @ {spec.shake.job_percentage:g}% of jobs")
    print(f"Distributions:  {', '.join(distributions)}")
    full_grid = (
        spec.shake.duplicates
        * len(spec.grid.queue_policies)
        * len(spec.grid.match_policies)
        * len(spec.grid.rabbit_job_percentages)
        * len(spec.grid.rabbit_ceiling_percentages)
        * len(distributions)
    )
    saved = full_grid - len(tasks)
    controls = sum(1 for t in tasks if not t.get("rabbit_active", True))
    if saved > 0:
        print(
            "Rabbit collapse: {saved} redundant task(s) skipped ({pct:.0f}% of the "
            "{full}-cell grid); {controls} zero-rabbit control(s) kept".format(
                saved=saved, pct=100.0 * saved / full_grid, full=full_grid, controls=controls
            )
        )
    print("")
    for task in tasks[:max(0, limit)]:
        # rabbit_distribution is absent from manifests generated before the
        # distribution axis existed.
        info = {"rabbit_distribution": "-", **task}
        print(
            "{task_id} queue_policy={queue_policy} match_policy={match_policy} "
            "shake_seed={shake_seed} rabbit_jobs={rabbit_job_percentage}% "
            "rabbit_ceiling={rabbit_ceiling_percentage}% "
            "dist={rabbit_distribution}".format(**info)
        )
    if len(tasks) > limit:
        print(f"... {len(tasks) - limit} more tasks")


def _print_task_progress(payload: dict) -> None:
    progress = payload.get("task_progress") or []
    if not progress:
        return
    print("")
    print("Per-policy completion (sim jobs completed before now/timeout):")
    for p in progress:
        total = int(p.get("jobs_total") or 0)
        done = int(p.get("jobs_completed") or 0)
        pct = f"{100.0 * done / total:5.1f}%" if total else "   ? %"
        print(
            "  {task:52s} {done:6d}/{total:<6d} {pct}  [{state}]".format(
                task=p.get("task_id", "?"),
                done=done,
                total=total,
                pct=pct,
                state=p.get("status") or p.get("child_state") or "?",
            )
        )


def _status_text(root: Path) -> int:
    spec = load_campaign_snapshot(root)
    payload = refresh_status(root, spec=spec)
    batch_counts = payload["batch_counts"]
    task_counts = payload["task_counts"]
    print(f"Campaign:       {payload['campaign']}")
    print(f"Root:           {payload['root']}")
    print(f"Updated:        {payload['updated_at']}")
    print(
        "Batches:        queued={queued} submitted={submitted} pending={pending} running={running} "
        "succeeded={succeeded} failed={failed} total={total}".format(
            total=payload["total_batches"],
            **batch_counts,
        )
    )
    print(
        "Tasks:          succeeded={succeeded} failed={failed} remaining={remaining} total={total}".format(
            total=payload["total_tasks"],
            **task_counts,
        )
    )
    print(
        "Sim jobs:       completed={} / {} (across all policies)".format(
            payload.get("sim_jobs_completed", 0),
            payload.get("sim_jobs_total", 0),
        )
    )
    _print_task_progress(payload)
    if payload.get("recent_failures"):
        print("")
        print("Recent failures:")
        for failure in payload["recent_failures"]:
            print(f"  {failure['batch_id']}: {failure.get('failure_reason') or 'see log'}")
            print(f"    {failure['log']}")
    return 0


class _StatusHandler(BaseHTTPRequestHandler):
    root: Path

    def _send_json(self, payload, status: int = 200) -> None:
        data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"/", "/status"}:
                spec = load_campaign_snapshot(self.root)
                self._send_json(refresh_status(self.root, spec=spec))
            elif parsed.path == "/tasks":
                self._send_json({"tasks": read_jsonl(tasks_path(self.root))})
            elif parsed.path == "/batches":
                self._send_json({"batches": read_jsonl(batches_path(self.root))})
            elif parsed.path == "/state":
                self._send_json(read_json(self.root / "state.json"))
            elif parsed.path == "/failures":
                payload = refresh_status(
                    self.root,
                    spec=load_campaign_snapshot(self.root),
                )
                self._send_json({"failures": payload.get("recent_failures", [])})
            else:
                self._send_json({"error": "not found"}, status=404)
        except Exception as exc:
            self._send_json({"error": repr(exc)}, status=500)


def _serve(root: Path, host: str, port: int) -> int:
    handler = type("StatusHandler", (_StatusHandler,), {"root": root})
    server = ThreadingHTTPServer((host, port), handler)
    print(f"Serving Flux Fiction ensemble status at http://{host}:{port}/status")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 130
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and launch Flux Fiction ensemble campaigns."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    example = sub.add_parser("example", help="Write an example campaign TOML.")
    example.add_argument("path")

    init = sub.add_parser("init", help="Create task and batch manifests.")
    init.add_argument("spec")
    init.add_argument("--force", action="store_true", help="Overwrite manifests/state.")

    plan = sub.add_parser("plan", help="Create manifests if needed and print the task plan.")
    plan.add_argument("spec")
    plan.add_argument("--limit", type=int, default=10)

    launch = sub.add_parser("launch", help="Run one or more launcher ticks.")
    launch.add_argument("spec")
    launch.add_argument("--once", action="store_true", help="Submit one tick and exit.")
    launch.add_argument("--dry-run", action="store_true", help="Print submissions without calling Flux.")
    launch.add_argument("--poll", action="store_true", help="Use periodic polling instead of Flux reactor watches.")

    run = sub.add_parser("run", help="Poll and submit until the campaign drains.")
    run.add_argument("spec")
    run.add_argument("--dry-run", action="store_true", help="Print submissions without calling Flux.")
    run.add_argument("--poll", action="store_true", help="Use periodic polling instead of Flux reactor watches.")

    resume = sub.add_parser("resume", help="Resume from a campaign root, state.json, or status.json.")
    resume.add_argument("root_or_state")
    resume.add_argument("--once", action="store_true", help="Submit one tick and exit.")
    resume.add_argument("--dry-run", action="store_true", help="Print submissions without calling Flux.")
    resume.add_argument("--poll", action="store_true", help="Use periodic polling instead of Flux reactor watches.")

    status = sub.add_parser("status", help="Print campaign status.")
    status.add_argument("root")
    status.add_argument("--json", action="store_true", help="Emit raw status JSON.")

    progress = sub.add_parser(
        "progress",
        help="Print how many sim jobs each policy completed (works during a run and after a timeout).",
    )
    progress.add_argument("root")
    progress.add_argument("--json", action="store_true", help="Emit raw task-progress JSON.")

    results = sub.add_parser(
        "results",
        help="Write results.csv: one tidy row per task joining grid factors to measured metrics.",
    )
    results.add_argument("root")

    run_partial = sub.add_parser(
        "run-partial",
        help="Run this host's share of a campaign split across several clusters.",
    )
    run_partial.add_argument("spec")
    run_partial.add_argument(
        "--hosts", required=True, help="Host config TOML describing every cluster."
    )
    run_partial.add_argument(
        "--host",
        default=None,
        help="Which host to run as. Defaults to auto-detection from the hostname.",
    )
    run_partial.add_argument("--once", action="store_true", help="Submit one tick and exit.")
    run_partial.add_argument("--dry-run", action="store_true", help="Print submissions only.")
    run_partial.add_argument("--poll", action="store_true", help="Poll instead of Flux reactor waits.")
    run_partial.add_argument("--force", action="store_true", help="Re-partition into a fresh root.")
    run_partial.add_argument(
        "--plan-only",
        action="store_true",
        help="Show this host's assignment and exit without submitting.",
    )

    merge = sub.add_parser(
        "merge",
        help="Merge every host's published results.csv into one table in the shared root.",
    )
    merge.add_argument("--hosts", required=True, help="Host config TOML.")
    merge.add_argument("--out", default=None, help="Output path (default <shared_root>/results_all.csv).")

    serve = sub.add_parser("serve", help="Serve campaign status over HTTP.")
    serve.add_argument("root")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)

    worker = sub.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("root")
    worker.add_argument("batch_id")

    materialize = sub.add_parser("materialize-task", help=argparse.SUPPRESS)
    materialize.add_argument("root")
    materialize.add_argument("task_id")
    materialize.add_argument("out_dir")

    retry = sub.add_parser(
        "retry-failed",
        help="Requeue batches the launcher marked failed so they are submitted again.",
    )
    retry.add_argument("root")
    retry.add_argument(
        "--dry-run", action="store_true",
        help="List what would be requeued and exit without touching anything.",
    )
    retry.add_argument(
        "--include-partial", action="store_true",
        help=(
            "Also requeue failed batches that DID produce child summaries "
            "(default: only total losses, so a batch killed at its walltime "
            "keeps the data it wrote)."
        ),
    )
    retry.add_argument(
        "--batch", action="append", default=None, dest="batch_ids",
        help="Restrict to this batch id; repeatable.",
    )

    reset = sub.add_parser(
        "reset",
        help="Archive the campaign root (rename to <name>-archived-<timestamp>) so a fresh run can start.",
    )
    reset.add_argument("root")

    delete = sub.add_parser("delete", help="Delete the campaign root and all of its data.")
    delete.add_argument("root")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "example":
            copy_example_spec(Path(args.path).expanduser())
            print(f"Wrote {args.path}")
            return 0
        if args.command in {"init", "plan", "launch", "run"}:
            spec = load_campaign_spec(args.spec)
            root = initialize_campaign(spec, force=bool(getattr(args, "force", False)))
            if args.command == "init":
                print(f"Campaign root: {root}")
                print(f"Status:        {status_path(root)}")
                return 0
            if args.command == "plan":
                _print_plan(root, limit=args.limit)
                return 0
            if args.command == "launch":
                return run_launcher(
                    root,
                    spec,
                    once=args.once,
                    dry_run=args.dry_run,
                    use_reactor=not args.poll,
                )
            return run_launcher(
                root,
                spec,
                once=False,
                dry_run=args.dry_run,
                use_reactor=not args.poll,
            )
        if args.command == "resume":
            root = _root_from_resume_arg(args.root_or_state)
            spec = load_campaign_snapshot(root)
            return run_launcher(
                root,
                spec,
                once=args.once,
                dry_run=args.dry_run,
                use_reactor=not args.poll,
            )
        if args.command == "status":
            root = _root_from_resume_arg(args.root)
            if args.json:
                payload = refresh_status(root, spec=load_campaign_snapshot(root))
                print(json.dumps(payload, indent=2, sort_keys=True))
                return 0
            return _status_text(root)
        if args.command == "progress":
            root = _root_from_resume_arg(args.root)
            # Dumb-simple: read each sim's live status.json off the shared
            # filesystem; no Flux queries needed, works mid-run or post-timeout.
            payload = update_status(root, spec=load_campaign_snapshot(root))
            if args.json:
                print(json.dumps(payload.get("task_progress", []), indent=2, sort_keys=True))
                return 0
            print(f"Campaign:       {payload['campaign']}")
            print(
                "Sim jobs:       completed={} / {} (across all policies)".format(
                    payload.get("sim_jobs_completed", 0),
                    payload.get("sim_jobs_total", 0),
                )
            )
            _print_task_progress(payload)
            return 0
        if args.command == "run-partial":
            host_config = load_host_config(args.hosts)
            host_name = args.host or detect_host(host_config)
            if not host_name:
                raise EnsembleConfigError(
                    "Could not determine the host. Pass --host explicitly (configured: "
                    f"{', '.join(sorted(host_config.hosts))})."
                )
            spec = load_campaign_spec(args.spec)
            root, assignment = initialize_partial_campaign(
                spec, host_name, host_config, force=args.force
            )
            host = host_config.require(host_name)
            print(f"Host:            {host_name} ({host.backend})")
            print(f"Campaign root:   {root}")
            print(f"Shared context:  {host_config.shared_root}")
            print(
                "Assignment:      {assigned}/{total} tasks "
                "(share {share} of {cycle}) ".format(
                    assigned=assignment["assigned_tasks"],
                    total=assignment["total_tasks"],
                    share=host.share,
                    cycle="+".join(
                        f"{n}:{s}" for n, s in sorted(assignment["hosts"].items())
                    ),
                )
            )
            print(f"Packing:         {host.runs_per_node} concurrent run(s)/node, queues={host.queues}")
            if args.plan_only:
                for task_id in assignment["task_ids"][:20]:
                    print(f"  {task_id}")
                extra = len(assignment["task_ids"]) - 20
                if extra > 0:
                    print(f"  ... {extra} more")
                return 0
            spec_for_run = load_campaign_snapshot(root)
            rc = run_launcher(
                root,
                spec_for_run,
                once=args.once,
                dry_run=args.dry_run,
                use_reactor=not args.poll,
            )
            if not args.dry_run:
                published = publish_host_results(root, host_config, host_name)
                if published:
                    print(f"Published results to shared context: {published}")
            return rc
        if args.command == "merge":
            host_config = load_host_config(args.hosts)
            out_path, per_host = merge_host_results(
                host_config, Path(args.out).expanduser() if args.out else None
            )
            total = sum(per_host.values())
            print(f"Merged {total} task row(s) -> {out_path}")
            for name, count in sorted(per_host.items()):
                note = "" if count else "   (nothing published yet)"
                print(f"  {name:12s} {count:6d}{note}")
            return 0
        if args.command == "results":
            root = _root_from_resume_arg(args.root)
            path = write_results_csv(root)
            rows = max(0, sum(1 for _ in path.open(encoding="utf-8")) - 1)
            print(f"Wrote {path} ({rows} task rows)")
            progress_csv = progress_csv_path(root)
            if progress_csv.exists():
                samples = max(0, sum(1 for _ in progress_csv.open(encoding="utf-8")) - 1)
                print(f"Wall-clock throughput samples: {progress_csv} ({samples} rows)")
            return 0
        if args.command == "serve":
            return _serve(_root_from_resume_arg(args.root), args.host, args.port)
        if args.command == "worker":
            return run_worker(args.root, args.batch_id)
        if args.command == "materialize-task":
            root = Path(args.root).expanduser().resolve()
            spec = load_campaign_snapshot(root)
            tasks = {task["task_id"]: task for task in read_jsonl(tasks_path(root))}
            if args.task_id not in tasks:
                raise SystemExit(f"Unknown task id: {args.task_id}")
            materialize_task_inputs(spec, tasks[args.task_id], Path(args.out_dir).expanduser().resolve())
            return 0
        if args.command == "retry-failed":
            root = _root_from_resume_arg(args.root)
            report = failed_batch_report(root)
            total_loss = [item for item in report if item["total_loss"]]
            partial = [item for item in report if not item["total_loss"]]
            print(f"Failed batches:      {len(report)}")
            print(f"  produced nothing:  {len(total_loss)}  (requeued by default)")
            print(f"  produced partial:  {len(partial)}  (kept unless --include-partial)")
            selected = retry_failed_batches(
                root,
                batch_ids=args.batch_ids,
                include_partial=args.include_partial,
                dry_run=args.dry_run,
            )
            if not selected:
                print("\nNothing to requeue.")
                return 0
            print("")
            for item in selected[:20]:
                print(
                    "  {batch_id}  tasks={task_count:<3} summaries={summaries:<3} "
                    "rc={return_code} {flux_result}".format(**item)
                )
            if len(selected) > 20:
                print(f"  ... {len(selected) - 20} more")
            if args.dry_run:
                print(f"\nDRY-RUN: would requeue {len(selected)} batch(es). Nothing changed.")
                return 0
            print(
                f"\nRequeued {len(selected)} batch(es): cleared their batch_result.json and "
                "dropped them from state.json.\nA running launcher picks these up on its next "
                "tick (it re-reads state.json every tick); no restart needed.\n"
                "If the launcher happened to rewrite state.json in the same instant, simply "
                "run this again -- it is idempotent."
            )
            return 0
        if args.command == "reset":
            root = _root_from_resume_arg(args.root)
            archive = reset_campaign(root)
            if archive is None:
                print(f"Nothing to reset: {root} does not exist")
            else:
                print(f"Archived campaign root: {root} -> {archive}")
            return 0
        if args.command == "delete":
            root = _root_from_resume_arg(args.root)
            if delete_campaign(root):
                print(f"Deleted campaign root: {root}")
            else:
                print(f"Nothing to delete: {root} does not exist")
            return 0
    except (FileNotFoundError, EnsembleConfigError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
