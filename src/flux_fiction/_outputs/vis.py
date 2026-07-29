from __future__ import annotations

import csv
import json
import logging
import math
import os
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)


STORAGE_RESOURCE_TYPES = {"ssd", "rabbit", "storage"}
IGNORED_RESOURCE_TYPES = {"slot"}


def match_policy_from_config(config_json: str | None) -> str:
    if not config_json:
        return ""
    try:
        with open(config_json, "r") as f:
            data = json.load(f)
    except Exception:
        logger.exception("Could not read scheduler config JSON: %s", config_json)
        return ""

    section = data.get("sched-fluxion-resource", {})
    policy = section.get("match-policy") or section.get("policy") or ""
    return str(policy)


def match_policy_is_node_exclusive(config_json: str | None) -> bool:
    policy = match_policy_from_config(config_json).strip().lower()
    return bool(policy and policy.endswith("x"))


def summarize_and_plot_resources(
    simulation,
    adapter,
    resource_desc: dict[str, Any],
    output_dir: str,
    *,
    config_json: str | None = None,
    config_exclusive: bool = False,
    make_plots: bool = True,
) -> dict[str, Any]:
    os.makedirs(output_dir or ".", exist_ok=True)

    match_policy = match_policy_from_config(config_json)
    node_exclusive = bool(config_exclusive or match_policy.lower().endswith("x"))
    capacities = _resource_capacities(resource_desc)
    rows = _job_resource_rows(
        simulation,
        adapter,
        resource_desc,
        capacities,
        node_exclusive=node_exclusive,
    )

    usage_csv = os.path.join(output_dir, "resource_usage_timeseries.csv")
    allocation_csv = os.path.join(output_dir, "resource_allocations.csv")
    plot_path = os.path.join(output_dir, "resource_utilization.png")

    segments = build_usage_segments(rows, capacities)
    write_allocation_csv(allocation_csv, rows, capacities)
    write_usage_csv(usage_csv, segments, capacities)
    # The CSVs above carry everything the plot shows, so skipping the render
    # loses no data -- resource_usage_timeseries.csv can be replotted later.
    written_plot_path = plot_usage(segments, capacities, plot_path) if make_plots else None

    summary = resource_summary(rows, capacities)
    summary["plot_written"] = bool(written_plot_path)
    summary["node_exclusive"] = node_exclusive
    summary["match_policy"] = match_policy
    summary["usage_csv"] = usage_csv
    summary["allocation_csv"] = allocation_csv
    summary["plot_path"] = written_plot_path or plot_path

    print_resource_summary(summary)
    print(f"Resource usage CSV: {usage_csv}")
    print(f"Resource allocation CSV: {allocation_csv}")
    if summary.get("plot_written"):
        print(f"Resource utilization plot: {summary['plot_path']}")
    return summary


def _resource_capacities(resource_desc: dict[str, Any]) -> dict[str, float]:
    capacities: dict[str, float] = {}

    nnodes = int(resource_desc.get("nnodes") or 0)
    cores_per_node = int(resource_desc.get("cores_per_node") or 0)
    gpus_per_node = int(resource_desc.get("gpus_per_node") or 0)

    if nnodes > 0:
        capacities["node"] = float(nnodes)
    if nnodes > 0 and cores_per_node > 0:
        capacities["core"] = float(nnodes * cores_per_node)
    if nnodes > 0 and gpus_per_node > 0:
        capacities["gpu"] = float(nnodes * gpus_per_node)

    leaf_resources = resource_desc.get("leaf_resources") or {}
    for resource_type, facts in leaf_resources.items():
        if resource_type in IGNORED_RESOURCE_TYPES:
            continue
        if (
            isinstance(facts, dict)
            and resource_type in STORAGE_RESOURCE_TYPES
            and _as_float(facts.get("size")) > 0
        ):
            count = _as_float(facts.get("size"))
        else:
            count = _as_float(facts.get("count") if isinstance(facts, dict) else facts)
        if count > 0:
            capacities[str(resource_type)] = count

    rabbit = resource_desc.get("rabbit_storage") or {}
    rabbit_capacity_gib = float(
        rabbit.get("parent_count") or 0
    ) * float(
        rabbit.get("max_parent_gib")
        or (
            float(rabbit.get("shares_per_parent") or 0)
            * float(rabbit.get("share_gib") or 0)
        )
        or 0
    )
    rabbit_type = str(rabbit.get("resource_type") or "ssd")
    if rabbit_capacity_gib > 0 and rabbit_type not in capacities:
        capacities[rabbit_type] = rabbit_capacity_gib

    return {
        key: value
        for key, value in sorted(capacities.items())
        if value and value > 0
    }


def _job_resource_rows(
    simulation,
    adapter,
    resource_desc: dict[str, Any],
    capacities: dict[str, float],
    *,
    node_exclusive: bool,
) -> list[dict[str, Any]]:
    rows = []
    cores_per_node = int(resource_desc.get("cores_per_node") or 0)
    gpus_per_node = int(resource_desc.get("gpus_per_node") or 0)

    for jobid, job in simulation.job_map.items():
        start = job.state_transitions.get("STARTED")
        finish = job.state_transitions.get("COMPLETED")
        if start in (None, "") or finish in (None, ""):
            continue
        start = float(start)
        finish = float(finish)
        if finish <= start:
            continue

        r_counts = _allocated_counts_from_adapter(adapter, jobid)
        request_counts = _jobspec_resource_counts(job.jobspec)
        counts: dict[str, float] = {}
        node_count = (
            r_counts.get("node")
            or _nodelist_count(adapter, jobid)
            or request_counts.get("node")
            or getattr(job, "nnodes", 0)
            or 0
        )

        for resource_type in capacities:
            if resource_type == "node":
                counts[resource_type] = node_count
            elif resource_type in {"core", "gpu"}:
                if node_exclusive and node_count > 0:
                    per_node = cores_per_node if resource_type == "core" else gpus_per_node
                    counts[resource_type] = node_count * per_node
                else:
                    counts[resource_type] = (
                        r_counts.get(resource_type)
                        or request_counts.get(resource_type)
                        or 0
                    )
            else:
                counts[resource_type] = (
                    request_counts.get(resource_type)
                    or _job_specific_resource_count(job, resource_type)
                    or r_counts.get(resource_type)
                    or 0
                )

        rows.append({
            "jobid": jobid,
            "trace_idx": getattr(job, "trace_index", ""),
            "start": start,
            "finish": finish,
            "duration": finish - start,
            "node_exclusive": node_exclusive,
            "resources": counts,
            "r_counts": r_counts,
            "request_counts": request_counts,
        })

    rows.sort(key=lambda row: (row["start"], row["finish"], str(row["jobid"])))
    return rows


def _allocated_counts_from_adapter(adapter, jobid) -> dict[str, float]:
    try:
        diag = adapter.get_job_diagnostics(jobid)
    except Exception:
        logger.debug("Could not get job diagnostics for %s", jobid, exc_info=True)
        return {}

    r_obj = None
    if isinstance(diag, dict):
        kvs = diag.get("kvs")
        if isinstance(kvs, dict):
            r_obj = kvs.get("R")
    return _allocated_counts_from_r(r_obj)


def _allocated_counts_from_r(r_obj) -> dict[str, float]:
    r_obj = _jsonish(r_obj)
    if not isinstance(r_obj, dict):
        return {}

    counts: Counter[str] = Counter()
    r_lite = r_obj.get("execution", {}).get("R_lite", [])
    for item in r_lite:
        rank_count = _idset_count(item.get("rank"))
        if rank_count <= 0:
            continue
        counts["node"] += rank_count
        children = item.get("children", {}) or {}
        for resource_type, ids in children.items():
            counts[str(resource_type)] += rank_count * _idset_count(ids)
    return {key: float(value) for key, value in counts.items() if value > 0}


def _jobspec_resource_counts(jobspec: dict[str, Any]) -> dict[str, float]:
    counts: Counter[str] = Counter()

    def walk(resource: dict[str, Any], multiplier: int = 1):
        resource_type = str(resource.get("type") or "")
        count = int(math.ceil(float(resource.get("count", 1) or 1)))
        total = multiplier * count
        children = resource.get("with") or []

        if resource_type == "node":
            counts["node"] += total

        if children:
            for child in children:
                if isinstance(child, dict):
                    walk(child, total)
        elif resource_type and resource_type not in IGNORED_RESOURCE_TYPES:
            counts[resource_type] += total

    for resource in jobspec.get("resources", []) or []:
        if isinstance(resource, dict):
            walk(resource)

    return {key: float(value) for key, value in counts.items() if value > 0}


def _job_specific_resource_count(job, resource_type: str) -> float:
    if resource_type == getattr(job, "rabbit_storage_resource_type", None):
        return float(getattr(job, "rabbit_storage_request_count", 0) or 0)
    return 0.0


def _nodelist_count(adapter, jobid) -> int:
    try:
        nodes, _source = adapter.nodelist_lookup(jobid)
        return len(nodes or [])
    except Exception:
        return 0


def build_usage_segments(
    rows: list[dict[str, Any]],
    capacities: dict[str, float],
) -> list[dict[str, Any]]:
    if not rows:
        return []

    times = sorted({row["start"] for row in rows} | {row["finish"] for row in rows})
    segments = []
    origin = times[0]

    for t0, t1 in zip(times[:-1], times[1:]):
        if t1 <= t0:
            continue

        active = [row for row in rows if row["start"] <= t0 < row["finish"]]
        usage = {
            resource_type: sum(
                float(row["resources"].get(resource_type, 0) or 0)
                for row in active
            )
            for resource_type in capacities
        }
        segments.append({
            "t0": t0,
            "t1": t1,
            "hours": (t0 - origin) / 3600.0,
            "duration": t1 - t0,
            "active_jobs": len(active),
            "usage": usage,
        })

    return segments


def resource_summary(
    rows: list[dict[str, Any]],
    capacities: dict[str, float],
) -> dict[str, Any]:
    if not rows:
        return {
            "makespan_hours": 0.0,
            "resources": {},
            "plot_written": False,
        }

    start = min(row["start"] for row in rows)
    finish = max(row["finish"] for row in rows)
    makespan_hours = max(0.0, (finish - start) / 3600.0)
    resources = {}

    for resource_type, capacity in capacities.items():
        used_hours = sum(
            float(row["resources"].get(resource_type, 0) or 0)
            * float(row["duration"])
            / 3600.0
            for row in rows
        )
        total_hours = capacity * makespan_hours
        utilization = 100.0 * used_hours / total_hours if total_hours > 0 else 0.0
        resources[resource_type] = {
            "capacity": capacity,
            "total_hours": total_hours,
            "used_hours": used_hours,
            "utilization_pct": utilization,
        }

    return {
        "makespan_hours": makespan_hours,
        "resources": resources,
        "plot_written": False,
    }


def write_allocation_csv(
    path: str,
    rows: list[dict[str, Any]],
    capacities: dict[str, float],
) -> None:
    resource_types = list(capacities)
    fieldnames = [
        "trace_idx",
        "jobid",
        "start",
        "finish",
        "duration",
        "node_exclusive",
    ]
    for resource_type in resource_types:
        fieldnames.extend([
            f"{resource_type}_used",
            f"{resource_type}_from_R",
            f"{resource_type}_requested",
        ])

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = {
                "trace_idx": row["trace_idx"],
                "jobid": row["jobid"],
                "start": f"{row['start']:.6f}",
                "finish": f"{row['finish']:.6f}",
                "duration": f"{row['duration']:.6f}",
                "node_exclusive": row["node_exclusive"],
            }
            for resource_type in resource_types:
                out[f"{resource_type}_used"] = _fmt(row["resources"].get(resource_type, 0))
                out[f"{resource_type}_from_R"] = _fmt(row["r_counts"].get(resource_type, 0))
                out[f"{resource_type}_requested"] = _fmt(row["request_counts"].get(resource_type, 0))
            writer.writerow(out)


def write_usage_csv(
    path: str,
    segments: list[dict[str, Any]],
    capacities: dict[str, float],
) -> None:
    resource_types = list(capacities)
    fieldnames = ["t0", "t1", "hours", "duration", "active_jobs"]
    for resource_type in resource_types:
        fieldnames.extend([f"{resource_type}_used", f"{resource_type}_utilization_pct"])

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for segment in segments:
            out = {
                "t0": f"{segment['t0']:.6f}",
                "t1": f"{segment['t1']:.6f}",
                "hours": f"{segment['hours']:.6f}",
                "duration": f"{segment['duration']:.6f}",
                "active_jobs": segment["active_jobs"],
            }
            for resource_type in resource_types:
                used = float(segment["usage"].get(resource_type, 0) or 0)
                capacity = float(capacities.get(resource_type, 0) or 0)
                out[f"{resource_type}_used"] = _fmt(used)
                out[f"{resource_type}_utilization_pct"] = (
                    f"{(100.0 * used / capacity):.6f}" if capacity > 0 else ""
                )
            writer.writerow(out)


def plot_usage(
    segments: list[dict[str, Any]],
    capacities: dict[str, float],
    path: str,
) -> str | None:
    if not segments or not capacities:
        return None

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        logger.warning("matplotlib is unavailable; writing SVG resource utilization plot")
        svg_path = os.path.splitext(path)[0] + ".svg"
        return _plot_usage_svg(segments, capacities, svg_path)

    compute_types = [
        resource_type for resource_type in capacities
        if resource_type not in STORAGE_RESOURCE_TYPES
    ]
    storage_types = [
        resource_type for resource_type in capacities
        if resource_type in STORAGE_RESOURCE_TYPES
    ]
    panels = [("Compute", compute_types)]
    if storage_types:
        panels.append(("Rabbit / Storage", storage_types))

    time_axis = _time_axis(segments)
    fig, axes = plt.subplots(
        len(panels),
        1,
        figsize=(11, 3.8 * len(panels)),
        sharex=True,
        squeeze=False,
    )
    axes = axes[:, 0]

    for ax, (title, resource_types) in zip(axes, panels):
        for resource_type in resource_types:
            xs, ys = _step_xy(
                segments,
                resource_type,
                capacities[resource_type],
                time_axis["divisor"],
            )
            ax.step(xs, ys, where="post", linewidth=2, label=_display_name(resource_type))
        ax.set_title(f"{title} Resource Utilization")
        ax.set_ylabel("Utilization (%)")
        ax.set_ylim(bottom=0)
        ax.grid(True, axis="y", alpha=0.25)
        ax.legend(loc="upper right")

    axes[-1].set_xlabel(
        f"Simulation time ({time_axis['unit']} since first job start)"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def _plot_usage_svg(
    segments: list[dict[str, Any]],
    capacities: dict[str, float],
    path: str,
) -> str | None:
    compute_types = [
        resource_type for resource_type in capacities
        if resource_type not in STORAGE_RESOURCE_TYPES
    ]
    storage_types = [
        resource_type for resource_type in capacities
        if resource_type in STORAGE_RESOURCE_TYPES
    ]
    panels = [("Compute Resource Utilization", compute_types)]
    if storage_types:
        panels.append(("Rabbit / Storage Resource Utilization", storage_types))

    colors = [
        "#1f77b4", "#2ca02c", "#ff7f0e", "#d62728",
        "#9467bd", "#8c564b", "#17becf",
    ]
    width = 1100
    panel_height = 310
    margin_l = 78
    margin_r = 28
    margin_t = 54
    margin_b = 58
    height = panel_height * len(panels) + margin_b
    plot_w = width - margin_l - margin_r
    time_axis = _time_axis(segments)
    max_axis = max(
        (segment["t1"] - segments[0]["t0"]) / time_axis["divisor"]
        for segment in segments
    )
    max_axis = max(max_axis, 1e-9)

    def esc(value) -> str:
        return (
            str(value)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:Arial,sans-serif;font-size:13px;fill:#222}'
        '.title{font-size:18px;font-weight:700}.axis{stroke:#333;stroke-width:1}'
        '.grid{stroke:#ddd;stroke-width:1}.series{fill:none;stroke-width:2.4}</style>',
    ]

    for panel_idx, (title, resource_types) in enumerate(panels):
        top = margin_t + panel_idx * panel_height
        plot_h = panel_height - margin_t - 36
        is_bottom_panel = panel_idx == len(panels) - 1
        max_pct = 100.0
        for resource_type in resource_types:
            for segment in segments:
                used = float(segment["usage"].get(resource_type, 0) or 0)
                cap = float(capacities.get(resource_type, 0) or 0)
                if cap > 0:
                    max_pct = max(max_pct, 100.0 * used / cap)
        y_max = max(10.0, math.ceil(max_pct / 10.0) * 10.0)

        svg.append(f'<text class="title" x="{margin_l}" y="{top - 20}">{esc(title)}</text>')

        for tick in range(0, 6):
            pct = y_max * tick / 5.0
            y = top + plot_h - (pct / y_max) * plot_h
            svg.append(f'<line class="grid" x1="{margin_l}" y1="{y:.2f}" x2="{margin_l + plot_w}" y2="{y:.2f}"/>')
            svg.append(f'<text x="{margin_l - 10}" y="{y + 4:.2f}" text-anchor="end">{pct:.0f}%</text>')

        for tick in range(0, 6):
            value = max_axis * tick / 5.0
            x = margin_l + (value / max_axis) * plot_w
            svg.append(f'<line class="grid" x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}"/>')
            svg.append(f'<line class="axis" x1="{x:.2f}" y1="{top + plot_h}" x2="{x:.2f}" y2="{top + plot_h + 5}"/>')
            if is_bottom_panel:
                svg.append(
                    f'<text x="{x:.2f}" y="{top + plot_h + 22}" text-anchor="middle">'
                    f'{_tick_label(value)}</text>'
                )

        svg.append(f'<line class="axis" x1="{margin_l}" y1="{top}" x2="{margin_l}" y2="{top + plot_h}"/>')
        svg.append(f'<line class="axis" x1="{margin_l}" y1="{top + plot_h}" x2="{margin_l + plot_w}" y2="{top + plot_h}"/>')
        svg.append(f'<text x="{18}" y="{top + plot_h / 2:.2f}" transform="rotate(-90 18 {top + plot_h / 2:.2f})">Utilization (%)</text>')

        for idx, resource_type in enumerate(resource_types):
            color = colors[idx % len(colors)]
            points = _svg_step_points(
                segments,
                resource_type,
                capacities[resource_type],
                margin_l,
                top,
                plot_w,
                plot_h,
                max_axis,
                y_max,
                time_axis["divisor"],
            )
            svg.append(f'<polyline class="series" stroke="{color}" points="{points}"/>')
            lx = margin_l + plot_w - 150
            ly = top + 20 + idx * 22
            svg.append(f'<line x1="{lx}" y1="{ly - 4}" x2="{lx + 26}" y2="{ly - 4}" stroke="{color}" stroke-width="2.4"/>')
            svg.append(f'<text x="{lx + 34}" y="{ly}">{esc(_display_name(resource_type))}</text>')

    axis_y = height - 18
    svg.append(
        f'<text x="{width / 2:.2f}" y="{axis_y}" text-anchor="middle">'
        f'Simulation time ({time_axis["unit"]} since first job start)</text>'
    )
    svg.append("</svg>")

    with open(path, "w") as f:
        f.write("\n".join(svg))
    return path


def _svg_step_points(
    segments: list[dict[str, Any]],
    resource_type: str,
    capacity: float,
    margin_l: float,
    top: float,
    plot_w: float,
    plot_h: float,
    max_axis: float,
    y_max: float,
    divisor: float,
) -> str:
    origin = segments[0]["t0"]
    points = []
    for segment in segments:
        x0_value = (segment["t0"] - origin) / divisor
        x1_value = (segment["t1"] - origin) / divisor
        used = float(segment["usage"].get(resource_type, 0) or 0)
        pct = 100.0 * used / capacity if capacity > 0 else 0.0
        x0 = margin_l + (x0_value / max_axis) * plot_w
        x1 = margin_l + (x1_value / max_axis) * plot_w
        y = top + plot_h - (pct / y_max) * plot_h
        points.append(f"{x0:.2f},{y:.2f}")
        points.append(f"{x1:.2f},{y:.2f}")
    return " ".join(points)


def print_resource_summary(summary: dict[str, Any]) -> None:
    print("Makespan (hours): {:.1f}".format(float(summary.get("makespan_hours", 0.0))))
    if summary.get("match_policy"):
        print(
            "Match policy: {}{}".format(
                summary["match_policy"],
                " (node-exclusive accounting)" if summary.get("node_exclusive") else "",
            )
        )

    for resource_type, facts in summary.get("resources", {}).items():
        label = _display_name(resource_type)
        unit = _hour_unit(resource_type)
        print(f"Total {label}-{unit}: {facts['total_hours']:,.1f}")
        print(f"Used {label}-{unit}: {facts['used_hours']:,.1f}")
        print(f"Average {label}-Utilization: {facts['utilization_pct']:.2f}%")


def _step_xy(
    segments: list[dict[str, Any]],
    resource_type: str,
    capacity: float,
    divisor: float,
) -> tuple[list[float], list[float]]:
    xs = []
    ys = []
    for segment in segments:
        used = float(segment["usage"].get(resource_type, 0) or 0)
        pct = 100.0 * used / capacity if capacity > 0 else 0.0
        if not xs:
            xs.append(float(segment["t0"] - segments[0]["t0"]) / divisor)
            ys.append(pct)
        xs.append(float(segment["t1"] - segments[0]["t0"]) / divisor)
        ys.append(pct)
    return xs, ys


def _time_axis(segments: list[dict[str, Any]]) -> dict[str, float | str]:
    span_s = max(0.0, float(segments[-1]["t1"] - segments[0]["t0"]))
    if span_s < 120.0:
        return {"unit": "seconds", "divisor": 1.0}
    if span_s < 7200.0:
        return {"unit": "minutes", "divisor": 60.0}
    return {"unit": "hours", "divisor": 3600.0}


def _tick_label(value: float) -> str:
    if abs(value) >= 10:
        return f"{value:.0f}"
    if abs(value) >= 1:
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _display_name(resource_type: str) -> str:
    names = {
        "node": "Node",
        "core": "Core",
        "gpu": "GPU",
        "ssd": "Rabbit SSD",
    }
    return names.get(resource_type, resource_type.replace("_", " ").title())


def _hour_unit(resource_type: str) -> str:
    if resource_type in STORAGE_RESOURCE_TYPES:
        return "GiB-Hours"
    return "Hours"


def _idset_count(value) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, int):
        return 1
    total = 0
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            total += abs(int(end) - int(start)) + 1
        else:
            total += 1
    return total


def _jsonish(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _as_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _fmt(value) -> str:
    value = _as_float(value)
    if value.is_integer():
        return str(int(value))
    return f"{value:.6f}"
