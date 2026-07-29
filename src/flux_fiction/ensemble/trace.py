from __future__ import annotations

import csv
from datetime import datetime, timedelta
import hashlib
import math
from pathlib import Path
import random
from typing import Any


SACCT_FIELDS = ["JobID", "NNodes", "NCPUS", "NGPUS", "Timelimit", "Submit", "Elapsed"]


def _parse_datetime(value: str) -> datetime:
    text = str(value).strip().strip('"')
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%b %d, %Y @ %H:%M:%S.%f",
        "%b %d, %Y @ %H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(text)


def _format_datetime(dt: datetime) -> str:
    text = dt.isoformat(timespec="microseconds")
    return text.rstrip("0").rstrip(".")


def _seconds_to_hms(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    whole = int(math.ceil(seconds))
    hours, rem = divmod(whole, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _read_csv_rows(path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(line for line in f if not line.startswith("#"))
        rows = [dict(row) for row in reader]
        return list(reader.fieldnames or []), rows


def _slice_rows(rows: list[dict[str, str]], *, start: int = 0, count: int | None = None) -> list[dict[str, str]]:
    if start < 0:
        raise ValueError("slice_start must be >= 0")
    end = None if count is None else start + max(0, count)
    return [dict(row) for row in rows[start:end]]


def detect_trace_format(fieldnames: list[str], requested: str = "auto") -> str:
    requested = requested.lower()
    if requested != "auto":
        return requested
    fields = set(fieldnames)
    if {"Submit", "Elapsed", "Timelimit", "NNodes", "NCPUS"}.issubset(fields):
        return "sacct"
    if {"job.submittime", "job.node.count"}.issubset(fields):
        return "tuolumne"
    raise ValueError(
        "Could not detect trace format. Set input.format to 'sacct' or 'tuolumne'."
    )


def _normalize_sacct_rows(rows: list[dict[str, str]], *, rabbit_field: str) -> list[dict[str, str]]:
    normalized = []
    for idx, row in enumerate(rows):
        item = dict(row)
        item.setdefault("JobID", str(idx + 1))
        # The sim's trace reader requires an NGPUS column when the machine has
        # GPUs; keep it present (default 0) even for traces that omit it.
        if not item.get("NGPUS"):
            item["NGPUS"] = "0"
        if rabbit_field not in item:
            item[rabbit_field] = item.get("RabbitGiB", item.get("RabbitStorageGiB", "0"))
        normalized.append(item)
    return normalized


def _normalize_tuolumne_rows(
    rows: list[dict[str, str]],
    *,
    cpus_per_node: int,
    rabbit_field: str,
) -> list[dict[str, str]]:
    normalized = []
    for idx, row in enumerate(rows):
        try:
            submit = _parse_datetime(row.get("job.submittime") or row.get("@timestamp") or "")
            nodes = max(1, int(float(row.get("job.node.count") or 1)))
            duration_raw = row.get("event.duration_seconds") or row.get("elapsed_s") or ""
            if duration_raw in ("", "-", None):
                end_raw = row.get("event.end") or row.get("@timestamp")
                elapsed = max(1.0, (_parse_datetime(end_raw) - submit).total_seconds()) if end_raw else 1.0
            else:
                elapsed = max(1.0, float(str(duration_raw).replace(",", "")))
            limit_raw = row.get("job.timelimit_seconds") or ""
            timelimit = elapsed if limit_raw in ("", "-", None) else max(elapsed, float(str(limit_raw).replace(",", "")))
        except Exception:
            continue
        normalized.append(
            {
                "JobID": str(row.get("job.id") or idx + 1),
                "NNodes": str(nodes),
                "NCPUS": str(nodes * max(1, int(cpus_per_node))),
                "NGPUS": "0",
                "Timelimit": _seconds_to_hms(timelimit),
                "Submit": _format_datetime(submit),
                "Elapsed": _seconds_to_hms(elapsed),
                rabbit_field: "0",
            }
        )
    return normalized


def load_normalized_trace(
    path: str | Path,
    *,
    trace_format: str = "auto",
    slice_start: int = 0,
    slice_count: int | None = None,
    cpus_per_node: int = 1,
    rabbit_field: str = "RabbitGiB",
) -> list[dict[str, str]]:
    fieldnames, rows = _read_csv_rows(path)
    rows = _slice_rows(rows, start=slice_start, count=slice_count)
    fmt = detect_trace_format(fieldnames, trace_format)
    if fmt == "sacct":
        return _normalize_sacct_rows(rows, rabbit_field=rabbit_field)
    if fmt in {"tuolumne", "tuo"}:
        return _normalize_tuolumne_rows(
            rows,
            cpus_per_node=cpus_per_node,
            rabbit_field=rabbit_field,
        )
    raise ValueError(f"Unsupported trace format: {fmt}")


SHAKE_ATTRIBUTES = ("interarrival", "runtime", "nodes")


def _attribute_seed(seed: int, attribute: str) -> int:
    """A distinct, stable RNG stream per shaken attribute.

    Each attribute draws its own selection and magnitudes, so enabling
    ``nodes`` does not reshuffle which jobs ``interarrival`` perturbs. Derived
    from the replica seed, so a replica stays reproducible either way.
    """
    digest = hashlib.sha256(f"{int(seed)}:{attribute}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _hms_to_seconds(value: str) -> float:
    """Parse ``[D-]HH:MM:SS`` (the sacct/normalizer duration format)."""
    text = str(value).strip()
    if not text:
        return 0.0
    days = 0.0
    if "-" in text:
        day_part, _, text = text.partition("-")
        try:
            days = float(day_part)
        except ValueError:
            return 0.0
    try:
        numbers = [float(part) for part in text.split(":")]
    except ValueError:
        return 0.0
    seconds = 0.0
    for number in numbers:
        seconds = seconds * 60.0 + number
    return seconds + days * 86400.0


def _selected_indices(count: int, percentage: float, rng: random.Random) -> set[int]:
    if count <= 0 or percentage <= 0:
        return set()
    selected_count = int(round(count * min(100.0, percentage) / 100.0))
    if selected_count >= count:
        return set(range(count))
    indices = list(range(count))
    rng.shuffle(indices)
    return set(indices[:selected_count])


def shake_interarrival(
    rows: list[dict[str, str]],
    *,
    seed: int,
    job_percentage: float,
    degree_seconds: float,
    relative_cap_percent: float | None = None,
) -> list[dict[str, str]]:
    rng = random.Random(int(seed))
    out = [dict(row) for row in rows]
    if not out or degree_seconds <= 0 or job_percentage <= 0:
        return out

    submit_times = [_parse_datetime(row["Submit"]) for row in out]
    selected = _selected_indices(len(out), job_percentage, rng)
    for idx in sorted(selected):
        if idx == 0:
            continue
        interarrival = (submit_times[idx] - submit_times[idx - 1]).total_seconds()
        perturb = float(degree_seconds)
        if relative_cap_percent is not None:
            perturb = min(perturb, abs(interarrival) * float(relative_cap_percent) / 100.0)
        if perturb <= 0:
            continue
        sign = -1.0 if rng.random() < 0.5 else 1.0
        delta = sign * perturb * rng.random()
        out[idx]["Submit"] = _format_datetime(submit_times[idx] + timedelta(seconds=delta))
        out[idx]["t_submit"] = "{:.6f}".format((submit_times[idx] + timedelta(seconds=delta)).timestamp())
    return out


def shake_runtime(
    rows: list[dict[str, str]],
    *,
    seed: int,
    job_percentage: float,
    relative_degree_percent: float,
) -> list[dict[str, str]]:
    """Perturb each selected job's measured runtime by +/- a share of itself.

    Runtimes in a real trace span seconds to a day, so the magnitude has to be
    relative: a fixed +/-60 s is a rounding error on a 24 h job and a total
    rewrite of a 90 s one. Each selected job draws its own factor from
    ``[1 - d, 1 + d]``, so the perturbation is symmetric in expectation and the
    workload's overall load is preserved.

    ``Timelimit`` is the job's *requested* wall time and must stay at or above
    the runtime, matching the invariant the trace normalizer establishes; a job
    shaken past its old limit carries the limit up with it.
    """
    rng = random.Random(int(seed))
    out = [dict(row) for row in rows]
    degree = float(relative_degree_percent) / 100.0
    if not out or degree <= 0 or job_percentage <= 0:
        return out

    for idx in sorted(_selected_indices(len(out), job_percentage, rng)):
        row = out[idx]
        elapsed = _hms_to_seconds(row.get("Elapsed", ""))
        if elapsed <= 0:
            continue
        factor = 1.0 + rng.uniform(-degree, degree)
        shaken = max(1.0, elapsed * factor)
        row["Elapsed"] = _seconds_to_hms(shaken)
        timelimit = _hms_to_seconds(row.get("Timelimit", ""))
        if shaken > timelimit:
            row["Timelimit"] = _seconds_to_hms(shaken)
    return out


def shake_nodes(
    rows: list[dict[str, str]],
    *,
    seed: int,
    job_percentage: float,
    relative_degree_percent: float,
    max_nodes: int | None = None,
) -> list[dict[str, str]]:
    """Perturb each selected job's node count by +/- a share of itself.

    Node counts are small integers and most of a real trace is single-node
    work, so a pure percentage would round to zero for the majority of jobs and
    the shake would be a no-op where it matters most. The step is therefore at
    least one node. A single-node job drawing a downward step stays at one --
    it cannot request less -- which biases the very smallest jobs upward; the
    effect on offered load is slight because those jobs contribute little of
    it, and it is identical across replicas, so cross-replica comparisons are
    unaffected.

    ``NCPUS``/``NGPUS`` are rescaled at the job's own per-node ratio so the
    request stays self-consistent.
    """
    rng = random.Random(int(seed))
    out = [dict(row) for row in rows]
    degree = float(relative_degree_percent) / 100.0
    if not out or degree <= 0 or job_percentage <= 0:
        return out

    for idx in sorted(_selected_indices(len(out), job_percentage, rng)):
        row = out[idx]
        try:
            nodes = int(float(row.get("NNodes") or 1))
        except ValueError:
            continue
        nodes = max(1, nodes)
        step = max(1, int(round(nodes * degree)))
        sign = -1 if rng.random() < 0.5 else 1
        shaken = nodes + sign * rng.randint(1, step)
        shaken = max(1, shaken)
        if max_nodes:
            shaken = min(int(max_nodes), shaken)
        if shaken == nodes:
            continue
        for field in ("NCPUS", "NGPUS"):
            try:
                value = int(float(row.get(field) or 0))
            except ValueError:
                continue
            if value <= 0:
                continue
            row[field] = str(max(1, int(round(value / nodes)) * shaken))
        row["NNodes"] = str(shaken)
    return out


def apply_shake(
    rows: list[dict[str, str]],
    *,
    seed: int,
    attributes: list[str] | tuple[str, ...],
    job_percentage: float,
    degree_seconds: float,
    relative_cap_percent: float | None = None,
    relative_degree_percent: float = 10.0,
    max_nodes: int | None = None,
) -> list[dict[str, str]]:
    """Apply every requested shake attribute, in a fixed order.

    Each attribute gets its own RNG stream, so which jobs get their runtime
    perturbed is independent of which get their submit time moved. Interarrival
    keeps the bare replica seed so specs that shake only submit times reproduce
    byte-identically to before this function existed.
    """
    unknown = [name for name in attributes if name not in SHAKE_ATTRIBUTES]
    if unknown:
        raise ValueError(
            f"Unsupported shake attribute(s): {', '.join(sorted(unknown))}; "
            f"supported: {', '.join(SHAKE_ATTRIBUTES)}"
        )
    out = [dict(row) for row in rows]
    # Fixed order regardless of how the spec lists them, so the result depends
    # only on the set of attributes, not on their spelling order in the TOML.
    if "runtime" in attributes:
        out = shake_runtime(
            out,
            seed=_attribute_seed(seed, "runtime"),
            job_percentage=job_percentage,
            relative_degree_percent=relative_degree_percent,
        )
    if "nodes" in attributes:
        out = shake_nodes(
            out,
            seed=_attribute_seed(seed, "nodes"),
            job_percentage=job_percentage,
            relative_degree_percent=relative_degree_percent,
            max_nodes=max_nodes,
        )
    if "interarrival" in attributes:
        out = shake_interarrival(
            out,
            seed=int(seed),
            job_percentage=job_percentage,
            degree_seconds=degree_seconds,
            relative_cap_percent=relative_cap_percent,
        )
    return out


def apply_rabbit_requests(
    rows: list[dict[str, str]],
    *,
    seed: int,
    job_percentage: float,
    capacity_gib: float,
    ceiling_percent: float,
    distribution: str = "uniform",
    field_name: str = "RabbitGiB",
    mean_fraction: float = 0.5,
    sigma_fraction: float = 1.0 / 6.0,
    tail_alpha: float = 1.5,
    tail_min_gib: float = 1.0,
) -> list[dict[str, str]]:
    """Assign per-job rabbit storage requests.

    ``ceiling_percent`` sets the UPPER BOUND of the draw, as a percentage of
    total cluster rabbit capacity -- it is not the amount each job asks for and
    the distribution is not centred on it. Every selected job draws its own
    independent value from the support [0, ceiling].

    ``mean_fraction`` / ``sigma_fraction`` apply to the gaussian distribution
    and are expressed as fractions of the ceiling, so the defaults (0.5, 1/6)
    put the mean at mid-range with +/-3 sigma spanning the full support -- the
    same mean as ``uniform`` but concentrated rather than flat.

    ``tail_alpha`` / ``tail_min_gib`` apply to the pareto (long-tail)
    distribution: smaller alpha puts more weight in the tail of very large
    requests, and tail_min_gib sets the floor that the bulk clusters near.
    """
    rng = random.Random(int(seed))
    out = [dict(row) for row in rows]
    for row in out:
        row[field_name] = "0"
    if not out or job_percentage <= 0 or ceiling_percent <= 0 or capacity_gib <= 0:
        return out

    selected = _selected_indices(len(out), job_percentage, rng)
    ceiling = float(capacity_gib) * float(ceiling_percent) / 100.0
    for idx in sorted(selected):
        if distribution == "uniform":
            value = rng.uniform(0.0, ceiling)
        elif distribution == "triangular":
            value = rng.triangular(0.0, ceiling, ceiling / 3.0)
        elif distribution == "loguniform":
            low = 1.0
            value = math.exp(rng.uniform(math.log(low), math.log(max(low, ceiling))))
        elif distribution in {"pareto", "longtail"}:
            # Bounded (truncated) Pareto by exact inverse CDF:
            #   x = (L^-a - u*(L^-a - H^-a))^(-1/a)
            # u=0 -> L and u=1 -> H, so the support is exactly [L, ceiling]
            # with no rejection loop and no mass piled on the bounds. Most
            # requests sit near the floor with a power-law tail of large ones;
            # smaller alpha = heavier tail. As alpha -> 0 this converges to
            # loguniform, so it generalises that option with a tunable weight.
            # The floor sets the scale of the "many small requests" bulk. It
            # matters as much as alpha: against a ceiling six orders of
            # magnitude larger, a 1 GiB floor collapses the bulk to a couple of
            # GiB, which is not a meaningful allocation on a PiB-scale rabbit.
            low = max(1.0, float(tail_min_gib))
            high = max(low * (1.0 + 1e-9), ceiling)
            alpha = max(1e-6, float(tail_alpha))
            u = rng.random()
            lo_a = low ** (-alpha)
            hi_a = high ** (-alpha)
            value = (lo_a - u * (lo_a - hi_a)) ** (-1.0 / alpha)
        elif distribution in {"gaussian", "normal"}:
            mu = ceiling * float(mean_fraction)
            sigma = max(1e-9, ceiling * float(sigma_fraction))
            # Truncated normal by rejection, so the support stays [0, ceiling]
            # instead of piling mass on the bounds. Bounded attempts keep it
            # deterministic and terminating even for an extreme mean/sigma.
            candidate = mu
            for _ in range(32):
                candidate = rng.gauss(mu, sigma)
                if 0.0 <= candidate <= ceiling:
                    break
            value = min(ceiling, max(0.0, candidate))
        else:
            raise ValueError(f"Unsupported rabbit distribution: {distribution}")
        out[idx][field_name] = str(max(1, int(math.ceil(value))))
    return out


def write_trace_csv(path: str | Path, rows: list[dict[str, str]], *, rabbit_field: str = "RabbitGiB") -> None:
    fieldnames = list(SACCT_FIELDS)
    if any("t_submit" in row for row in rows):
        fieldnames.append("t_submit")
    if rabbit_field not in fieldnames:
        fieldnames.append(rabbit_field)
    extras = sorted({key for row in rows for key in row if key not in fieldnames})
    fieldnames.extend(extras)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
