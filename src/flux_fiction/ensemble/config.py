from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
from typing import Any

from flux_fiction.ensemble.trace import SHAKE_ATTRIBUTES


class EnsembleConfigError(ValueError):
    pass


RABBIT_DISTRIBUTIONS = {
    "uniform", "triangular", "loguniform", "gaussian", "normal", "pareto", "longtail"
}


def _validate_distribution(value: Any, *, name: str) -> str:
    text = str(value).strip().lower()
    if text not in RABBIT_DISTRIBUTIONS:
        raise EnsembleConfigError(
            f"{name} must be one of uniform, triangular, loguniform, gaussian, "
            f"pareto; got {text!r}"
        )
    return text


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    with path.open("rb") as f:
        data = tomllib.load(f)
    if not isinstance(data, dict):
        raise EnsembleConfigError(f"Campaign spec is not a TOML table: {path}")
    return data


def _resolve_path(value: str | None, *, base: Path, must_exist: bool = False) -> str | None:
    if value in (None, ""):
        return None
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = (base / path).resolve()
    else:
        path = path.resolve()
    if must_exist and not path.exists():
        raise EnsembleConfigError(f"Referenced path does not exist: {path}")
    return str(path)


def _as_list(value: Any, *, name: str, default: list[Any] | None = None) -> list[Any]:
    if value is None:
        return list(default or [])
    if isinstance(value, list):
        return value
    raise EnsembleConfigError(f"{name} must be a TOML array")


def _as_percent_list(value: Any, *, name: str, default: list[float]) -> list[float]:
    out = [float(item) for item in _as_list(value, name=name, default=default)]
    for item in out:
        if item < 0 or item > 100:
            raise EnsembleConfigError(f"{name} values must be between 0 and 100")
    return out


@dataclass(frozen=True)
class CampaignSettings:
    name: str
    output_root: str
    batch_size: int = 16
    poll_interval_seconds: float = 60.0
    walltime_minutes: int = 58
    keep_generated_traces: bool = False
    no_faketime: bool = False
    broker_log_level: int = 6
    # Flux broker logs are the dominant artifact (GBs per replica at level 6);
    # set true to stop writing broker.log entirely for throughput runs.
    no_broker_log_file: bool = False
    # Real seconds held back from each batch's walltime so a run that would
    # otherwise be killed can finalize and write its outputs. Scale with the
    # number of jobs a run is expected to COMPLETE: post-sim analysis dumps
    # per-job rows (event log, transitions, allocations). Measured ~6s for 79
    # completed jobs on the 1153-node graph.
    finalize_reserve_seconds: float = 120.0
    # Where compute-node workers execute Flux Fiction:
    #   "container" - podman image path used by the original launcher
    #   "spack"     - host Python + system Flux + Spack Fluxion modules
    worker_runtime: str = "container"
    # Run the parallel replicas through a node-local Flux instance so Fluxion
    # pins each replica to an exclusive core block.
    flux_launch: bool = False
    flux_launch_cores: int | None = None
    # Host/Spack runtime overrides. Empty/None means worker.sh uses its tested
    # site defaults, or the corresponding environment variable if supplied at
    # submit time.
    spack_fluxion_prefix: str | None = None
    spack_faketime_prefix: str | None = None
    spack_gcc_runtime: str | None = None
    spack_view: str | None = None
    spack_ninja_prefix: str | None = None
    meson_pythonpath: str | None = None
    host_jobtap_cc: str | None = None
    host_tmpdir: str | None = None
    # Extra environment handed to every simulation in the campaign, e.g. which
    # libfaketime to preload, which Fluxion modules to load, whether DFTracer
    # is enabled. Baked into the generated worker.sh (and forwarded into the
    # container) so an arm's configuration lives in its spec rather than in
    # whatever shell happened to launch it.
    worker_env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class InputSettings:
    trace: str
    trace_format: str = "auto"
    slice_start: int = 0
    slice_count: int | None = None
    cpus_per_node: int = 1


@dataclass(frozen=True)
class ShakeSettings:
    duplicates: int = 100
    seed: int = 1
    job_percentage: float = 10.0
    degree_seconds: float = 60.0
    relative_cap_percent: float | None = None
    # Which job properties are perturbed. Any subset of
    # interarrival / runtime / nodes; each gets an independent RNG stream.
    attributes: tuple[str, ...] = ("interarrival",)
    # Magnitude for the relative attributes (runtime, nodes), as a percentage
    # of each job's own value. Runtimes span seconds to a day and node counts
    # are small integers, so neither can share interarrival's absolute
    # degree_seconds.
    relative_degree_percent: float = 10.0
    # Upper bound on a shaken node count; inferred from the resource graph.
    max_nodes: int | None = None

    @property
    def attribute(self) -> str:
        """Back-compat single-attribute view."""
        return self.attributes[0] if self.attributes else "interarrival"


@dataclass(frozen=True)
class RabbitSettings:
    # Total rabbit capacity across the whole cluster.
    capacity_gib: float | None = None
    # Capacity of a single rabbit node (one storage parent in the graph).
    per_rabbit_gib: float | None = None
    # What ceiling_percent is a percentage OF:
    #   "cluster"     - the whole cluster's rabbit capacity (historical default)
    #   "rabbit_node" - one rabbit node, so rc=100 means a job may request at
    #                   most one full rabbit. On tuolumne one rabbit is 1.39%
    #                   of the cluster, so the two bases differ by ~72x.
    ceiling_basis: str = "cluster"
    # uniform | triangular | loguniform | gaussian
    distribution: str = "uniform"
    field_name: str = "RabbitGiB"
    # Gaussian shape, as fractions of the ceiling (= ceiling_percent% of total
    # capacity). Defaults centre the draw at mid-range with +/-3 sigma covering
    # the support, matching uniform's mean but concentrating the mass.
    mean_fraction: float = 0.5
    sigma_fraction: float = 1.0 / 6.0
    # Pareto (long-tail) shape: smaller alpha = heavier tail of very large
    # requests. As alpha -> 0 the draw converges to loguniform.
    tail_alpha: float = 1.5
    # Floor of the pareto draw, in GiB: the scale the bulk clusters near.
    tail_min_gib: float = 1.0

    @property
    def ceiling_capacity_gib(self) -> float:
        """Capacity that ceiling_percent is applied to."""
        if self.ceiling_basis == "rabbit_node":
            return float(self.per_rabbit_gib or 0.0)
        return float(self.capacity_gib or 0.0)


@dataclass(frozen=True)
class GridSettings:
    queue_policies: list[str] = field(default_factory=lambda: ["easy"])
    match_policies: list[str] = field(default_factory=lambda: ["lonodex"])
    rabbit_job_percentages: list[float] = field(default_factory=lambda: [0.0])
    rabbit_ceiling_percentages: list[float] = field(default_factory=lambda: [0.0])
    # Request-size distributions to sweep. Defaults to the single value in
    # [rabbit].distribution, so specs written before this axis existed behave
    # exactly as they did. The zero-rabbit control is distribution-independent
    # (no requests are written at all), so the collapse keeps ONE control per
    # policy cell across the whole distribution list rather than one each.
    rabbit_distributions: list[str] = field(default_factory=lambda: ["uniform"])
    # Rabbit requests are emitted only when BOTH the job percentage and the
    # ceiling are non-zero (and capacity is known). Every combination where
    # either is zero yields a byte-identical zero-rabbit trace, so the whole
    # degenerate set collapses to a single control per policy cell instead of
    # being run once per redundant pairing.
    collapse_zero_rabbit: bool = True
    # Named Fluxion builds this campaign may draw on: name -> install prefix on
    # the submitting host. The prefix is the directory holding
    # `lib/flux/modules/sched-fluxion-*.so`. Each named prefix is staged to
    # node-local disk by the worker before any run starts, so the runs read the
    # modules from local storage rather than Lustre.
    fluxion_variants: dict[str, str] = field(default_factory=dict)
    # Which variant each queue policy uses: queue_policy -> variant name, plus
    # an optional "default" key for policies not listed. This is what lets one
    # arm carry a DIFFERENT Fluxion build per policy, which is the whole point
    # -- the traverser patches are policy-dependent (cancel-refresh helps
    # conservative and costs easy/hybrid), so a single build per campaign cannot
    # express a best-of configuration.
    policy_variants: dict[str, str] = field(default_factory=dict)

    def variant_for(self, queue_policy: str) -> str | None:
        """Variant name for a queue policy, or None to leave the build alone."""
        if not self.policy_variants:
            return None
        return self.policy_variants.get(queue_policy) or self.policy_variants.get("default")


@dataclass(frozen=True)
class FluxSettings:
    base_config: str
    base_config_json: str | None = None
    resource_file: str | None = None
    resource_R: str | None = None
    config_overrides: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QueueLimit:
    name: str
    max_active: int | None = None
    max_submit_per_hour: int | None = None


@dataclass(frozen=True)
class SubmissionSettings:
    queues: list[str] = field(default_factory=lambda: ["pbatch"])
    queue_limits: dict[str, QueueLimit] = field(default_factory=dict)
    job_name_prefix: str = "ffe"
    # Which RJMS the launcher submits worker jobs to. Both backends submit one
    # single-node job per batch; only the submit/query/cancel calls differ.
    #   "flux"  - flux python bindings (needs a Flux instance to submit into)
    #   "slurm" - sbatch/squeue/sacct/scancel (native Slurm clusters)
    #   "auto"  - slurm if there is no reachable Flux instance but sbatch exists
    backend: str = "flux"


@dataclass(frozen=True)
class CampaignSpec:
    path: str
    version: int
    campaign: CampaignSettings
    input: InputSettings
    shake: ShakeSettings
    rabbit: RabbitSettings
    grid: GridSettings
    flux: FluxSettings
    submission: SubmissionSettings

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "version": self.version,
            "campaign": self.campaign.__dict__,
            "input": self.input.__dict__,
            "shake": self.shake.__dict__,
            "rabbit": self.rabbit.__dict__,
            "grid": self.grid.__dict__,
            "flux": self.flux.__dict__,
            "submission": {
                "queues": self.submission.queues,
                "job_name_prefix": self.submission.job_name_prefix,
                "backend": self.submission.backend,
                "queue_limits": {
                    name: limit.__dict__
                    for name, limit in self.submission.queue_limits.items()
                },
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), indent=2, sort_keys=True)


def _validate_name(name: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in name.strip())
    clean = clean.strip(".-_")
    if not clean:
        raise EnsembleConfigError("campaign.name must not be empty")
    return clean


def _load_queue_limits(submission_data: dict[str, Any], queues: list[str]) -> dict[str, QueueLimit]:
    raw_limits = submission_data.get("queue_limits") or submission_data.get("queue") or {}
    if not isinstance(raw_limits, dict):
        raise EnsembleConfigError("submission.queue_limits must be a table")
    limits: dict[str, QueueLimit] = {}
    for queue in queues:
        item = raw_limits.get(queue, {})
        if item is None:
            item = {}
        if not isinstance(item, dict):
            raise EnsembleConfigError(f"submission.queue_limits.{queue} must be a table")
        max_active = item.get("max_active")
        max_submit_per_hour = item.get("max_submit_per_hour")
        limits[queue] = QueueLimit(
            name=queue,
            max_active=int(max_active) if max_active is not None else None,
            max_submit_per_hour=(
                int(max_submit_per_hour) if max_submit_per_hour is not None else None
            ),
        )
    return limits


#: Modules that must be present for a directory to count as a Fluxion prefix.
FLUXION_MODULE_NAMES = (
    "sched-fluxion-resource.so",
    "sched-fluxion-feasibility.so",
    "sched-fluxion-qmanager.so",
)


def _load_fluxion_variants(grid_data: dict[str, Any], *, base: Path) -> dict[str, str]:
    raw = grid_data.get("fluxion_variants") or {}
    if not isinstance(raw, dict):
        raise EnsembleConfigError(
            "grid.fluxion_variants must be a table of NAME = \"/install/prefix\" pairs"
        )
    variants: dict[str, str] = {}
    for name, value in raw.items():
        clean = _validate_name(str(name))
        prefix = _resolve_path(str(value), base=base, must_exist=True)
        if prefix is None:
            raise EnsembleConfigError(f"grid.fluxion_variants.{name} must name a directory")
        module_dir = Path(prefix) / "lib" / "flux" / "modules"
        missing = [
            module for module in FLUXION_MODULE_NAMES if not (module_dir / module).is_file()
        ]
        if missing:
            # A prefix that resolves but has no modules produces a campaign that
            # runs to completion against whatever build happened to be loaded --
            # a silently null A/B. Fail at spec load instead.
            raise EnsembleConfigError(
                f"grid.fluxion_variants.{name} ({prefix}) is missing "
                f"{', '.join(missing)} under lib/flux/modules"
            )
        variants[clean] = prefix
    return variants


def _load_policy_variants(grid_data: dict[str, Any]) -> dict[str, str]:
    raw = grid_data.get("policy_variants") or {}
    if not isinstance(raw, dict):
        raise EnsembleConfigError(
            "grid.policy_variants must be a table of QUEUE_POLICY = \"variant\" pairs"
        )
    return {str(key): str(value) for key, value in raw.items()}


def _validate_variant_selection(grid: GridSettings) -> None:
    if grid.policy_variants and not grid.fluxion_variants:
        raise EnsembleConfigError(
            "grid.policy_variants is set but grid.fluxion_variants is empty"
        )
    unknown = {
        name for name in grid.policy_variants.values() if name not in grid.fluxion_variants
    }
    if unknown:
        raise EnsembleConfigError(
            "grid.policy_variants references undefined variant(s): "
            + ", ".join(sorted(unknown))
        )
    # Every swept policy must resolve to a build, or some cells would silently
    # run whatever the worker environment last pointed at.
    if grid.fluxion_variants and not grid.policy_variants:
        raise EnsembleConfigError(
            "grid.fluxion_variants is set but grid.policy_variants selects none of them"
        )
    unmapped = [
        policy for policy in grid.queue_policies if grid.variant_for(policy) is None
    ]
    if grid.policy_variants and unmapped:
        raise EnsembleConfigError(
            "grid.policy_variants has no entry (and no \"default\") for queue policy/policies: "
            + ", ".join(sorted(unmapped))
        )


class _ResourceProbe:
    def __init__(self, *, resource_file: str | None, resource_R: str | None):
        self.resource_file = resource_file
        self.resource_R = resource_R
        self.nnodes = 0
        self.ncpus = 1
        self.ngpus = 0


def _infer_rabbit_capacity_gib(resource_file: str | None, resource_R: str | None) -> float | None:
    if not resource_file and not resource_R:
        return None
    try:
        from flux_fiction._adapters.flux.resources import describe_resource_config

        desc = describe_resource_config(
            _ResourceProbe(resource_file=resource_file, resource_R=resource_R)
        )
        rabbit = desc.get("rabbit_storage") or {}
        parent_count = float(rabbit.get("parent_count") or 0)
        parent_gib = float(
            rabbit.get("max_parent_gib")
            or (
                float(rabbit.get("shares_per_parent") or 0)
                * float(rabbit.get("share_gib") or 0)
            )
            or 0
        )
        capacity = parent_count * parent_gib
        return capacity if capacity > 0 else None
    except Exception:
        return None


def _infer_max_nodes(resource_file: str | None, resource_R: str | None) -> int | None:
    """Node count of the simulated machine, used to cap node shaking."""
    if not resource_file and not resource_R:
        return None
    try:
        from flux_fiction._adapters.flux.resources import describe_resource_config

        desc = describe_resource_config(
            _ResourceProbe(resource_file=resource_file, resource_R=resource_R)
        )
        nnodes = int(desc.get("nnodes") or 0)
        return nnodes if nnodes > 0 else None
    except Exception:
        return None


def _infer_per_rabbit_gib(resource_file: str | None, resource_R: str | None) -> float | None:
    """Capacity of a single rabbit node (one storage parent in the graph)."""
    if not resource_file and not resource_R:
        return None
    try:
        from flux_fiction._adapters.flux.resources import describe_resource_config

        desc = describe_resource_config(
            _ResourceProbe(resource_file=resource_file, resource_R=resource_R)
        )
        rabbit = desc.get("rabbit_storage") or {}
        parent_gib = float(
            rabbit.get("max_parent_gib")
            or (
                float(rabbit.get("shares_per_parent") or 0)
                * float(rabbit.get("share_gib") or 0)
            )
            or 0
        )
        return parent_gib if parent_gib > 0 else None
    except Exception:
        return None


def load_campaign_spec(path: str | os.PathLike[str]) -> CampaignSpec:
    spec_path = Path(path).expanduser().resolve()
    if not spec_path.exists():
        raise FileNotFoundError(f"Campaign spec not found: {spec_path}")
    raw = _load_toml(spec_path)
    base = spec_path.parent

    version = int(raw.get("version", 1))
    if version != 1:
        raise EnsembleConfigError(f"Unsupported campaign spec version {version!r}; expected 1")

    campaign_data = dict(raw.get("campaign") or {})
    input_data = dict(raw.get("input") or raw.get("trace") or {})
    shake_data = dict(raw.get("shake") or {})
    rabbit_data = dict(raw.get("rabbit") or {})
    grid_data = dict(raw.get("grid") or {})
    flux_data = dict(raw.get("flux") or {})
    submission_data = dict(raw.get("submission") or {})

    name = _validate_name(str(campaign_data.get("name") or spec_path.stem))
    output_root = _resolve_path(
        campaign_data.get("output_root") or campaign_data.get("output_dir") or "./ensemble-runs",
        base=base,
    )
    assert output_root is not None

    trace_path = _resolve_path(input_data.get("trace") or input_data.get("job_traces"), base=base, must_exist=True)
    if trace_path is None:
        raise EnsembleConfigError("input.trace is required")
    base_config = _resolve_path(flux_data.get("base_config") or flux_data.get("config_file"), base=base, must_exist=True)
    if base_config is None:
        raise EnsembleConfigError("flux.base_config is required")
    base_config_json = _resolve_path(
        flux_data.get("base_config_json") or flux_data.get("config_json"),
        base=base,
        must_exist=bool(flux_data.get("base_config_json") or flux_data.get("config_json")),
    )
    resource_file = _resolve_path(flux_data.get("resource_file"), base=base, must_exist=bool(flux_data.get("resource_file")))
    resource_R = _resolve_path(flux_data.get("resource_R"), base=base, must_exist=bool(flux_data.get("resource_R")))

    queues = [str(item) for item in _as_list(submission_data.get("queues"), name="submission.queues", default=["pbatch"])]
    if not queues:
        raise EnsembleConfigError("submission.queues must contain at least one queue")

    backend = str(submission_data.get("backend", "flux")).strip().lower()
    if backend not in {"flux", "slurm", "auto"}:
        raise EnsembleConfigError(
            f"submission.backend must be 'flux', 'slurm', or 'auto'; got {backend!r}"
        )

    worker_runtime = str(campaign_data.get("worker_runtime", "container")).strip().lower()
    if worker_runtime == "podman":
        worker_runtime = "container"
    if worker_runtime not in {"container", "spack"}:
        raise EnsembleConfigError(
            "campaign.worker_runtime must be 'container' or 'spack'; "
            f"got {worker_runtime!r}"
        )
    flux_launch_cores = (
        int(campaign_data["flux_launch_cores"])
        if campaign_data.get("flux_launch_cores") is not None
        else None
    )
    if flux_launch_cores is not None and flux_launch_cores < 1:
        raise EnsembleConfigError("campaign.flux_launch_cores must be >= 1")

    def _optional_text(name: str, *aliases: str) -> str | None:
        for key in (name, *aliases):
            value = campaign_data.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    raw_worker_env = raw.get("worker_env", campaign_data.get("worker_env")) or {}
    if not isinstance(raw_worker_env, dict):
        raise EnsembleConfigError("worker_env must be a table of NAME = \"value\" pairs")
    worker_env: dict[str, str] = {}
    for key, value in raw_worker_env.items():
        name_text = str(key).strip()
        # These names are pasted into generated shell as `NAME="..."`, so a
        # name that is not a plain shell identifier would be a shell injection
        # rather than an environment variable.
        if not name_text or not name_text.replace("_", "").isalnum() or name_text[0].isdigit():
            raise EnsembleConfigError(f"worker_env key is not a valid variable name: {key!r}")
        if isinstance(value, bool):
            worker_env[name_text] = "1" if value else "0"
        else:
            worker_env[name_text] = str(value)

    # `attributes` is the list form; `attribute` remains as the older scalar.
    raw_attributes = shake_data.get("attributes", shake_data.get("attribute", "interarrival"))
    if isinstance(raw_attributes, str):
        raw_attributes = [raw_attributes]
    attributes = tuple(str(item).strip().lower() for item in _as_list(raw_attributes, name="shake.attributes"))
    unsupported = [name for name in attributes if name not in SHAKE_ATTRIBUTES]
    if unsupported:
        raise EnsembleConfigError(
            "shake.attributes may only contain "
            f"{', '.join(SHAKE_ATTRIBUTES)}; got {', '.join(sorted(unsupported))}"
        )
    if not attributes:
        raise EnsembleConfigError("shake.attributes must not be empty")

    shake = ShakeSettings(
        duplicates=int(shake_data.get("duplicates", 100)),
        seed=int(shake_data.get("seed", 1)),
        job_percentage=float(shake_data.get("job_percentage", 10.0)),
        degree_seconds=float(shake_data.get("degree_seconds", 60.0)),
        relative_cap_percent=(
            float(shake_data["relative_cap_percent"])
            if shake_data.get("relative_cap_percent") is not None
            else None
        ),
        attributes=attributes,
        relative_degree_percent=float(shake_data.get("relative_degree_percent", 10.0)),
        max_nodes=(
            int(shake_data["max_nodes"])
            if shake_data.get("max_nodes") is not None
            else _infer_max_nodes(resource_file, resource_R)
        ),
    )
    if shake.duplicates < 1:
        raise EnsembleConfigError("shake.duplicates must be >= 1")
    if shake.relative_degree_percent < 0:
        raise EnsembleConfigError("shake.relative_degree_percent must be >= 0")
    if shake.job_percentage < 0 or shake.job_percentage > 100:
        raise EnsembleConfigError("shake.job_percentage must be between 0 and 100")
    if shake.degree_seconds < 0:
        raise EnsembleConfigError("shake.degree_seconds must be >= 0")

    # [rabbit].distribution is the single-value default; grid.rabbit_distributions
    # sweeps it. Listing it in the grid is what makes gaussian and long-tail one
    # campaign instead of two.
    rabbit_distribution = _validate_distribution(
        rabbit_data.get("distribution", "uniform"), name="rabbit.distribution"
    )
    rabbit_distributions = [
        _validate_distribution(item, name="grid.rabbit_distributions")
        for item in _as_list(
            grid_data.get("rabbit_distributions"),
            name="grid.rabbit_distributions",
            default=[rabbit_distribution],
        )
    ]
    if not rabbit_distributions:
        raise EnsembleConfigError("grid.rabbit_distributions must not be empty")
    if len(set(rabbit_distributions)) != len(rabbit_distributions):
        raise EnsembleConfigError("grid.rabbit_distributions must not repeat a distribution")

    grid = GridSettings(
        queue_policies=[str(item) for item in _as_list(grid_data.get("queue_policies"), name="grid.queue_policies", default=["easy"])],
        match_policies=[str(item) for item in _as_list(grid_data.get("match_policies"), name="grid.match_policies", default=["lonodex"])],
        rabbit_job_percentages=_as_percent_list(
            grid_data.get("rabbit_job_percentages"),
            name="grid.rabbit_job_percentages",
            default=[0.0],
        ),
        rabbit_ceiling_percentages=_as_percent_list(
            grid_data.get("rabbit_ceiling_percentages"),
            name="grid.rabbit_ceiling_percentages",
            default=[0.0],
        ),
        rabbit_distributions=rabbit_distributions,
        collapse_zero_rabbit=bool(grid_data.get("collapse_zero_rabbit", True)),
        fluxion_variants=_load_fluxion_variants(grid_data, base=base),
        policy_variants=_load_policy_variants(grid_data),
    )
    if not grid.queue_policies or not grid.match_policies:
        raise EnsembleConfigError("grid.queue_policies and grid.match_policies must not be empty")
    _validate_variant_selection(grid)
    rabbit_tail_alpha = float(rabbit_data.get("tail_alpha", 1.5))
    if rabbit_tail_alpha <= 0:
        raise EnsembleConfigError("rabbit.tail_alpha must be > 0")
    rabbit_tail_min_gib = float(rabbit_data.get("tail_min_gib", 1.0))
    if rabbit_tail_min_gib < 1:
        raise EnsembleConfigError("rabbit.tail_min_gib must be >= 1")
    rabbit_mean_fraction = float(rabbit_data.get("mean_fraction", 0.5))
    rabbit_sigma_fraction = float(rabbit_data.get("sigma_fraction", 1.0 / 6.0))
    if not 0.0 <= rabbit_mean_fraction <= 1.0:
        raise EnsembleConfigError("rabbit.mean_fraction must be between 0 and 1 (fraction of the ceiling)")
    if rabbit_sigma_fraction <= 0:
        raise EnsembleConfigError("rabbit.sigma_fraction must be > 0")

    rabbit_capacity_gib = (
        float(rabbit_data["capacity_gib"])
        if rabbit_data.get("capacity_gib") is not None
        else _infer_rabbit_capacity_gib(resource_file, resource_R)
    )
    rabbit_per_node_gib = (
        float(rabbit_data["per_rabbit_gib"])
        if rabbit_data.get("per_rabbit_gib") is not None
        else _infer_per_rabbit_gib(resource_file, resource_R)
    )
    rabbit_ceiling_basis = str(rabbit_data.get("ceiling_basis", "cluster")).strip().lower()
    if rabbit_ceiling_basis not in {"cluster", "rabbit_node"}:
        raise EnsembleConfigError(
            "rabbit.ceiling_basis must be 'cluster' or 'rabbit_node'; "
            f"got {rabbit_ceiling_basis!r}"
        )
    if rabbit_ceiling_basis == "rabbit_node" and not rabbit_per_node_gib:
        raise EnsembleConfigError(
            "rabbit.ceiling_basis = 'rabbit_node' needs a per-rabbit capacity: set "
            "rabbit.per_rabbit_gib or point flux.resource_R at a graph with rabbits"
        )
    if any(value > 0 for value in grid.rabbit_ceiling_percentages) and rabbit_capacity_gib is None:
        raise EnsembleConfigError(
            "rabbit.capacity_gib is required when rabbit ceiling percentages are non-zero "
            "and capacity cannot be inferred from flux.resource_file/resource_R"
        )

    return CampaignSpec(
        path=str(spec_path),
        version=version,
        campaign=CampaignSettings(
            name=name,
            output_root=output_root,
            batch_size=int(campaign_data.get("batch_size", 16)),
            poll_interval_seconds=float(campaign_data.get("poll_interval_seconds", 60.0)),
            walltime_minutes=int(campaign_data.get("walltime_minutes", 58)),
            keep_generated_traces=bool(campaign_data.get("keep_generated_traces", False)),
            no_faketime=bool(campaign_data.get("no_faketime", False)),
            broker_log_level=int(campaign_data.get("broker_log_level", 6)),
            no_broker_log_file=bool(campaign_data.get("no_broker_log_file", False)),
            finalize_reserve_seconds=float(
                campaign_data.get("finalize_reserve_seconds", 120.0)
            ),
            worker_runtime=worker_runtime,
            flux_launch=bool(campaign_data.get("flux_launch", False)),
            flux_launch_cores=flux_launch_cores,
            spack_fluxion_prefix=_optional_text("spack_fluxion_prefix"),
            spack_faketime_prefix=_optional_text("spack_faketime_prefix"),
            spack_gcc_runtime=_optional_text(
                "spack_gcc_runtime", "spack_gcc_runtime_prefix"
            ),
            spack_view=_optional_text("spack_view"),
            spack_ninja_prefix=_optional_text("spack_ninja_prefix"),
            meson_pythonpath=_optional_text("meson_pythonpath"),
            host_jobtap_cc=_optional_text("host_jobtap_cc"),
            host_tmpdir=_optional_text("host_tmpdir"),
            worker_env=worker_env,
        ),
        input=InputSettings(
            trace=trace_path,
            trace_format=str(input_data.get("format", input_data.get("trace_format", "auto"))),
            slice_start=int(input_data.get("slice_start", 0)),
            slice_count=(
                int(input_data["slice_count"]) if input_data.get("slice_count") is not None else None
            ),
            cpus_per_node=int(input_data.get("cpus_per_node", 1)),
        ),
        shake=shake,
        rabbit=RabbitSettings(
            capacity_gib=rabbit_capacity_gib,
            per_rabbit_gib=rabbit_per_node_gib,
            ceiling_basis=rabbit_ceiling_basis,
            distribution=rabbit_distribution,
            field_name=str(rabbit_data.get("field_name", "RabbitGiB")),
            mean_fraction=rabbit_mean_fraction,
            sigma_fraction=rabbit_sigma_fraction,
            tail_alpha=rabbit_tail_alpha,
            tail_min_gib=rabbit_tail_min_gib,
        ),
        grid=grid,
        flux=FluxSettings(
            base_config=base_config,
            base_config_json=base_config_json,
            resource_file=resource_file,
            resource_R=resource_R,
            config_overrides=dict(flux_data.get("config_overrides") or {}),
        ),
        submission=SubmissionSettings(
            queues=queues,
            queue_limits=_load_queue_limits(submission_data, queues),
            job_name_prefix=str(submission_data.get("job_name_prefix", "ffe")),
            backend=backend,
        ),
    )
