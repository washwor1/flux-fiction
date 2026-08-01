from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any
import logging
import sys
import os
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError

try:
    from pydantic import ConfigDict, model_validator
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import root_validator
    ConfigDict = None
    _PYDANTIC_V2 = False


logger = logging.getLogger(__name__)

_PATH_FIELDS = {
    "job_traces": False,
    "config_file": False,
    "source_config_file": False,
    "config_json": False,
    "raw_jobspec_file": False,
    "resource_file": False,
    "resource_R": False,
    "output_dir": True,
    "log_file": True,
    "status_file": True,
    "summary_file": True,
    "faketime_timestamp_file": True,
    "otel_bridge_socket": True,
    "otel_summary_file": True,
    "otel_spans_file": True,
    "otel_bridge_log_file": True,
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _workspace_root() -> Path:
    override = os.environ.get("FLUX_FICTION_WORKSPACE_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return _repo_root().parent


def _legacy_prefixes() -> list[tuple[str, str]]:
    repo_root = _repo_root()
    workspace_root = _workspace_root()
    return [
        ("/home/j/Desktop/flux/sc25_poster/flux-fiction", str(repo_root)),
        ("/home/j/Desktop/flux/sc25_poster", str(workspace_root)),
        ("/work/flux/sc25_poster/flux-fiction", str(repo_root)),
        ("/work/flux/sc25_poster", str(workspace_root)),
    ]


def _parse_path_map_entry(entry: str) -> tuple[str, str]:
    if "=" in entry:
        src, dst = entry.split("=", 1)
    elif ">" in entry:
        src, dst = entry.split(">", 1)
    else:
        raise ValueError(
            "path map entries must look like '/host/prefix=/container/prefix'"
        )
    src = src.rstrip("/")
    dst = dst.rstrip("/")
    if not src or not dst:
        raise ValueError("path map source and destination must be non-empty")
    return src, dst


def _path_mappings() -> list[tuple[str, str]]:
    mappings: list[tuple[str, str]] = []
    raw = os.environ.get("FLUX_FICTION_PATH_MAP", "")
    for entry in raw.split(os.pathsep):
        entry = entry.strip()
        if entry:
            mappings.append(_parse_path_map_entry(entry))

    mappings.extend(_legacy_prefixes())

    # Prefer the longest prefix when multiple mappings could match.
    return sorted(dict.fromkeys(mappings), key=lambda item: len(item[0]), reverse=True)


def _is_under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def _rewrite_path(value: str, *, for_output: bool = False) -> str:
    if not value:
        return value

    path = Path(value).expanduser()
    if not path.is_absolute():
        return str(path)
    if path.exists():
        return str(path)

    raw_path = str(path)
    for src, dst in _path_mappings():
        if not _is_under(raw_path, src):
            continue

        suffix = raw_path[len(src):].lstrip("/")
        candidate = Path(dst) / suffix if suffix else Path(dst)
        if for_output or candidate.exists():
            logger.info("Rewriting path %s -> %s", raw_path, candidate)
            return str(candidate)

    return raw_path


def _normalize_config_paths(data: dict) -> dict:
    normalized = dict(data)
    for key, for_output in _PATH_FIELDS.items():
        value = normalized.get(key)
        if isinstance(value, str):
            normalized[key] = _rewrite_path(value, for_output=for_output)
    return normalized


def _arg_value(args: Any, key: str, default: Any = None) -> Any:
    if isinstance(args, dict):
        return args.get(key, default)
    return getattr(args, key, default)

@dataclass(frozen=True)
class ExperimentConfig:
    job_traces: Optional[str] = None
    config_file: Optional[str] = None
    source_config_file: Optional[str] = None
    config_json: Optional[str] = None
    raw_jobspec_file: Optional[str] = None

    resource_file: Optional[str] = None
    resource_R: Optional[str] = None

    nnodes: int = 0
    nsockets: int = 1
    ncpus: int = 1
    ngpus: int = 0

    log_level: int = 10
    log_file: Optional[str] = None
    status_file: Optional[str] = None
    summary_file: Optional[str] = None

    exclusive: bool = False
    quiet: bool = False

    backend: Optional[str] = "flux"
    batch_job_starts: bool = True
    account_system_latency: bool = True
    jobtap_logging: bool = False
    rabbit_storage_emit_dw: bool = False
    rabbit_storage_name: str = "rabbit"
    # Request nodes (and any requested accelerators/storage) without a core
    # child in the generated jobspec.  This is useful for isolating how a
    # scheduler handles node-only requests from CPU-constrained requests.
    omit_core_resources: bool = False
    # Name of a staged Fluxion build to load, resolved against
    # FLUX_FICTION_FLUXION_STAGE_ROOT at module-reload time. Set per task by the
    # ensemble launcher so one campaign can run a DIFFERENT Fluxion build per
    # queue policy. Takes precedence over the FLUX_FICTION_FLUXION_*_MODULE
    # environment variables, which are campaign-wide and therefore less
    # specific. None leaves module selection exactly as it was.
    fluxion_variant: Optional[str] = None
    # Pipeline job submissions: start every submission for a timestep before
    # collecting any job id, instead of one blocking round-trip per job. Every
    # id is collected before the scheduler is told what to expect, so the
    # scheduler sees exactly the same jobs at the same logical time.
    async_submit: bool = False
    # Skip Flux job-ingest validation for internally generated jobspecs. This
    # avoids the feasibility validator's extra Fluxion query per submitted job.
    submit_novalidate: bool = False
    # Render resource_utilization.png during post-processing. The underlying
    # CSVs (resource_usage_timeseries.csv, resource_allocations.csv) are always
    # written; this only controls the picture, which costs matplotlib time on
    # every run and is dead weight across a large campaign.
    make_plots: bool = True

    output_dir: Optional[str] = "./"

    faketime_timestamp_file: Optional[str] = None
    faketime_initial_epoch: float = 0.0
    faketime_seed: bool = True
    faketime_tolerance: float = 1e-6
    faketime_near_event_threshold: float = 0.0
    quiescent_accumulation_window: float = 0.0
    otel_enabled: bool = False
    otel_endpoint: str = "http://127.0.0.1:4318/v1/traces"
    otel_service_name: str = "flux-fiction"
    otel_bridge_socket: Optional[str] = None
    otel_summary_file: Optional[str] = None
    otel_spans_file: Optional[str] = None
    otel_bridge_log_file: Optional[str] = None

class ExperimentConfigModel(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="forbid")
    else:
        class Config:
            extra = "forbid"

    job_traces: Optional[str] = None
    config_file: Optional[str] = None
    source_config_file: Optional[str] = None
    config_json: Optional[str] = None
    raw_jobspec_file: Optional[str] = None

    resource_file: Optional[str] = None
    resource_R: Optional[str] = None

    nnodes: int = Field(default=0, ge=0)
    nsockets: int = Field(default=1, ge=1)
    ncpus: int = Field(default=1, ge=1)
    ngpus: int = Field(default=0, ge=0)

    log_level: int = Field(default=10, ge=0)
    log_file: Optional[str] = None
    status_file: Optional[str] = None
    summary_file: Optional[str] = None

    exclusive: bool = False
    quiet: bool = False

    backend: Optional[str] = "flux"
    batch_job_starts: bool = True
    account_system_latency: bool = True
    jobtap_logging: bool = False
    rabbit_storage_emit_dw: bool = False
    rabbit_storage_name: str = "rabbit"
    omit_core_resources: bool = False
    # Name of a staged Fluxion build to load, resolved against
    # FLUX_FICTION_FLUXION_STAGE_ROOT at module-reload time. Set per task by the
    # ensemble launcher so one campaign can run a DIFFERENT Fluxion build per
    # queue policy. Takes precedence over the FLUX_FICTION_FLUXION_*_MODULE
    # environment variables, which are campaign-wide and therefore less
    # specific. None leaves module selection exactly as it was.
    fluxion_variant: Optional[str] = None
    # Pipeline job submissions: start every submission for a timestep before
    # collecting any job id, instead of one blocking round-trip per job. Every
    # id is collected before the scheduler is told what to expect, so the
    # scheduler sees exactly the same jobs at the same logical time.
    async_submit: bool = False
    # Skip Flux job-ingest validation for internally generated jobspecs. This
    # avoids the feasibility validator's extra Fluxion query per submitted job.
    submit_novalidate: bool = False
    # Render resource_utilization.png during post-processing. The underlying
    # CSVs (resource_usage_timeseries.csv, resource_allocations.csv) are always
    # written; this only controls the picture, which costs matplotlib time on
    # every run and is dead weight across a large campaign.
    make_plots: bool = True

    output_dir: Optional[str] = "./"

    faketime_timestamp_file: Optional[str] = None
    faketime_initial_epoch: float = 0.0
    faketime_seed: bool = True
    faketime_tolerance: float = Field(default=1e-6, ge=0)
    faketime_near_event_threshold: float = Field(default=0.0, ge=0)
    quiescent_accumulation_window: float = Field(default=0.0, ge=0)
    otel_enabled: bool = False
    otel_endpoint: str = "http://127.0.0.1:4318/v1/traces"
    otel_service_name: str = "flux-fiction"
    otel_bridge_socket: Optional[str] = None
    otel_summary_file: Optional[str] = None
    otel_spans_file: Optional[str] = None
    otel_bridge_log_file: Optional[str] = None

    if _PYDANTIC_V2:
        @model_validator(mode="after")
        def _checks(self):
            if self.resource_file and self.resource_R:
                raise ValueError("Use only one of resource_file or resource_R.")
            if self.job_traces is None and self.config_file is None:
                raise ValueError("No job_traces input. Provide job_traces (or set it in TOML).")
            if self.backend != "flux" and self.backend != "mock":
                raise ValueError("Backend must be set to either flux or mock.")
            if self.output_dir[-1] != '/':
                self.output_dir = self.output_dir + '/'
            return self
    else:
        @root_validator(skip_on_failure=True)
        def _checks(cls, values):
            if values.get("resource_file") and values.get("resource_R"):
                raise ValueError("Use only one of resource_file or resource_R.")
            if values.get("job_traces") is None and values.get("config_file") is None:
                raise ValueError("No job_traces input. Provide job_traces (or set it in TOML).")
            if values.get("backend") not in {"flux", "mock"}:
                raise ValueError("Backend must be set to either flux or mock.")
            output_dir = values.get("output_dir")
            if output_dir and output_dir[-1] != '/':
                values["output_dir"] = output_dir + '/'
            return values

def _to_dataclass(data: dict) -> ExperimentConfig:
    try:
        data = {k: v for k, v in data.items() if v is not None}
        data = _normalize_config_paths(data)
        if _PYDANTIC_V2:
            m = ExperimentConfigModel.model_validate(data)
            model_data = m.model_dump()
        else:
            m = ExperimentConfigModel.parse_obj(data)
            model_data = m.dict()
    except ValidationError as e:
        # make CLI/TOML errors readable
        raise ValueError(str(e)) from e

    return ExperimentConfig(**model_data)

def _load_toml(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"TOML config file not found: {path}")

    # Python 3.11+: tomllib. Older: tomli.
    try:
        import tomllib  # type: ignore
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ModuleNotFoundError:
        import tomli  # type: ignore
        with open(path, "rb") as f:
            return tomli.load(f)
        
# TODO write a toml parser for configuration
def from_toml(args: dict) -> ExperimentConfig:
    '''
    from_toml
    ----------
    
    :param args: Description
    :type args: dict
    :return: Description
    :rtype: ExperimentConfig

    Build ExperimentConfig from a TOML file using Pydantic validation.

    Supports either:
      - Top-level keys, or
      - A [flux_fiction] table.
    '''
    cfg_path = _rewrite_path(_arg_value(args, "config_file"))
    if not cfg_path:
        raise ValueError("from_toml called but args.config_file is missing")
    
    raw = _load_toml(cfg_path)
    
    data = raw.get("flux_fiction", raw)
    if not isinstance(data, dict):
        raise ValueError("TOML config must be a table (dict-like)")
    
        # Normalize + inject config_file path
    merged: dict[str, Any] = dict(data)
    merged["config_file"] = cfg_path

    # Optional override: CLI job_traces wins if provided
    cli_job_traces = _arg_value(args, "job_traces")
    if cli_job_traces is not None:
        merged["job_traces"] = cli_job_traces

    cli_overrides = [
        "faketime_timestamp_file",
        "faketime_initial_epoch",
        "faketime_seed",
        "faketime_tolerance",
        "faketime_near_event_threshold",
        "quiescent_accumulation_window",
        "account_system_latency",
        "jobtap_logging",
        "submit_novalidate",
        "otel_enabled",
        "otel_endpoint",
        "otel_service_name",
        "otel_bridge_socket",
        "otel_summary_file",
        "otel_spans_file",
        "otel_bridge_log_file",
    ]
    for key in cli_overrides:
        value = _arg_value(args, key)
        if value is not None:
            merged[key] = value

    merged = _normalize_config_paths(merged)

    try:
        if _PYDANTIC_V2:
            model = ExperimentConfigModel.model_validate(merged)
            model_data = model.model_dump()
        else:
            model = ExperimentConfigModel.parse_obj(merged)
            model_data = model.dict()
    except ValidationError as e:
        raise ValueError(f"Invalid TOML config in {cfg_path}:\n{e}") from e

    return ExperimentConfig(**model_data)
    

def from_cli_args(args) -> ExperimentConfig:
    '''
    from_cli_args
    --------------
    
    :param args: parsed command line arguments given to Flux Fiction
    :return: Returns an ExperimentConfig object containing the full configuration being used for the Flux Fiction run
    :rtype: ExperimentConfig

    This function is used as a loader to take a set of command line arguments and turn it into a configuration for a run of Flux Fiction
    '''
    data = vars(args).copy()  

    if data.get("config_file"):
        return from_toml(args)

    return _to_dataclass(data)

def setup_logging(*, level: int, log_file: Optional[str] = None, quiet: bool = False) -> None:
    """
    Configure logging once for the whole application.
    All modules should only do `logger = logging.getLogger(__name__)`.
    """
    root = logging.getLogger()  
    root.setLevel(level)

    # Remove existing handlers to prevent duplicated logs in reruns/tests
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (stderr)
    if quiet == False:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)

    # Optional file handler
    if log_file:
        fh = logging.FileHandler(log_file, mode="w")
        fh.setFormatter(fmt)
        root.addHandler(fh)
