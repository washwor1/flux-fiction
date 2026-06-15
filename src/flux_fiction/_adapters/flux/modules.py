import logging
logger = logging.getLogger(__name__)

def get_loaded_modules(flux_handle):
    """
    Retrieve the list of loaded modules in the current Flux instance.
    """
    try:
        modules = flux_handle.rpc("module.list").get()["mods"]
        return modules
    except Exception as e:
        raise RuntimeError(f"Error retrieving loaded modules: {e}")


def load_missing_modules(flux_handle):
    # TODO: check that necessary modules are loaded
    # if not, load them
    # return an updated list of loaded modules
    # Should be checking for the jobtap module
    loaded_modules = get_loaded_modules(flux_handle)
    pass


def start_all_queues(flux_handle):
    """
    Ensure job-manager queues are accepting scheduling work.

    Reloading scheduler modules can leave job-manager queues stopped. Jobs can
    still be submitted in that state, but they remain in SCHED without alloc
    events because no allocation requests are sent to the scheduler.
    """
    payload = {"start": True, "all": True, "nocheckpoint": False}
    try:
        flux_handle.rpc(
            "job-manager.queue-start",
            payload=payload,
            nodeid=0,
        ).get()
        logger.debug("Started all job-manager queues after scheduler reload")
    except Exception as e:
        raise RuntimeError(f"Could not start job-manager queues: {e}") from e


def queue_status(flux_handle):
    try:
        queues = flux_handle.rpc("job-manager.queue-list").get()
    except Exception as e:
        return {"queue_list_error": repr(e)}

    statuses = {"queue_list": queues}
    names = queues.get("queues", []) if isinstance(queues, dict) else []
    if names:
        per_queue = {}
        for name in names:
            try:
                per_queue[name] = flux_handle.rpc(
                    "job-manager.queue-status",
                    payload={"name": name},
                    nodeid=0,
                ).get()
            except Exception as e:
                per_queue[name] = {"error": repr(e)}
        statuses["queue_status"] = per_queue
    else:
        try:
            statuses["queue_status"] = flux_handle.rpc(
                "job-manager.queue-status",
                payload={},
                nodeid=0,
            ).get()
        except Exception as e:
            statuses["queue_status_error"] = repr(e)
    return statuses

def reset_jobtap_plugin(
    flux_handle,
    *,
    keep_timestep=False,
    batch_job_starts=True,
    log_enabled=False,
    otel_enabled=False,
    otel_socket=None,
    otel_service_name="flux-fiction-jobtap",
):
    try:
        flux_handle.rpc(
            "job-manager.emu-jobtap.reset",
            payload={
                "keep_timestep": keep_timestep,
                "batch_job_starts": batch_job_starts,
                "log_enabled": log_enabled,
                "otel_enabled": otel_enabled,
                "otel_socket": otel_socket,
                "otel_service_name": otel_service_name,
            },
        ).get()
        logger.debug(
            "Reset emu-jobtap probe to defaults (batch_job_starts=%s, log_enabled=%s, otel_enabled=%s)",
            batch_job_starts,
            log_enabled,
            otel_enabled,
        )
    except Exception as e:
        logger.error(f"Failed to reset emu-jobtap probe: {e}")

import copy
import json
import logging
from pathlib import Path




def _load_config_object(config_source):
    """
    Accept either:
      - a Python dict already produced from `flux config get`
      - a path to a .json file containing that object

    Returns a deep-copied dict so we can mutate it safely.
    """
    if config_source is None:
        return {}

    if isinstance(config_source, dict):
        return copy.deepcopy(config_source)

    if isinstance(config_source, (str, Path)):
        path = Path(config_source)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    raise TypeError(
        f"config_source must be dict, str, Path, or None; got {type(config_source)!r}"
    )


def _extract_runtime_config(config_obj):
    """
    Return the full config object that should be reloaded into Flux.

    Flux Fiction may still inject ingest validator defaults, but all caller-
    supplied config tables should be preserved when reloading modules.
    """
    return copy.deepcopy(config_obj) if config_obj else {}


def _with_default_ingest_validator_plugins(config_obj):
    cfg = copy.deepcopy(config_obj) if config_obj else {}
    ingest = cfg.setdefault("ingest", {})
    validator = ingest.setdefault("validator", {})

    plugins = validator.get("plugins")
    if isinstance(plugins, list):
        merged = ["feasibility", "jobspec"]
        for plugin in plugins:
            if isinstance(plugin, str) and plugin not in merged:
                merged.append(plugin)
        validator["plugins"] = merged
    else:
        validator["plugins"] = ["feasibility", "jobspec"]

    return cfg


def _reload_job_ingest_config(flux_handle, config_payload, job_ingest_loaded):
    if not job_ingest_loaded:
        logger.debug("job-ingest not loaded; skipping explicit config reload")
        return

    flux_handle.rpc("job-ingest.config-reload", payload=config_payload).get()


def reload_modules(flux_handle, config_source=None):
    """
    Reload resource + scheduler modules in the order:

      Sched Unload -> Res Unload -> config.load(updated Flux config)
      -> Res Load(with raw args) -> Sched Load

    config_source may be:
      - dict from `flux config get`
      - path to a JSON file containing that object
      - None (load a minimal config enabling ingest feasibility validation)
    """
    sched_module = "sched-simple"
    sched_simple_path = None
    resource_module_path = None
    fluxion_qmanager_path = None
    fluxion_resource_path = None
    feasibility_module_path = None
    job_ingest_loaded = False

    # config_source = "/home/j/Desktop/flux/sc25_poster/flux-fiction/experiment_data/ff_traces/experiment_scheduler_easy_resdepth32_20260330_165549/flux_config.json"

    for module in get_loaded_modules(flux_handle):
        logger.debug("loaded module: %s", module)

        services = module.get("services", [])
        name = module.get("name", "")

        if "sched-simple" in services:
            sched_module = module["name"]
            sched_simple_path = module["path"]
        elif "sched-fluxion-qmanager" in name:
            sched_module = "fluxion"
            fluxion_qmanager_path = module["path"]
        elif "sched-fluxion-resource" in name:
            fluxion_resource_path = module["path"]
        elif name == "job-ingest":
            job_ingest_loaded = True
        elif name == "resource" or "resource" in name:
            resource_module_path = module["path"]
        elif "feasibility" in name:
            feasibility_module_path = module["path"]

    logger.debug("Reloading '%s' and 'resource' modules", sched_module)

    if resource_module_path is None:
        raise RuntimeError(
            "Unable to get resource module path (is the resource module loaded?)"
        )

    # 1. Unload scheduler + resource
    try:
        if sched_module == "sched-simple":
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-simple"},
            ).get()
        else:
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-fluxion-qmanager"},
            ).get()
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-fluxion-feasibility"},
            ).get()
            flux_handle.rpc(
                "module.remove",
                payload={"name": "sched-fluxion-resource"},
            ).get()

        flux_handle.rpc(
            "module.remove",
            payload={"name": "resource"},
        ).get()

    except Exception as e:
        logger.error("Error removing modules: %s", e)
        raise

    # 2. Reload the caller-supplied Flux config while keeping ingest
    # feasibility validation enabled after the module restart.
    try:
        config_payload = _with_default_ingest_validator_plugins(
            _extract_runtime_config(_load_config_object(config_source))
        )
        logger.debug(
            "Loading Flux config via config.load:\n%s",
            json.dumps(config_payload, indent=2, sort_keys=True),
        )
        flux_handle.rpc("config.load", payload=config_payload).get()
    except Exception as e:
        logger.error("Error loading Flux config: %s", e)
        raise

    # 3. Reload resource + scheduler
    try: 
        flux_handle.rpc("module.load", payload={"path": resource_module_path,
                                                "args": [ "noverify", "monitor-force-up"]}).get()

        flux_handle.rpc("module.load", payload={"path": fluxion_resource_path,
                                                "args": []}).get()

        flux_handle.rpc("module.load", payload={"path": feasibility_module_path,
                                                "args": []}).get()

        flux_handle.rpc("module.load", payload={"path": fluxion_qmanager_path,
                                                "args": []}).get()

        _reload_job_ingest_config(flux_handle, config_payload, job_ingest_loaded)

        start_all_queues(flux_handle)

    except Exception as e:
        logger.error("Error loading modules: %s", e)
        raise
