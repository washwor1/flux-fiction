from __future__ import annotations

import argparse
import logging
import os

from flux_fiction.api import client
from flux_fiction.api.config import from_toml
from flux_fiction.api.status import RunStatusWriter

logger = logging.getLogger(__name__)

# All manual log_event() calls in _core/engine.py/_core/faketime.py pass
# simulated-clock start_time/duration converted to NANOSECONDS (* 1e9). This
# only reads correctly if dftracer itself is configured to interpret raw
# timestamps as nanoseconds -- DFTRACER_TIME_METRIC (available on develop,
# dftracer/core/common/constants.h) controls this;
# its default is microseconds ("US"), which would silently misinterpret our
# nanosecond values as 1000x too large. Set it BEFORE initialize_log() so the
# C core picks it up at init time. Respect an existing external override
# rather than clobbering it (setdefault).
os.environ.setdefault("DFTRACER_TIME_METRIC", "NS")

# Per-event int/string/float args (our job_id, mhost, trace_idx tags) are only
# written to the trace when DFTRACER_INC_METADATA=1; without it dftracer emits
# the event but silently drops the args object. Must be set BEFORE
# initialize_log() so the C core enables metadata capture at init time.
os.environ.setdefault("DFTRACER_INC_METADATA", "1")

# dftracer FUNCTION-mode requires an explicit process-level initialize_log()
# call before any dftracer.get_instance().log_event(...) call will actually
# write anything (get_instance().logger stays None, and log_event silently
# no-ops, until this runs). Do it once here at the process entry point so
# both the manual log_event() calls in _core/engine.py/_core/faketime.py and
# any auto-decorated @_dft.log calls elsewhere in the tree produce a real
# trace. Controlled by the standard DFTRACER_ENABLE/DFTRACER_LOG_FILE/
# DFTRACER_DATA_DIR environment variables supplied by the launcher.
try:
    from dftracer.python import dftracer as _dftracer

    if os.environ.get("DFTRACER_ENABLE") == "1":
        _dftracer.initialize_log(
            logfile=os.environ.get("DFTRACER_LOG_FILE"),
            data_dir=os.environ.get("DFTRACER_DATA_DIR"),
        )
except ImportError:
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one Flux Fiction experiment through the programmatic API."
    )
    parser.add_argument("--config-file", required=True, help="Generated Flux Fiction TOML config file.")
    parser.add_argument(
        "--faketime_timestamp_file",
        "--faketime-timestamp-file",
        dest="faketime_timestamp_file",
        default=None,
        help="libfaketime timestamp file path.",
    )
    parser.add_argument(
        "--faketime_seed",
        "--faketime-seed",
        dest="faketime_seed",
        action="store_true",
        default=None,
        help="Seed the libfaketime timestamp file before the run.",
    )
    parser.add_argument(
        "--faketime_no_seed",
        "--faketime-no-seed",
        dest="faketime_seed",
        action="store_false",
        help="Do not seed the libfaketime timestamp file before the run.",
    )
    parser.add_argument(
        "--faketime_tolerance",
        "--faketime-tolerance",
        dest="faketime_tolerance",
        type=float,
        default=None,
        help="Faketime tolerance override.",
    )
    parser.add_argument(
        "--faketime_near_event_threshold",
        "--faketime-near-event-threshold",
        dest="faketime_near_event_threshold",
        type=float,
        default=None,
        help="Faketime near-event threshold override.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = from_toml(args)
    status = RunStatusWriter(cfg.status_file)
    status.update(
        state="running",
        config_file=cfg.config_file,
        source_config_file=cfg.source_config_file,
        trace_file=cfg.job_traces,
    )

    # App-parameter metadata events: attach this run's config as key/value
    # context on the trace so it can be filtered/correlated after the fact.
    # Inserted right after initialize_log() has run (module import time) and
    # cfg is available (here), since metadata needs a live logger. Failures
    # here must never break the actual simulation run.
    if os.environ.get("DFTRACER_ENABLE") == "1":
        try:
            from dftracer.python import dftracer as _dftracer

            _dft_log = _dftracer.get_instance()
            _dft_log.log_metadata_event("app", "flux-fiction")
            _dft_log.log_metadata_event("backend", str(getattr(cfg, "backend", "flux")))
            _dft_log.log_metadata_event("nnodes", str(getattr(cfg, "nnodes", "")))
            _dft_log.log_metadata_event("ncpus", str(getattr(cfg, "ncpus", "")))
            _dft_log.log_metadata_event("ngpus", str(getattr(cfg, "ngpus", "")))
            _dft_log.log_metadata_event("job_traces", str(getattr(cfg, "job_traces", "")))
            _dft_log.log_metadata_event("config_file", str(cfg.config_file))
        except Exception:
            logger.debug("Failed to log app metadata events", exc_info=True)

    try:
        result = client.run_experiment(cfg, status=status)
    finally:
        # dftracer FUNCTION-mode only flushes its trace file on an explicit
        # finalize() call (or a caught SIGABRT/SIGINT/SIGTERM) -- a normal
        # process exit does NOT auto-flush. Always finalize here, even on
        # exception, so a real trace is written for whatever ran.
        if os.environ.get("DFTRACER_ENABLE") == "1":
            try:
                from dftracer.python import dftracer as _dftracer

                _dftracer.get_instance().finalize()
            except ImportError:
                pass

    if not result.ok:
        logger.critical("Run failed: %s", result.message)
        return 1

    logger.info(result.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
