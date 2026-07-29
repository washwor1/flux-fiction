from __future__ import annotations

# dftracer imports for simulated-time tracing
try:
    from dftracer.python import dftracer, dft_fn as DFTracerFn
    _dft = DFTracerFn("simulation")
    _dft_available = True
except ImportError:
    _dft = None
    _dft_available = False

from dataclasses import dataclass
import ctypes
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_SHARED_CLOCK_ENV = "FAKETIME_SHARED_CLOCK"


class _CtypesSharedClock:
    """Thin binding to the API exported by an LD_PRELOADed libfaketime."""

    def __init__(self) -> None:
        process = ctypes.CDLL(None, use_errno=True)
        try:
            available = process.faketime_shared_clock_available
            set_realtime = process.faketime_set_realtime_ns
        except AttributeError as exc:
            raise RuntimeError(
                "shared libfaketime clock requested, but the preloaded library "
                "does not export faketime_shared_clock_available and "
                "faketime_set_realtime_ns"
            ) from exc

        available.argtypes = []
        available.restype = ctypes.c_int
        set_realtime.argtypes = [ctypes.c_int64]
        set_realtime.restype = ctypes.c_int
        if available() != 1:
            error = ctypes.get_errno()
            detail = os.strerror(error) if error else "shared state is inactive"
            raise RuntimeError(
                f"shared libfaketime clock requested, but unavailable: {detail}"
            )
        self._set_realtime = set_realtime

    def set_realtime_ns(self, target_ns: int) -> None:
        if self._set_realtime(int(target_ns)) != 0:
            error = ctypes.get_errno()
            detail = os.strerror(error) if error else "unknown libfaketime error"
            raise RuntimeError(f"could not update shared libfaketime clock: {detail}")


def _load_shared_clock() -> _CtypesSharedClock:
    # LD_PRELOAD libraries are in the process-global symbol scope, so loading
    # the main program avoids a second dlopen and guarantees this binding uses
    # the same mapped state as intercepted time calls.
    return _CtypesSharedClock()


def _clean_real_time() -> float:
    """
    Return real wall time even when this Python process is under libfaketime.

    libfaketime interposes normal Python clock calls in the current process. A
    tiny child process with faketime-related environment removed gives us the
    real wall clock needed to compute a relative FAKETIME_TIMESTAMP_FILE offset.
    """
    env = os.environ.copy()
    for key in list(env):
        if key == "LD_PRELOAD" or key.startswith("FAKETIME"):
            env.pop(key, None)

    try:
        out = subprocess.check_output(
            [sys.executable, "-c", "import time; print(repr(time.time()))"],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return float(out.strip())
    except Exception as e:
        raise RuntimeError("could not determine real wall time outside libfaketime") from e


def _format_relative_offset(offset: float) -> str:
    return f"{float(offset):+.9f}s\n"


def _parse_relative_offset(text: str) -> float:
    value = text.strip()
    if value.endswith(("s", "S")):
        value = value[:-1]
    if not value or value[0] not in "+-":
        raise ValueError("expected a relative faketime offset such as '+300s'")
    return float(value)


def _atomic_write_text(path: Path, text: str) -> None:
    """
    Replace the stamp file atomically via write-to-temp plus rename.

    The rename matters: libfaketime reopens the file by path on every clock call,
    so an in-place rewrite would let readers observe a torn offset. Durability
    does not, since the file lives on tmpfs and is reseeded at the start of every
    run -- so this deliberately does not fsync. The clock advances once per
    simulated event, and an fsync per advance is pure overhead.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            logger.warning("Could not remove stale faketime temp file %s", tmp_path)


@dataclass
class FakeTimeDecision:
    action: str
    target: float
    effective: float
    offset: float


class FakeTimeController:
    """
    Control libfaketime through its shared clock or a timestamp-file offset.

    ``initial_epoch`` is the fake wall-clock timestamp corresponding to
    simulation time zero, so target fake wall time is:

        initial_epoch + simulation_time

    After optional startup seeding, each ``advance_to`` call moves the fake wall
    clock forward to the requested simulation time. If scheduler or startup work
    has already allowed fake time to drift past that target, the controller
    leaves time alone; Flux rejects job submissions after a backward clock step.
    """

    def __init__(
        self,
        timestamp_file: str | os.PathLike[str],
        *,
        initial_epoch: float = 0.0,
        tolerance: float = 1e-6,
        near_event_threshold: float = 1.0,
        real_time: Optional[Callable[[], float]] = None,
        fake_time: Optional[Callable[[], float]] = None,
        seed: bool = True,
        shared_clock: Optional[bool] = None,
        shared_clock_setter: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.timestamp_file = Path(timestamp_file)
        self.initial_epoch = float(initial_epoch or 0.0)
        self.tolerance = float(tolerance)
        self.near_event_threshold = float(near_event_threshold)
        self._real_time = real_time or _clean_real_time
        self._fake_time = fake_time or time.time
        self._offset: Optional[float] = None
        self.shared_clock = (
            os.environ.get(_SHARED_CLOCK_ENV) == "1"
            if shared_clock is None
            else bool(shared_clock)
        )
        self._set_shared_realtime: Optional[Callable[[int], None]] = None

        if self.shared_clock:
            if shared_clock_setter is not None:
                self._set_shared_realtime = shared_clock_setter
            else:
                self._set_shared_realtime = _load_shared_clock().set_realtime_ns

        if seed:
            self.seed(0.0)
        elif self.shared_clock:
            # The launcher initializes the shared page before Flux starts. An
            # exec'd controller attaches to it and must not reseed it.
            pass
        elif self.timestamp_file.exists():
            self._offset = self._read_offset()
        else:
            raise FileNotFoundError(
                f"faketime timestamp file does not exist and startup seeding is disabled: "
                f"{self.timestamp_file}"
            )

    def seed(self, simulation_time: float) -> FakeTimeDecision:
        # Log faketime seed as simulated-time event
        if _dft_available:
            try:
                target = self.target_time(simulation_time)
                dftracer.get_instance().log_event(
                    name="faketime_seed",
                    cat="simulation",
                    start_time=int(target * 1e9),
                    duration=0,
                    int_args={"target_sim_time_s": (0, int(simulation_time))},
                )
            except Exception as e:
                logger.debug("Failed to log faketime_seed event: %s", e)
        target = self.target_time(simulation_time)
        if self.shared_clock:
            assert self._set_shared_realtime is not None
            self._set_shared_realtime(self.target_time_ns(simulation_time))
            logger.info("Seeded shared faketime clock to target=%0.9f", target)
            return FakeTimeDecision("seeded", target, target, 0.0)

        now = self._real_time()
        offset = target - now
        self._write_offset(offset)
        logger.info(
            "Seeded faketime timestamp file %s to target=%0.6f offset=%+0.6fs",
            self.timestamp_file,
            target,
            offset,
        )
        return FakeTimeDecision("seeded", target, target, offset)

    def target_time(self, simulation_time: float) -> float:
        return self.initial_epoch + float(simulation_time)

    def target_time_ns(self, simulation_time: float) -> int:
        """Return the absolute fake epoch as integer nanoseconds."""
        return int(round(self.target_time(simulation_time) * 1_000_000_000))

    def current_effective_time(self, *, fake_now: Optional[float] = None) -> float:
        return self._fake_time() if fake_now is None else float(fake_now)

    def advance_to(self, simulation_time: float) -> FakeTimeDecision:
        # Log faketime advance as simulated-time event
        if _dft_available:
            try:
                target = self.target_time(simulation_time)
                current = self.current_effective_time()
                advance_amount_ns = max(0, int((target - current) * 1e9))
                start_time_ns = int(current * 1e9)
                dftracer.get_instance().log_event(
                    name="faketime_advance",
                    cat="simulation",
                    start_time=start_time_ns,
                    duration=advance_amount_ns,
                    int_args={"target_sim_time_s": (0, int(simulation_time))},
                )
            except Exception as e:
                logger.debug("Failed to log faketime_advance event: %s", e)
        target = self.target_time(simulation_time)
        effective = self.current_effective_time()

        remaining = target - effective
        if abs(remaining) <= self.tolerance:
            logger.info(
                "Faketime already at target %0.6f; effective fake time is %0.6f",
                target,
                effective,
            )
            return FakeTimeDecision("already", target, effective, self._offset or 0.0)

        if remaining < 0:
            logger.info(
                "Faketime is %0.6fs past target %0.6f; leaving clock monotonic at %0.6f",
                abs(remaining),
                target,
                effective,
            )
            return FakeTimeDecision("overrun", target, effective, self._offset or 0.0)

        if remaining > 0 and remaining <= self.near_event_threshold:
            logger.info(
                "Waiting %0.6fs for faketime to naturally reach %0.6f",
                remaining,
                target,
            )
            while True:
                effective = self.current_effective_time()
                if effective + self.tolerance >= target:
                    return FakeTimeDecision(
                        "waited",
                        target,
                        effective,
                        self._offset or 0.0,
                    )
                time.sleep(min(0.01, max(0.0, target - effective)))

        if self.shared_clock:
            assert self._set_shared_realtime is not None
            self._set_shared_realtime(self.target_time_ns(simulation_time))
            logger.info(
                "Pinned shared faketime clock to %0.9f (effective was %0.9f)",
                target,
                effective,
            )
            return FakeTimeDecision("jumped", target, target, 0.0)

        if self._offset is None:
            self._offset = self._read_offset()
        offset = self._offset + remaining
        self._write_offset(offset)
        logger.info(
            "Pinned faketime to %0.6f via offset=%+0.6fs (effective was %0.6f)",
            target,
            offset,
            effective,
        )
        return FakeTimeDecision("jumped", target, target, offset)

    def _read_offset(self) -> float:
        try:
            text = self.timestamp_file.read_text()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"faketime timestamp file not found: {self.timestamp_file}") from e
        try:
            return _parse_relative_offset(text)
        except ValueError as e:
            raise ValueError(
                f"faketime timestamp file must contain a relative offset; "
                f"got {text.strip()!r} from {self.timestamp_file}"
            ) from e

    def _write_offset(self, offset: float) -> None:
        self._offset = float(offset)
        _atomic_write_text(self.timestamp_file, _format_relative_offset(self._offset))
