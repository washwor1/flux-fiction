from __future__ import annotations

import argparse
import atexit
from collections import defaultdict
import csv
import json
import logging
import os
from pathlib import Path
import signal
import socket
import sys
import time
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


logger = logging.getLogger(__name__)


def _sanitize_attr_value(value: Any):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_sanitize_attr_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _sanitize_attr_value(v) for k, v in value.items()}
    return str(value)


class Bridge:
    def __init__(
        self,
        socket_path: str,
        endpoint: str,
        service_name: str,
        summary_file: str | None = None,
        spans_file: str | None = None,
    ):
        self.socket_path = socket_path
        self.endpoint = endpoint
        self.service_name = service_name
        self.summary_file = summary_file
        self.spans_file = spans_file
        self._stop = False
        self._sock = None
        self._spans_file_handle = None
        self._active_spans: dict[tuple[str, str], Any] = {}
        self._summary_rows: list[dict[str, Any]] = []

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        self._provider = provider
        self._tracer_cache: dict[str, Any] = {}

    def _tracer(self, service: str):
        tracer = self._tracer_cache.get(service)
        if tracer is None:
            tracer = trace.get_tracer(service)
            self._tracer_cache[service] = tracer
        return tracer

    def _open(self):
        path = Path(self.socket_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.settimeout(0.25)
        sock.bind(str(path))
        self._sock = sock

        if self.spans_file:
            spans_path = Path(self.spans_file)
            spans_path.parent.mkdir(parents=True, exist_ok=True)
            self._spans_file_handle = spans_path.open("w", encoding="utf-8")

    def _close(self):
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None
        try:
            Path(self.socket_path).unlink(missing_ok=True)
        except Exception:
            logger.debug("Could not unlink telemetry socket", exc_info=True)
        if self._spans_file_handle is not None:
            self._spans_file_handle.close()
            self._spans_file_handle = None

    def _record_summary(self, row: dict[str, Any]):
        self._summary_rows.append(row)
        if self._spans_file_handle is not None:
            self._spans_file_handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
            self._spans_file_handle.flush()

    def _handle_start(self, msg: dict[str, Any]):
        service = str(msg.get("service") or self.service_name)
        source = str(msg.get("source") or "unknown")
        span_id = str(msg["span_id"])
        name = str(msg["name"])
        attrs = {
            str(k): _sanitize_attr_value(v)
            for k, v in (msg.get("attrs") or {}).items()
        }
        attrs.setdefault("ff.source", source)
        attrs.setdefault("ff.pid", int(msg.get("pid") or 0))
        now = time.time_ns()
        span = self._tracer(service).start_span(
            name,
            start_time=now,
            attributes=attrs,
        )
        self._active_spans[(service, span_id)] = {
            "span": span,
            "name": name,
            "source": source,
            "start_ns": now,
            "attrs": dict(attrs),
        }

    def _handle_end(self, msg: dict[str, Any]):
        service = str(msg.get("service") or self.service_name)
        span_id = str(msg["span_id"])
        key = (service, span_id)
        entry = self._active_spans.pop(key, None)
        if entry is None:
            return

        attrs = {
            str(k): _sanitize_attr_value(v)
            for k, v in (msg.get("attrs") or {}).items()
        }
        for key2, value in attrs.items():
            entry["span"].set_attribute(key2, value)
            entry["attrs"][key2] = value

        end_ns = time.time_ns()
        entry["span"].end(end_time=end_ns)
        duration_ms = (end_ns - entry["start_ns"]) / 1_000_000.0
        self._record_summary({
            "service": service,
            "source": entry["source"],
            "name": entry["name"],
            "span_id": span_id,
            "start_ns": entry["start_ns"],
            "end_ns": end_ns,
            "duration_ms": duration_ms,
            "attrs": entry["attrs"],
        })

    def _write_summary(self):
        if not self.summary_file:
            return
        path = Path(self.summary_file)
        path.parent.mkdir(parents=True, exist_ok=True)

        grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        for row in self._summary_rows:
            grouped[(row["service"], row["source"], row["name"])].append(float(row["duration_ms"]))

        with path.open("w", newline="", encoding="utf-8") as f:
            fieldnames = ["service", "source", "name", "count", "total_ms", "avg_ms", "max_ms", "min_ms"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for (service, source, name), durations in sorted(grouped.items()):
                writer.writerow({
                    "service": service,
                    "source": source,
                    "name": name,
                    "count": len(durations),
                    "total_ms": sum(durations),
                    "avg_ms": sum(durations) / len(durations),
                    "max_ms": max(durations),
                    "min_ms": min(durations),
                })

    def stop(self, *_args):
        self._stop = True

    def run(self):
        self._open()
        atexit.register(self._write_summary)
        atexit.register(self._close)

        while not self._stop:
            assert self._sock is not None
            try:
                data = self._sock.recv(65535)
            except socket.timeout:
                continue
            except OSError:
                if self._stop:
                    break
                raise

            msg = json.loads(data.decode("utf-8"))
            kind = msg.get("kind")
            if kind == "span_start":
                self._handle_start(msg)
            elif kind == "span_end":
                self._handle_end(msg)

        for key in list(self._active_spans):
            entry = self._active_spans.pop(key)
            end_ns = time.time_ns()
            entry["span"].end(end_time=end_ns)
            self._record_summary({
                "service": key[0],
                "source": entry["source"],
                "name": entry["name"],
                "span_id": key[1],
                "start_ns": entry["start_ns"],
                "end_ns": end_ns,
                "duration_ms": (end_ns - entry["start_ns"]) / 1_000_000.0,
                "attrs": entry["attrs"],
            })

        self._write_summary()
        try:
            self._provider.shutdown()
        except Exception:
            logger.exception("Telemetry provider shutdown failed")
        self._close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Flux Fiction OTel bridge")
    parser.add_argument("--socket", required=True, help="Unix datagram socket path")
    parser.add_argument("--endpoint", default="http://127.0.0.1:4318/v1/traces", help="OTLP HTTP traces endpoint")
    parser.add_argument("--service-name", default="flux-fiction-bridge", help="Bridge service name")
    parser.add_argument("--summary-file", default=None, help="Optional CSV summary output")
    parser.add_argument("--spans-file", default=None, help="Optional JSONL span dump output")
    parser.add_argument("--log-file", default=None, help="Optional bridge log file")
    args = parser.parse_args(argv)

    handlers = []
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, mode="w"))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )

    bridge = Bridge(
        socket_path=args.socket,
        endpoint=args.endpoint,
        service_name=args.service_name,
        summary_file=args.summary_file,
        spans_file=args.spans_file,
    )
    signal.signal(signal.SIGTERM, bridge.stop)
    signal.signal(signal.SIGINT, bridge.stop)
    bridge.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
