from __future__ import annotations

import json
from pathlib import Path
import socket
from types import SimpleNamespace

from flux_fiction.telemetry import bridge as bridge_module
from flux_fiction.telemetry import client as client_module
from flux_fiction.telemetry import telemetry as telemetry_module


class _FakeSocket:
    def __init__(self):
        self.blocking = None
        self.timeout = None
        self.sent = []
        self.closed = False

    def setblocking(self, value):
        self.blocking = value

    def settimeout(self, value):
        self.timeout = value

    def sendto(self, data, path):
        self.sent.append((data, path))

    def close(self):
        self.closed = True


class _FakeSpan:
    def __init__(self, name, start_time, attributes):
        self.name = name
        self.start_time = start_time
        self.attributes = dict(attributes)
        self.end_time = None

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def end(self, end_time=None):
        self.end_time = end_time


class _FakeTracer:
    def __init__(self):
        self.started = []

    def start_span(self, name, start_time=None, attributes=None):
        span = _FakeSpan(name, start_time, attributes or {})
        self.started.append(span)
        return span


class _FakeProvider:
    def __init__(self, resource):
        self.resource = resource
        self.processors = []
        self.shutdown_called = False

    def add_span_processor(self, processor):
        self.processors.append(processor)

    def shutdown(self):
        self.shutdown_called = True


def test_telemetry_client_sends_sanitized_payloads(monkeypatch):
    fake_socket = _FakeSocket()
    monkeypatch.setattr(client_module.socket, "socket", lambda *args: fake_socket)
    monkeypatch.setattr(client_module.os, "getpid", lambda: 4242)

    client = client_module.TelemetryClient("/tmp/ff.sock", "svc", "engine")
    span_id = client.start_span(
        "advance",
        number=3,
        items=[1, {"nested": object()}],
        skip=None,
    )
    client.end_span(span_id, status={"ok": True})
    client.close()

    assert fake_socket.blocking is False
    assert fake_socket.closed is True
    assert len(fake_socket.sent) == 2
    start_payload = json.loads(fake_socket.sent[0][0].decode("utf-8"))
    end_payload = json.loads(fake_socket.sent[1][0].decode("utf-8"))
    assert start_payload["service"] == "svc"
    assert start_payload["source"] == "engine"
    assert start_payload["pid"] == 4242
    assert start_payload["attrs"]["items"][1]["nested"] == str(start_payload["attrs"]["items"][1]["nested"])
    assert end_payload["attrs"]["status"] == {"ok": True}


def test_telemetry_client_context_manager_and_disabled_mode(monkeypatch):
    fake_socket = _FakeSocket()
    monkeypatch.setattr(client_module.socket, "socket", lambda *args: fake_socket)
    monkeypatch.setattr(client_module.os, "getpid", lambda: 99)

    disabled = client_module.TelemetryClient("", "svc", "source")
    assert disabled.enabled is False
    assert disabled.start_span("noop") is None

    client = client_module.TelemetryClient("/tmp/bridge.sock", "svc", "source")
    with client.span("work", payload={"a": 1}) as span_id:
        assert span_id is not None

    assert len(fake_socket.sent) == 2


def test_telemetry_get_tracer_builds_provider(monkeypatch):
    created = {}

    monkeypatch.setattr(
        telemetry_module.Resource,
        "create",
        staticmethod(lambda attrs: {"resource": attrs}),
    )
    monkeypatch.setattr(
        telemetry_module,
        "TracerProvider",
        lambda resource: _FakeProvider(resource),
    )
    monkeypatch.setattr(
        telemetry_module,
        "BatchSpanProcessor",
        lambda exporter: ("processor", exporter),
    )
    monkeypatch.setattr(
        telemetry_module,
        "OTLPSpanExporter",
        lambda endpoint: {"endpoint": endpoint},
    )
    monkeypatch.setattr(
        telemetry_module.trace,
        "set_tracer_provider",
        lambda provider: created.setdefault("provider", provider),
    )
    monkeypatch.setattr(
        telemetry_module.trace,
        "get_tracer",
        lambda service: {"service": service},
    )

    tracer = telemetry_module.get_tracer("ff-test")

    assert tracer == {"service": "ff-test"}
    assert created["provider"].resource == {"resource": {"service.name": "ff-test"}}
    assert created["provider"].processors == [
        ("processor", {"endpoint": "http://127.0.0.1:4318/v1/traces"})
    ]


def _patch_bridge_dependencies(monkeypatch):
    tracers = {}

    monkeypatch.setattr(
        bridge_module.Resource,
        "create",
        staticmethod(lambda attrs: {"resource": attrs}),
    )
    monkeypatch.setattr(
        bridge_module,
        "TracerProvider",
        lambda resource: _FakeProvider(resource),
    )
    monkeypatch.setattr(
        bridge_module,
        "BatchSpanProcessor",
        lambda exporter: ("processor", exporter),
    )
    monkeypatch.setattr(
        bridge_module,
        "OTLPSpanExporter",
        lambda endpoint: {"endpoint": endpoint},
    )
    monkeypatch.setattr(bridge_module.trace, "set_tracer_provider", lambda provider: None)
    monkeypatch.setattr(
        bridge_module.trace,
        "get_tracer",
        lambda service: tracers.setdefault(service, _FakeTracer()),
    )
    return tracers


def test_bridge_open_close_and_summary_files(tmp_path: Path, monkeypatch):
    _patch_bridge_dependencies(monkeypatch)
    socket_path = tmp_path / "bridge.sock"
    spans_file = tmp_path / "spans.jsonl"
    summary_file = tmp_path / "summary.csv"

    bridge = bridge_module.Bridge(
        socket_path=str(socket_path),
        endpoint="http://collector",
        service_name="bridge-svc",
        summary_file=str(summary_file),
        spans_file=str(spans_file),
    )
    bridge._open()
    assert socket_path.exists()
    assert bridge._sock is not None
    assert bridge._sock.gettimeout() == 0.25
    assert bridge._spans_file_handle is not None
    bridge._record_summary(
        {
            "service": "svc",
            "source": "engine",
            "name": "advance",
            "span_id": "abc",
            "start_ns": 100,
            "end_ns": 1100,
            "duration_ms": 2.5,
            "attrs": {"ok": True},
        }
    )
    bridge._write_summary()
    bridge._close()

    assert socket_path.exists() is False
    assert "advance" in spans_file.read_text(encoding="utf-8")
    assert "service,source,name,count,total_ms,avg_ms,max_ms,min_ms" in summary_file.read_text(encoding="utf-8")


def test_bridge_handle_start_end_and_run_cleanup(tmp_path: Path, monkeypatch):
    tracers = _patch_bridge_dependencies(monkeypatch)
    times = iter([1_000_000, 4_000_000, 10_000_000, 12_000_000])
    monkeypatch.setattr(bridge_module.time, "time_ns", lambda: next(times))
    monkeypatch.setattr(bridge_module.atexit, "register", lambda fn: fn)

    bridge = bridge_module.Bridge(
        socket_path=str(tmp_path / "bridge.sock"),
        endpoint="http://collector",
        service_name="bridge-svc",
        summary_file=str(tmp_path / "summary.csv"),
        spans_file=None,
    )

    bridge._handle_start(
        {
            "service": "engine-svc",
            "source": "engine",
            "pid": 7,
            "span_id": "span-1",
            "name": "advance",
            "attrs": {"jobid": 5, "extra": object()},
        }
    )
    bridge._handle_end(
        {
            "service": "engine-svc",
            "span_id": "span-1",
            "attrs": {"status": "ok"},
        }
    )

    span = tracers["engine-svc"].started[0]
    assert span.attributes["ff.source"] == "engine"
    assert span.attributes["ff.pid"] == 7
    assert span.attributes["status"] == "ok"
    assert span.end_time == 4_000_000
    assert bridge._summary_rows[0]["duration_ms"] == 3.0

    start_msg = json.dumps({
        "kind": "span_start",
        "service": "engine-svc",
        "source": "engine",
        "pid": 9,
        "span_id": "span-2",
        "name": "linger",
        "attrs": {"step": 1},
    }).encode("utf-8")
    stop_msg = json.dumps({"kind": "noop"}).encode("utf-8")

    class _RecvSocket:
        def __init__(self):
            self.calls = 0
            self.closed = False
            self.timeout = None

        def settimeout(self, value):
            self.timeout = value

        def recv(self, _size):
            self.calls += 1
            if self.calls == 1:
                return start_msg
            if self.calls == 2:
                raise socket.timeout()
            bridge.stop()
            return stop_msg

        def close(self):
            self.closed = True

    recv_socket = _RecvSocket()
    monkeypatch.setattr(bridge, "_open", lambda: setattr(bridge, "_sock", recv_socket))
    monkeypatch.setattr(bridge, "_close", lambda: setattr(recv_socket, "closed", True))

    bridge.run()

    assert bridge._provider.shutdown_called is True
    assert recv_socket.closed is True
    assert any(row["name"] == "linger" for row in bridge._summary_rows)


def test_bridge_writes_summary_before_shutdown_failure(tmp_path: Path, monkeypatch):
    _patch_bridge_dependencies(monkeypatch)
    monkeypatch.setattr(bridge_module.time, "time_ns", lambda: 1_000_000)
    monkeypatch.setattr(bridge_module.atexit, "register", lambda fn: fn)

    class _ExplodingProvider(_FakeProvider):
        def shutdown(self):
            self.shutdown_called = True
            raise RuntimeError("exporter hung")

    monkeypatch.setattr(
        bridge_module,
        "TracerProvider",
        lambda resource: _ExplodingProvider(resource),
    )

    summary_file = tmp_path / "summary.csv"
    bridge = bridge_module.Bridge(
        socket_path=str(tmp_path / "bridge.sock"),
        endpoint="http://collector",
        service_name="bridge-svc",
        summary_file=str(summary_file),
        spans_file=None,
    )
    bridge._summary_rows.append(
        {
            "service": "svc",
            "source": "engine",
            "name": "advance",
            "span_id": "abc",
            "start_ns": 100,
            "end_ns": 1100,
            "duration_ms": 1.0,
            "attrs": {},
        }
    )

    closed = {"value": False}
    monkeypatch.setattr(bridge, "_close", lambda: closed.__setitem__("value", True))

    class _StopSocket:
        def __init__(self):
            self.closed = False

        def settimeout(self, _value):
            return None

        def recv(self, _size):
            bridge.stop()
            raise socket.timeout()

        def close(self):
            self.closed = True

    stop_socket = _StopSocket()
    monkeypatch.setattr(bridge, "_open", lambda: setattr(bridge, "_sock", stop_socket))

    bridge.run()

    assert summary_file.exists()
    assert "advance" in summary_file.read_text(encoding="utf-8")
    assert closed["value"] is True


def test_bridge_main_wires_bridge_and_signals(monkeypatch, tmp_path: Path):
    observed = {}

    class _DummyBridge:
        def __init__(self, **kwargs):
            observed["kwargs"] = kwargs

        def stop(self, *_args):
            observed["stopped"] = True

        def run(self):
            observed["ran"] = True

    monkeypatch.setattr(bridge_module, "Bridge", _DummyBridge)
    monkeypatch.setattr(bridge_module.signal, "signal", lambda sig, handler: observed.setdefault("signals", []).append(sig))
    monkeypatch.setattr(bridge_module.logging, "basicConfig", lambda **kwargs: observed.setdefault("logging", kwargs))

    rc = bridge_module.main(
        [
            "--socket",
            str(tmp_path / "bridge.sock"),
            "--summary-file",
            str(tmp_path / "summary.csv"),
            "--spans-file",
            str(tmp_path / "spans.jsonl"),
            "--log-file",
            str(tmp_path / "bridge.log"),
        ]
    )

    assert rc == 0
    assert observed["kwargs"]["socket_path"].endswith("bridge.sock")
    assert observed["ran"] is True
    assert len(observed["signals"]) == 2
