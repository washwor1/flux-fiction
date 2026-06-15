import json
import importlib.util
import sys
import types

try:
    _flux_missing = importlib.util.find_spec("flux") is None
except ValueError:
    _flux_missing = "flux" not in sys.modules

if _flux_missing:
    flux_stub = types.ModuleType("flux")
    flux_stub.Flux = type("Flux", (), {})
    flux_stub.constants = types.SimpleNamespace(FLUX_MSGTYPE_REQUEST=0)

    job_stub = types.ModuleType("flux.job")

    class _DummyJournalConsumer:
        def __init__(self, *args, **kwargs):
            pass

        def set_callback(self, cb):
            self._callback = cb

        def start(self):
            return None

    class _DummyJobID(int):
        @property
        def f58(self):
            return str(int(self))

    job_stub.submit = lambda *args, **kwargs: None
    job_stub.job_kvs_lookup = lambda *args, **kwargs: {}
    job_stub.JobID = _DummyJobID
    job_stub.JournalConsumer = _DummyJournalConsumer
    job_stub.RAW = types.SimpleNamespace(cancel=lambda *args, **kwargs: None)

    job_list_stub = types.ModuleType("flux.job.list")
    job_list_stub.get_job = lambda *args, **kwargs: {}
    job_list_stub.job_list_id = lambda *args, **kwargs: types.SimpleNamespace(
        get_jobinfo=lambda: types.SimpleNamespace(nodelist="")
    )

    resource_stub = types.ModuleType("flux.resource")
    resource_stub.ResourceSet = type("ResourceSet", (), {})

    flux_stub.job = job_stub
    flux_stub.resource = resource_stub

    sys.modules["flux"] = flux_stub
    sys.modules["flux.job"] = job_stub
    sys.modules["flux.job.list"] = job_list_stub
    sys.modules["flux.resource"] = resource_stub

from flux_fiction._adapters.flux import adapter as flux_adapter_module
from flux_fiction._adapters.flux.adapter import FluxAdapter


class _DummyRPC:
    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error

    def get(self):
        if self._error is not None:
            raise self._error
        return self._result


class _DummyHandle:
    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    def rpc(self, method, payload=None):
        self.calls.append((method, payload))
        response = self._responses[method]
        if isinstance(response, Exception):
            return _DummyRPC(error=response)
        return _DummyRPC(result=response)


def test_check_jobspec_satisfiability_respects_scheduler_response():
    handle = _DummyHandle({
        "sched-fluxion-resource.satisfiability": {
            "satisfiable": False,
            "errnum": 19,
            "errstr": "Unsatisfiable request",
        },
    })
    adapter = FluxAdapter()
    adapter._handle = handle

    result = adapter.check_jobspec_satisfiability(json.dumps({"resources": []}))

    assert result["satisfiable"] is False
    assert result["method"] == "sched-fluxion-resource.satisfiability"
    assert handle.calls == [
        (
            "sched-fluxion-resource.satisfiability",
            {"jobspec": {"resources": []}},
        )
    ]


def test_check_jobspec_satisfiability_falls_back_to_match_rpc():
    handle = _DummyHandle({
        "sched-fluxion-resource.satisfiability": RuntimeError("no service"),
        "feasibility.check": RuntimeError("no service"),
        "sched-fluxion-resource.match": {"R": "match"},
    })
    adapter = FluxAdapter()
    adapter._handle = handle

    jobspec = {"resources": [{"type": "node", "count": 2}]}
    result = adapter.check_jobspec_satisfiability(json.dumps(jobspec))

    assert result["satisfiable"] is True
    assert result["method"] == "sched-fluxion-resource.match"
    assert [call[0] for call in handle.calls] == [
        "sched-fluxion-resource.satisfiability",
        "feasibility.check",
        "sched-fluxion-resource.match",
    ]


def test_submit_job_does_not_call_feasibility_rpc(monkeypatch):
    submitted = []

    def fake_submit(handle, jobspec_json):
        submitted.append((handle, jobspec_json))
        return 1234

    monkeypatch.setattr(flux_adapter_module.flux.job, "submit", fake_submit)

    adapter = FluxAdapter()
    adapter._handle = object()
    jobspec_json = json.dumps({"resources": [{"type": "slot", "count": 1}]})

    jobid = adapter.submit_job(jobspec_json)

    assert jobid == 1234
    assert submitted == [(adapter._handle, jobspec_json)]
