from flux_fiction._adapters.flux import modules


class _DummyRPC:
    def __init__(self, result=None):
        self._result = result if result is not None else {}

    def get(self):
        return self._result


class _DummyHandle:
    def __init__(self, mods):
        self._mods = mods
        self.calls = []

    def rpc(self, method, payload=None, nodeid=None):
        self.calls.append((method, payload, nodeid))
        if method == "module.list":
            return _DummyRPC({"mods": self._mods})
        return _DummyRPC({})


def _loaded_fluxion_modules():
    return [
        {
            "name": "job-ingest",
            "path": "/tmp/job-ingest.so",
            "services": [],
        },
        {
            "name": "sched-fluxion-qmanager",
            "path": "/tmp/sched-fluxion-qmanager.so",
            "services": [],
        },
        {
            "name": "sched-fluxion-resource",
            "path": "/tmp/sched-fluxion-resource.so",
            "services": [],
        },
        {
            "name": "sched-fluxion-feasibility",
            "path": "/tmp/sched-fluxion-feasibility.so",
            "services": [],
        },
        {
            "name": "resource",
            "path": "/tmp/resource.so",
            "services": [],
        },
    ]


def _config_load_payload(handle):
    for method, payload, _nodeid in handle.calls:
        if method == "config.load":
            return payload
    raise AssertionError("config.load was not called")


def _job_ingest_reload_payload(handle):
    for method, payload, _nodeid in handle.calls:
        if method == "job-ingest.config-reload":
            return payload
    raise AssertionError("job-ingest.config-reload was not called")


def test_reload_modules_injects_feasibility_validator_without_config_source():
    handle = _DummyHandle(_loaded_fluxion_modules())

    modules.reload_modules(handle, None)

    assert _config_load_payload(handle) == {
        "ingest": {
            "validator": {
                "plugins": ["feasibility", "jobspec"],
            }
        }
    }
    assert _job_ingest_reload_payload(handle) == {
        "ingest": {
            "validator": {
                "plugins": ["feasibility", "jobspec"],
            }
        }
    }


def test_reload_modules_preserves_existing_validator_plugins():
    handle = _DummyHandle(_loaded_fluxion_modules())
    config = {
        "sched-fluxion-qmanager": {"queue-policy": "easy"},
        "sched-fluxion-resource": {
            "match-policy": "firstnodex",
            "match-format": "rv1_noexec",
        },
        "ingest": {
            "validator": {
                "plugins": ["require-instance", "jobspec"],
            }
        },
    }

    modules.reload_modules(handle, config)

    assert _config_load_payload(handle) == {
        "sched-fluxion-qmanager": {"queue-policy": "easy"},
        "sched-fluxion-resource": {
            "match-policy": "firstnodex",
            "match-format": "rv1_noexec",
        },
        "ingest": {
            "validator": {
                "plugins": ["feasibility", "jobspec", "require-instance"],
            }
        },
    }
    assert _job_ingest_reload_payload(handle) == {
        "sched-fluxion-qmanager": {"queue-policy": "easy"},
        "sched-fluxion-resource": {
            "match-policy": "firstnodex",
            "match-format": "rv1_noexec",
        },
        "ingest": {
            "validator": {
                "plugins": ["feasibility", "jobspec", "require-instance"],
            }
        },
    }
