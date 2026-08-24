from __future__ import annotations

from flux_fiction._adapters.flux import resources


def test_insert_resource_data_normalizes_bare_scheduling_graph(monkeypatch, capsys):
    captured = {}

    class FakeKVS:
        @staticmethod
        def put(_handle, key, value):
            captured[key] = value
            return None

        @staticmethod
        def commit(_handle):
            captured["committed"] = True
            return None

    class FakeFlux:
        kvs = FakeKVS()

    class FakeResourceSet:
        def __init__(self, _payload):
            pass

        def encode(self):
            return "{}"

    monkeypatch.setattr(
        resources,
        "_build_resource_r",
        lambda *args, **kwargs: {
            "version": 1,
            "execution": {"R_lite": [], "starttime": 0.0, "expiration": 0.0},
        },
    )
    monkeypatch.setattr(resources, "_flux_modules", lambda: (FakeFlux, FakeResourceSet))

    scheduling = {
        "graph": {
            "nodes": [
                {"id": 0, "metadata": {"type": "node"}},
                {"id": 1, "metadata": {"type": "ssd", "status": 1}},
                {"id": 2, "metadata": {"type": "ssd", "status": 1}},
            ],
            "edges": [],
        }
    }

    resources.insert_resource_data(
        object(),
        num_ranks=1,
        cores_per_rank=1,
        scheduling_obj=scheduling,
    )

    statuses = [
        node["metadata"]["status"]
        for node in captured["resource.R"]["scheduling"]["graph"]["nodes"]
        if node["metadata"].get("type") == "ssd"
    ]
    assert statuses == [0, 0]
    assert [node["metadata"]["status"] for node in scheduling["graph"]["nodes"][1:]] == [1, 1]
    assert captured["committed"] is True
    assert (
        "Normalized 2 legacy SSD status values from 1/DOWN to 0/UP for Fluxion"
        in capsys.readouterr().err
    )
