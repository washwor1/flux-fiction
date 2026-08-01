from __future__ import annotations

import importlib.util
import sys
import types

if importlib.util.find_spec("tqdm") is None:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._core.models import Job
from flux_fiction._outputs.vis import _resource_capacities


RABBIT_STORAGE = {
    "resource_type": "ssd",
    "parent_type": "rack",
    "nodes_per_parent": 16,
    "shares_per_parent": 36,
    "share_gib": 453.0,
    "max_parent_gib": 16308.0,
    "parent_count": 72,
}


def test_rabbit_storage_jobspec_uses_gib_for_fluxion_count():
    job = Job(
        nnodes=4,
        ncpus=160,
        submit_time=0,
        elapsed_time=10,
        timelimit=20,
        rabbit_storage_gib=8154.0,
    )
    job.set_rabbit_storage_shape(RABBIT_STORAGE)

    assert job.rabbit_storage_share_count == 18
    assert job.rabbit_storage_request_count == 8154

    rabbit_slot = job.jobspec["resources"][0]
    children = {child["type"]: child for child in rabbit_slot["with"]}

    assert rabbit_slot["type"] == "slot"
    assert rabbit_slot["label"] == "rabbit"
    assert rabbit_slot["count"] == 1
    assert children["node"]["count"] == 4
    assert children["ssd"]["count"] == 8154
    assert children["ssd"]["exclusive"] is True


def test_rabbit_storage_jobspec_splits_large_requests_across_parents():
    job = Job(
        nnodes=4,
        ncpus=160,
        submit_time=0,
        elapsed_time=10,
        timelimit=20,
        rabbit_storage_gib=20000.0,
    )
    job.set_rabbit_storage_shape(RABBIT_STORAGE)

    rabbit_slot = job.jobspec["resources"][0]
    children = {child["type"]: child for child in rabbit_slot["with"]}

    assert rabbit_slot["type"] == "slot"
    assert rabbit_slot["label"] == "rabbit"
    assert rabbit_slot["count"] == 2
    assert children["node"]["count"] == 2
    assert children["ssd"]["count"] == 10000


def test_rabbit_storage_capacity_prefers_storage_size_over_share_count():
    capacities = _resource_capacities({
        "nnodes": 1153,
        "cores_per_node": 96,
        "gpus_per_node": 4,
        "leaf_resources": {
            "ssd": {"count": 2592, "size": 1153692.0, "unit": "GiB"},
        },
        "rabbit_storage": RABBIT_STORAGE,
    })

    assert capacities["ssd"] == 1153692.0


def test_jobspec_override_bypasses_generated_shape():
    job = Job(
        nnodes=4,
        ncpus=160,
        submit_time=0,
        elapsed_time=10,
        timelimit=20,
        rabbit_storage_gib=8154.0,
    )
    override = {
        "version": 1,
        "resources": [
            {
                "type": "slot",
                "count": 1,
                "label": "rabbit",
                "with": [
                    {
                        "type": "node",
                        "count": 1,
                        "with": [
                            {
                                "type": "slot",
                                "count": 1,
                                "label": "task",
                                "with": [{"type": "core", "count": 1}],
                            }
                        ],
                    }
                ],
            }
        ],
        "tasks": [{"command": ["hostname"], "slot": "task", "count": {"per_slot": 1}}],
        "attributes": {"system": {"duration": 0}},
    }

    job.set_jobspec_override(override)

    assert job.jobspec == override


def test_generated_jobspec_can_omit_core_resources():
    job = Job(
        nnodes=4,
        ncpus=160,
        submit_time=0,
        elapsed_time=10,
        timelimit=20,
    )
    job.set_jobspec_shape({}, omit_core_resources=True)

    node = job.jobspec["resources"][0]
    assert node["type"] == "node"
    assert node["count"] == 4
    assert "with" not in node
