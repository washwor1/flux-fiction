import os
import json
import copy
import sys
from collections import Counter, deque

import logging 
logger = logging.getLogger(__name__)


def _flux_modules():
    import flux
    from flux.resource import ResourceSet
    return flux, ResourceSet


def attach_scheduling_graph(rlist_json: dict, scheduling: dict) -> dict:
    """
    Mirror cmd_parse_config(): rl->scheduling = sched_json
    No conversion, no wrapping (unless you choose to).
    """
    if not isinstance(rlist_json, dict):
        raise TypeError("rlist_json must be a dict")
    if not isinstance(scheduling, dict):
        raise TypeError("scheduling must be a dict (parsed JSON object)")
    rlist_json["scheduling"] = scheduling
    return rlist_json


def load_json_file(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def is_resource_r_json(obj: dict) -> bool:
    return (
        isinstance(obj, dict)
        and obj.get("version") == 1
        and isinstance(obj.get("execution"), dict)
        and isinstance(obj["execution"].get("R_lite"), list)
    )


def _graph_obj(obj: dict) -> dict | None:
    if not isinstance(obj, dict):
        return None
    scheduling = obj.get("scheduling")
    if isinstance(scheduling, dict) and isinstance(scheduling.get("graph"), dict):
        return scheduling["graph"]
    if isinstance(obj.get("graph"), dict):
        return obj["graph"]
    return None


def _normalize_legacy_storage_status(resource_obj: dict) -> dict:
    """
    Normalize legacy graph status values before handing resource.R to Fluxion.

    Fluxion's JGF reader uses status 0 for UP and 1 for DOWN. Some generated
    Tuolumne/Rabbit graphs used status 1 on every SSD vertex to mean present or
    enabled, which makes the scheduler load all Rabbit storage shares as down.
    If every SSD vertex has that legacy value, load a corrected copy with those
    storage vertices marked UP.
    """
    graph = _graph_obj(resource_obj)
    if not graph:
        return resource_obj

    ssd_metadata = [
        node.get("metadata", {})
        for node in graph.get("nodes", [])
        if node.get("metadata", {}).get("type") == "ssd"
    ]
    if not ssd_metadata:
        return resource_obj

    statuses = [metadata.get("status") for metadata in ssd_metadata]
    if statuses and all(status == 1 for status in statuses):
        normalized = copy.deepcopy(resource_obj)
        normalized_graph = _graph_obj(normalized)
        changed = 0
        for node in normalized_graph.get("nodes", []):
            metadata = node.get("metadata", {})
            if metadata.get("type") == "ssd":
                metadata["status"] = 0
                changed += 1
        message = (
            f"Normalized {changed} legacy SSD status values "
            "from 1/DOWN to 0/UP for Fluxion"
        )
        logger.warning(message)
        print(message, file=sys.stderr)
        return normalized

    return resource_obj


def _idset_count(value) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, int):
        return 1
    total = 0
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            total += abs(int(end) - int(start)) + 1
        else:
            total += 1
    return total


def _mode_positive(counter: Counter, default=0) -> int:
    positives = Counter({int(k): v for k, v in counter.items() if int(k) > 0})
    if not positives:
        return int(default or 0)
    return positives.most_common(1)[0][0]


def _infer_from_r_lite(resource_obj: dict) -> dict:
    nnodes = 0
    core_counts = Counter()
    gpu_counts = Counter()

    for item in resource_obj.get("execution", {}).get("R_lite", []):
        rank_count = _idset_count(item.get("rank"))
        if rank_count <= 0:
            continue
        nnodes += rank_count
        children = item.get("children", {})
        core_counts[_idset_count(children.get("core"))] += rank_count
        gpu_counts[_idset_count(children.get("gpu"))] += rank_count

    return {
        "nnodes": nnodes,
        "cores_per_node": _mode_positive(core_counts),
        "gpus_per_node": _mode_positive(gpu_counts),
    }


def _node_type_maps(graph: dict):
    type_by_id = {}
    children_by_id = {}
    for node in graph.get("nodes", []):
        node_id = str(node.get("id"))
        metadata = node.get("metadata", {})
        type_by_id[node_id] = metadata.get("type")
    for edge in graph.get("edges", []):
        source = str(edge.get("source"))
        target = str(edge.get("target"))
        children_by_id.setdefault(source, []).append(target)
    return type_by_id, children_by_id


def _shortest_type_path(type_by_id: dict, children_by_id: dict,
                        start_id: str, target_type: str) -> list[str] | None:
    queue = deque([(start_id, [])])
    visited = {start_id}
    while queue:
        node_id, path = queue.popleft()
        for child_id in children_by_id.get(node_id, []):
            if child_id in visited:
                continue
            visited.add(child_id)
            child_type = type_by_id.get(child_id)
            next_path = path + [child_type]
            if child_type == target_type:
                return next_path
            queue.append((child_id, next_path))
    return None


def _mode_child_count(type_by_id: dict, children_by_id: dict,
                      parent_type: str, child_type: str) -> int:
    counts = Counter()
    for node_id, node_type in type_by_id.items():
        if node_type != parent_type:
            continue
        count = sum(
            1 for child_id in children_by_id.get(node_id, [])
            if type_by_id.get(child_id) == child_type
        )
        if count:
            counts[count] += 1
    return _mode_positive(counts, default=1) or 1


def _infer_rabbit_storage(graph: dict | None) -> dict:
    empty = {
        "resource_type": "ssd",
        "parent_type": None,
        "nodes_per_parent": 0,
        "shares_per_parent": 0,
        "share_gib": 0,
        "min_share_gib": 0,
        "max_share_gib": 0,
        "min_parent_gib": 0,
        "max_parent_gib": 0,
        "parent_count": 0,
    }
    if not graph:
        return empty

    type_by_id, children_by_id = _node_type_maps(graph)
    metadata_by_id = {
        str(node.get("id")): node.get("metadata", {})
        for node in graph.get("nodes", [])
    }

    parent_records = []
    for parent_id, child_ids in children_by_id.items():
        node_children = [
            child_id for child_id in child_ids
            if type_by_id.get(child_id) == "node"
        ]
        storage_children = [
            child_id for child_id in child_ids
            if type_by_id.get(child_id) == "ssd"
        ]
        if not node_children or not storage_children:
            continue

        sizes = [
            float(metadata_by_id.get(child_id, {}).get("size", 0) or 0)
            for child_id in storage_children
        ]
        parent_records.append({
            "parent_type": type_by_id.get(parent_id),
            "node_count": len(node_children),
            "share_count": len(storage_children),
            "sizes": sizes,
            "total_gib": sum(sizes),
        })

    if not parent_records:
        return empty

    parent_type = Counter(
        record["parent_type"] for record in parent_records
        if record["parent_type"]
    ).most_common(1)[0][0]
    relevant = [
        record for record in parent_records
        if record["parent_type"] == parent_type
    ]
    all_sizes = [
        size for record in relevant
        for size in record["sizes"]
        if size > 0
    ]
    size_counts = Counter(all_sizes)
    share_gib = size_counts.most_common(1)[0][0] if size_counts else 0
    totals = [record["total_gib"] for record in relevant if record["total_gib"] > 0]

    return {
        "resource_type": "ssd",
        "parent_type": parent_type,
        "nodes_per_parent": _mode_positive(Counter(record["node_count"] for record in relevant)),
        "shares_per_parent": _mode_positive(Counter(record["share_count"] for record in relevant)),
        "share_gib": share_gib,
        "min_share_gib": min(all_sizes) if all_sizes else 0,
        "max_share_gib": max(all_sizes) if all_sizes else 0,
        "min_parent_gib": min(totals) if totals else 0,
        "max_parent_gib": max(totals) if totals else 0,
        "parent_count": len(relevant),
    }


def _infer_jobspec_shape(graph: dict | None) -> dict:
    if not graph:
        return {"intermediate_types": [], "intermediate_counts": {}}

    type_by_id, children_by_id = _node_type_maps(graph)
    node_ids = [node_id for node_id, node_type in type_by_id.items() if node_type == "node"]
    if not node_ids:
        return {"intermediate_types": [], "intermediate_counts": {}}

    sample_node = node_ids[0]
    paths = []
    for resource_type in ("core", "gpu"):
        path = _shortest_type_path(type_by_id, children_by_id, sample_node, resource_type)
        if path:
            paths.append(path[:-1])

    if not paths:
        return {"intermediate_types": [], "intermediate_counts": {}}

    shortest = min(len(path) for path in paths)
    common = []
    for idx in range(shortest):
        values = {path[idx] for path in paths}
        if len(values) != 1:
            break
        common.append(paths[0][idx])

    counts = {}
    parent_type = "node"
    for child_type in common:
        counts[child_type] = _mode_child_count(type_by_id, children_by_id, parent_type, child_type)
        parent_type = child_type

    return {"intermediate_types": common, "intermediate_counts": counts}


def _infer_leaf_resources(graph: dict | None) -> dict:
    if not graph:
        return {}

    type_by_id, children_by_id = _node_type_maps(graph)
    counts = Counter()
    sizes = {}
    units = {}
    metadata_by_id = {
        str(node.get("id")): node.get("metadata", {})
        for node in graph.get("nodes", [])
    }

    for node_id, node_type in type_by_id.items():
        if not node_type or children_by_id.get(node_id):
            continue
        counts[node_type] += 1
        metadata = metadata_by_id.get(node_id, {})
        size = metadata.get("size")
        if size not in (None, ""):
            sizes[node_type] = sizes.get(node_type, 0.0) + float(size)
        unit = metadata.get("unit")
        if unit:
            units[node_type] = unit

    out = {}
    for node_type, count in counts.items():
        facts = {"count": int(count)}
        if node_type in sizes:
            facts["size"] = sizes[node_type]
        if node_type in units:
            facts["unit"] = units[node_type]
        out[node_type] = facts
    return out


def describe_resource_config(cfg) -> dict:
    """
    Return the resource facts the simulator needs without touching Flux KVS.

    The values inferred from a full RV1 resource.R are intentionally based on
    the mode rather than the max so a small management rank does not define the
    normal compute-node shape.
    """
    resource_obj = None
    source_path = getattr(cfg, "resource_R", None) or getattr(cfg, "resource_file", None)
    if source_path:
        obj = load_json_file(source_path)
        if is_resource_r_json(obj):
            resource_obj = obj
        elif getattr(cfg, "resource_R", None):
            raise ValueError(f"resource_R must be a full RV1 resource.R JSON object: {source_path}")

    inferred = {}
    if resource_obj:
        inferred.update(_infer_from_r_lite(resource_obj))

    graph = _graph_obj(resource_obj) if resource_obj else None
    if graph is None and source_path:
        graph = _graph_obj(load_json_file(source_path))

    shape = _infer_jobspec_shape(graph)

    return {
        "source_path": source_path,
        "nnodes": inferred.get("nnodes") or int(getattr(cfg, "nnodes", 0) or 0),
        "cores_per_node": inferred.get("cores_per_node") or int(getattr(cfg, "ncpus", 1) or 1),
        "gpus_per_node": inferred.get("gpus_per_node") or int(getattr(cfg, "ngpus", 0) or 0),
        "jobspec_shape": shape,
        "rabbit_storage": _infer_rabbit_storage(graph),
        "leaf_resources": _infer_leaf_resources(graph),
    }


def _idset_range(count: int) -> str:
    return f"0-{count - 1}" if count > 1 else "0"


def _build_resource_r(num_ranks, cores_per_rank,
                      hostname_pattern="node{rank}", gpus_per_rank=0) -> dict:
    """
    Build an RFC20/RV1 resource.R object.

    Flux's older Python bindings exposed flux.resource.Rlist with add_rank()
    helpers. Newer bindings expose ResourceSet/Rv1Set, but not that builder
    API, so synthesize the small RV1 JSON structure we need directly.
    """
    children = {"core": _idset_range(cores_per_rank)}
    if gpus_per_rank and gpus_per_rank > 0:
        children["gpu"] = _idset_range(gpus_per_rank)

    rjson = {
        "version": 1,
        "execution": {
            "R_lite": [
                {
                    "rank": _idset_range(num_ranks),
                    "children": children,
                }
            ],
            "starttime": 0.0,
            "expiration": 0.0,
            "nodelist": [
                hostname_pattern.format(rank=rank)
                for rank in range(num_ranks)
            ],
        },
    }

    # Validate against the installed Flux resource parser, and normalize the
    # output shape before writing it into the KVS.
    _, ResourceSet = _flux_modules()
    return ResourceSet(rjson).to_dict()


def insert_resource_data(flux_handle, num_ranks, cores_per_rank,
                         hostname_pattern="node{rank}", gpus_per_rank=0,
                         scheduling_path=None, scheduling_obj=None):
    """
    Build resource.R as RFC20/RV1 JSON, then commit it to KVS.
    """
    if num_ranks <= 0 or cores_per_rank <= 0:
        raise ValueError("Number of ranks and cores per rank must be positive integers")

    rlist_json = _build_resource_r(
        num_ranks,
        cores_per_rank,
        hostname_pattern=hostname_pattern,
        gpus_per_rank=gpus_per_rank,
    )

    logger.debug("resource.R going to KVS:\n%s",
                 json.dumps(rlist_json, indent=2, sort_keys=True))

    # Attach scheduling JSON
    if scheduling_path and scheduling_obj:
        raise ValueError("Use only one of scheduling_path or scheduling_obj")

    if scheduling_path:
        sched = _normalize_legacy_storage_status(load_json_file(scheduling_path))
        out = attach_scheduling_graph(rlist_json, sched)
        if out is not None:
            rlist_json = out
    elif scheduling_obj:
        out = attach_scheduling_graph(
            rlist_json,
            _normalize_legacy_storage_status(scheduling_obj),
        )
        if out is not None:
            rlist_json = out

    # debug-print the final thing
    logger.debug("FINAL resource.R going to KVS:\n%s",
                 json.dumps(rlist_json, indent=2, sort_keys=True))


    # Put into KVS and commit
    flux, ResourceSet = _flux_modules()
    kvs_key = "resource.R"
    put_rc = flux.kvs.put(flux_handle, kvs_key, rlist_json)
    if put_rc is not None:
        raise ValueError(f"Error inserting resource data into KVS, rc={put_rc}")

    commit_rc = flux.kvs.commit(flux_handle)
    if commit_rc is not None:
        raise ValueError(f"Error committing resource data to KVS, rc={commit_rc}")
    
    logger.debug("ResourceSet ENCODE(): %s", ResourceSet(rlist_json).encode())


def install_resource_from_path(flux_handle, rjson_path: str):
    """
    Load either a full RV1 resource.R JSON object or a scheduling graph file.

    Full resource.R files are installed directly. Plain scheduling graph files
    should be handled by insert_resource_data() so the caller can synthesize the
    execution R_lite from cfg.nnodes/cfg.ncpus and attach the graph.
    """
    if not os.path.exists(rjson_path):
        raise FileNotFoundError(f"Resource JSON file not found: {rjson_path}")
    rjson = load_json_file(rjson_path)
    if not is_resource_r_json(rjson):
        raise ValueError(f"Resource JSON is not a full RV1 resource.R object: {rjson_path}")
    insert_resource_R_obj(flux_handle, rjson, source_path=rjson_path)


def insert_resource_R_obj(flux_handle, rjson: dict, *, source_path: str | None = None):
    if not isinstance(rjson, dict):
        raise ValueError("resource.R JSON must be a JSON object")
    rjson = _normalize_legacy_storage_status(rjson)

    if source_path:
        logger.info("Loading full resource.R from %s", source_path)
    else:
        logger.info("Loading full resource.R object")

    flux, _ = _flux_modules()
    rc = flux.kvs.put(flux_handle, "resource.R", rjson)
    if rc is not None:
        raise RuntimeError(f"flux.kvs.put(resource.R) failed, rc={rc}")

    rc = flux.kvs.commit(flux_handle)
    if rc is not None:
        raise RuntimeError(f"flux.kvs.commit() failed, rc={rc}")


def insert_resource_R_from_json(flux_handle, rjson_path: str):
    """
    Load a complete RFC20 / RV1 resource description from JSON
    and install it into KVS as resource.R.

    This mirrors the C path:
      json_load_file -> rlist_from_json -> rlist_puts
    except we already trust the JSON is valid.
    """
    if not os.path.exists(rjson_path):
        raise FileNotFoundError(f"Resource JSON file not found: {rjson_path}")

    rjson = load_json_file(rjson_path)
    if not is_resource_r_json(rjson):
        raise ValueError(f"resource_R must be a full RV1 resource.R JSON object: {rjson_path}")
    insert_resource_R_obj(flux_handle, rjson, source_path=rjson_path)
