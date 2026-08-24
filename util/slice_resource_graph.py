#!/usr/bin/env python3
"""Carve a whole number of chassis out of the live Tuolumne JGF graph.

Why: the gaussian ablation needs 128 nodes *with rabbits*. Flux-fiction can
synthesize a graph from nnodes/ncpus/ngpus, but that path emits only
cluster/node/core/gpu -- no ssd and no storage_node -- so rabbit requests have
no capacity to draw against and `ceiling_basis = "rabbit_node"` cannot infer
one. Slicing the real graph keeps the rabbit topology intact.

Chassis 0 is deliberately skipped: it holds 12 nodes, not 16, so starting there
would make the node count depend on how many chassis you asked for. Chassis
1..8 is exactly 128 nodes.

Usage:
  slice_resource_graph.py --src tuolumne.json --out out.json --first 1 --count 8
"""
from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path


def containment(node: dict) -> str:
    return ((node.get("metadata") or {}).get("paths") or {}).get("containment", "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--first", type=int, default=1, help="first chassis index to keep")
    ap.add_argument("--count", type=int, default=8, help="how many chassis to keep")
    args = ap.parse_args()

    doc = json.loads(Path(args.src).read_text())
    graph = doc["graph"]
    keep_idx = set(range(args.first, args.first + args.count))

    keep_ids: set[str] = set()
    kept_nodes = []
    for node in graph["nodes"]:
        path = containment(node)
        kind = (node.get("metadata") or {}).get("type")
        if kind == "cluster":
            take = True
        else:
            m = re.match(r"/tuolumne/chassis(\d+)", path)
            take = bool(m) and int(m.group(1)) in keep_idx
        if take:
            keep_ids.add(str(node["id"]))
            kept_nodes.append(node)

    # An edge survives only if both endpoints did, otherwise the traverser walks
    # into a vertex that is no longer in the graph.
    kept_edges = [
        e for e in graph.get("edges", [])
        if str(e.get("source")) in keep_ids and str(e.get("target")) in keep_ids
    ]

    out = {"graph": {**{k: v for k, v in graph.items() if k not in ("nodes", "edges")},
                     "nodes": kept_nodes, "edges": kept_edges}}
    Path(args.out).write_text(json.dumps(out))

    counts = collections.Counter((n.get("metadata") or {}).get("type") for n in kept_nodes)
    print(f"wrote {args.out}")
    print(f"  vertices {len(kept_nodes)}  edges {len(kept_edges)}")
    print(f"  types {dict(counts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
