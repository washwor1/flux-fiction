from __future__ import annotations

from pathlib import Path

from flux_fiction.parallel import flux_launch


def _write_cpu(sys_cpu: Path, cpu: int, siblings: str) -> None:
    topo = sys_cpu / f"cpu{cpu}" / "topology"
    topo.mkdir(parents=True)
    (topo / "thread_siblings_list").write_text(siblings, encoding="utf-8")


def test_topology_respects_current_cpu_affinity(tmp_path: Path):
    sys_node = tmp_path / "node"
    sys_cpu = tmp_path / "cpu"
    node0 = sys_node / "node0"
    node0.mkdir(parents=True)
    (node0 / "cpulist").write_text("0-7", encoding="utf-8")
    for cpu, siblings in {
        0: "0,4",
        1: "1,5",
        2: "2,6",
        3: "3,7",
        4: "0,4",
        5: "1,5",
        6: "2,6",
        7: "3,7",
    }.items():
        _write_cpu(sys_cpu, cpu, siblings)

    topology = flux_launch._read_topology(
        str(sys_node),
        str(sys_cpu),
        allowed_cpus={0, 1, 4, 5},
    )

    assert topology is not None
    assert topology.total_physical_cores() == 2
