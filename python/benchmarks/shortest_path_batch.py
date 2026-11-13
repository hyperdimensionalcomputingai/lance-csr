# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Latency and peak memory of shortest paths between many bound endpoint pairs.

A ``WANTS`` relationship binds each (source, target) request, so the search operator
receives the pairs as a join's output: more rows than one batch, spread over partitions.
Each workload runs in a fresh subprocess, over Arrow tables in memory and over Lance
datasets on disk, and prints one JSON line with the median latency, the result's row
count and summed path length (to compare builds), and the process's peak RSS.

Graphs have 20,000 nodes and 160,000 ``KNOWS`` edges: ``uniform`` picks targets evenly,
``skewed`` sends half of the edges to a few hubs. Workloads:

- ``pairs``: 20,000 pairs with mostly distinct sources (one target per source);
- ``multi``: 1,000 sources with 16 targets each;
- ``few``: 64 sources with 3 targets each;
- ``all_paths``: ``allShortestPaths`` for 300 sources with 8 targets each;
- ``multi_no_csr``: ``multi`` without the CSR, where the adjacency is built per query.

To compare two builds, run it once per wheel:

    uv run --no-project --with <wheel> --with pylance --with numpy \\
        python benchmarks/shortest_path_batch.py --data /tmp/sp-bench
"""

import argparse
import json
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

NODES, DEGREE = 20_000, 8
# name -> (sources, targets per source, Cypher function, use_csr)
WORKLOADS = {
    "pairs": (20_000, 1, "shortestPath", True),
    "multi": (1_000, 16, "shortestPath", True),
    "few": (64, 3, "shortestPath", True),
    "all_paths": (300, 8, "allShortestPaths", True),
    "multi_no_csr": (1_000, 16, "shortestPath", False),
}


def generate(root: Path) -> None:
    import lance
    import numpy as np
    import pyarrow as pa

    if (root / "_done").exists():
        return
    rng = np.random.default_rng(7)
    ids = np.arange(NODES, dtype=np.int64)
    lance.write_dataset(pa.table({"id": ids}), root / "person.lance", mode="overwrite")
    src = np.repeat(ids, DEGREE)
    uniform = rng.integers(0, NODES, src.size, dtype=np.int64)
    hubs = (rng.zipf(1.5, src.size) % NODES).astype(np.int64)
    targets = {
        "uniform": uniform,
        "skewed": np.where(rng.random(src.size) < 0.5, uniform, hubs),
    }
    for graph, dst in targets.items():
        table = pa.table({"src": src, "dst": dst})
        lance.write_dataset(table, root / f"knows-{graph}.lance", mode="overwrite")
    for name, (sources, per_source, _, _) in WORKLOADS.items():
        wanted = pa.table(
            {
                "src": np.repeat(rng.integers(0, NODES, sources), per_source),
                "dst": rng.integers(0, NODES, sources * per_source),
            }
        )
        lance.write_dataset(wanted, root / f"wants-{name}.lance", mode="overwrite")
    (root / "_done").touch()


def child(root: Path, graph: str, name: str, storage: str, repeats: int) -> dict:
    import lance
    import pyarrow.compute as pc
    from lance_csr import CypherEngine, GraphConfig

    _, _, function, use_csr = WORKLOADS[name]
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .rel_table("WANTS", "Person", "Person", "src", "dst")
        .build()
    )
    datasets = {
        "Person": lance.dataset(root / "person.lance"),
        "KNOWS": lance.dataset(root / f"knows-{graph}.lance"),
        "WANTS": lance.dataset(root / f"wants-{name}.lance"),
    }
    if storage == "memory":
        datasets = {label: data.to_table() for label, data in datasets.items()}
    engine = CypherEngine(config, datasets, use_csr=use_csr)
    query = (
        "MATCH (a:Person)-[:WANTS]->(b:Person) "
        f"MATCH p = {function}((a)-[:KNOWS*1..8]->(b)) "
        "RETURN length(p) AS l, nodes(p) AS ns"
    )
    result = engine.execute(query)  # warm-up
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        engine.execute(query)
        samples.append(time.perf_counter() - start)
    return {
        "graph": graph,
        "workload": name,
        "storage": storage,
        "median_ms": statistics.median(samples) * 1e3,
        "rows": result.num_rows,
        "total_length": pc.sum(result.column("l")).as_py() or 0,
        "peak_rss_mib": peak_rss_mib(),
    }


def peak_rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Bytes on macOS, kibibytes on Linux.
    return peak / (2**20 if sys.platform == "darwin" else 2**10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--child", nargs=3, metavar=("GRAPH", "WORKLOAD", "STORAGE"))
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(args.data, *args.child, args.repeats)))
        return
    args.data.mkdir(parents=True, exist_ok=True)
    generate(args.data)
    for graph in ("uniform", "skewed"):
        for name in WORKLOADS:
            for storage in ("memory", "lance"):
                command = [sys.executable, __file__, "--data", str(args.data)]
                command += ["--repeats", str(args.repeats)]
                command += ["--child", graph, name, storage]
                subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
