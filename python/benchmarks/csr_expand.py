# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""CSR build/rebuild cost and single-hop latency against the join path.

For each edge count, writes a deterministic random graph to Lance (nodes =
edges / AVG_DEGREE), then in a fresh subprocess per mode (``csr`` / ``join``):

- engine construction time, and for ``csr`` the CSR build time (both directions);
- warm latency (median/p95) of single-hop queries, with row counts and checksums
  checked against the other mode;
- refresh cost after appending edges (CSR rebuild) and with nothing changed;
- peak RSS of the process.

    python benchmarks/csr_expand.py --data /tmp/csr-bench --edges 100000 1000000
"""

import argparse
import json
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

AVG_DEGREE = 10
QUERIES = {
    "point": "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 42 RETURN b.id",
    "selective_100": (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id < 100 RETURN b.id"
    ),
    "selective_1pct": None,  # filled in from the node count
    # Same shapes returning a non-id target column, so the target rows are fetched.
    "point_name": "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 42 RETURN b.name",
    "selective_1pct_name": None,
    "all_edges": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n",
}


def generate(root: Path, edges: int) -> None:
    import lance
    import numpy as np
    import pyarrow as pa

    if (root / "_done").exists():
        return
    nodes = edges // AVG_DEGREE
    rng = np.random.default_rng(7)
    ids = np.arange(nodes, dtype=np.int64)
    lance.write_dataset(
        pa.table({"id": ids, "name": pa.array([f"n{i}" for i in range(nodes)])}),
        root / "person.lance",
        mode="overwrite",
    )
    src = rng.integers(0, nodes, edges, dtype=np.int64)
    dst = rng.integers(0, nodes, edges, dtype=np.int64)
    lance.write_dataset(
        pa.table({"src": src, "dst": dst}), root / "knows.lance", mode="overwrite"
    )
    (root / "_done").touch()


def summary(samples):
    ordered = sorted(samples)
    p95 = ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]
    return {"median_ms": statistics.median(ordered) * 1e3, "p95_ms": p95 * 1e3}


def child(root: Path, mode: str, repeats: int) -> dict:
    import lance
    import pyarrow as pa
    import pyarrow.compute as pc
    from lance_csr import CypherEngine, GraphConfig

    config = (
        GraphConfig.builder()
        .with_node_label("Person", "id")
        .with_relationship("KNOWS", "src", "dst")
        .build()
    )
    nodes = lance.dataset(root / "person.lance").count_rows()
    queries = dict(QUERIES)
    queries["selective_1pct"] = (
        f"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id < {nodes // 100} RETURN b.id"
    )
    queries["selective_1pct_name"] = queries["selective_1pct"].replace("b.id", "b.name")

    def open_engine():
        datasets = {
            "Person": lance.dataset(root / "person.lance"),
            "KNOWS": lance.dataset(root / "knows.lance"),
        }
        return CypherEngine(config, datasets, use_csr=(mode == "csr"))

    out = {"mode": mode, "nodes": nodes}
    start = time.perf_counter()
    engine = open_engine()
    out["construct_ms"] = (time.perf_counter() - start) * 1e3
    if mode == "csr":
        stats = engine.csr_stats()["KNOWS"]
        out["csr_build_ms"] = stats["build_ms"]
        out["csr_edges"] = stats["edges"]
        out["node_index_build_ms"] = engine.node_index_stats()["Person"]["build_ms"]
        plan = engine.explain(queries["point"])
        assert "take=b" in plan and "HashJoinExec" not in plan
    out["rss_after_construct_mib"] = peak_rss_mib()

    for name, query in queries.items():
        result = engine.execute(query)
        first = result.column(0)
        # Name columns are checked by summed length; ids by summed value.
        values = pc.utf8_length(first) if pa.types.is_string(first.type) else first
        check = {"rows": result.num_rows, "checksum": pc.sum(values).as_py() or 0}
        samples = []
        for _ in range(repeats):
            t = time.perf_counter()
            engine.execute(query)
            samples.append(time.perf_counter() - t)
        out[name] = {**check, **summary(samples)}

    t = time.perf_counter()
    engine.refresh()
    out["refresh_unchanged_ms"] = (time.perf_counter() - t) * 1e3
    # Recorded before the copy below, which materializes the edge table.
    out["peak_rss_mib"] = peak_rss_mib()

    # Append 0.1% more edges, then refresh: the CSR engine must rebuild.
    n_new = max(1, lance.dataset(root / "knows.lance").count_rows() // 1000)
    new_edges = pa.table(
        {
            "src": pa.array([0] * n_new, pa.int64()),
            "dst": pa.array([1] * n_new, pa.int64()),
        }
    )
    knows = root / f"knows-{mode}.lance"
    # Work on a private copy so runs do not change each other's input.
    lance.write_dataset(
        lance.dataset(root / "knows.lance").to_table(), knows, mode="overwrite"
    )
    engine = CypherEngine(
        config,
        {"Person": lance.dataset(root / "person.lance"), "KNOWS": lance.dataset(knows)},
        use_csr=(mode == "csr"),
    )
    lance.write_dataset(new_edges, knows, mode="append")
    t = time.perf_counter()
    engine.refresh()
    out["refresh_after_append_ms"] = (time.perf_counter() - t) * 1e3
    if mode == "csr":
        out["csr_rebuild_ms"] = engine.csr_stats()["KNOWS"]["build_ms"]
    return out


def peak_rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return (peak if sys.platform == "darwin" else peak * 1024) / 2**20


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--edges", type=int, nargs="+", default=[100_000, 1_000_000])
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--child", nargs=2, metavar=("ROOT", "MODE"))
    args = parser.parse_args()

    if args.child:
        print(json.dumps(child(Path(args.child[0]), args.child[1], args.repeats)))
        return

    for edges in args.edges:
        root = args.data / f"e{edges}"
        root.mkdir(parents=True, exist_ok=True)
        generate(root, edges)
        results = {}
        for mode in ("join", "csr"):
            cmd = [sys.executable, __file__, "--data", str(args.data)]
            cmd += ["--repeats", str(args.repeats), "--child", str(root), mode]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                print(
                    json.dumps(
                        {"edges": edges, "mode": mode, "error": proc.stderr[-2000:]}
                    )
                )
                continue
            results[mode] = json.loads(proc.stdout.strip().splitlines()[-1])
            print(json.dumps({"edges": edges, **results[mode]}), flush=True)
        if len(results) == 2:
            for name in QUERIES:
                same = all(
                    results["join"][name][k] == results["csr"][name][k]
                    for k in ("rows", "checksum")
                )
                assert same, (edges, name, results["join"][name], results["csr"][name])


if __name__ == "__main__":
    main()
