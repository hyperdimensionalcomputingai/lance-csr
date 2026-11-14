# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Latency of a named hop that returns a relationship column, against the join path.

Writes a deterministic random graph to Lance (nodes = edges / AVG_DEGREE) whose
relationship has a fixed-width column (``since``) and a string column (``note``),
then in a fresh subprocess per mode (``csr`` / ``join``) reads it lazily and times

    MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id < N RETURN sum(r.since)

for a growing number of start nodes, in both directions, and the same hop returning
``note``. Row counts and checksums are checked between the modes. A source's edges are
scattered through the relationship table, so the CSR fetches scattered rows.

    python benchmarks/named_hop_columns.py --data /tmp/hop-bench --edges 2000000
"""

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

AVG_DEGREE = 160
HOPS = {
    "out, since": "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id < {n} "
    "RETURN count(r.since) AS rows, sum(r.since) AS checksum",
    "in, since": "MATCH (a:Person)<-[r:KNOWS]-(b:Person) WHERE a.id < {n} "
    "RETURN count(r.since) AS rows, sum(r.since) AS checksum",
    "out, note": "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id < {n} "
    "RETURN count(r.note) AS rows, sum(size(r.note)) AS checksum",
}


def generate(root: Path, edges: int) -> None:
    import lance
    import numpy as np
    import pyarrow as pa

    if (root / "_done").exists():
        return
    nodes = edges // AVG_DEGREE
    rng = np.random.default_rng(7)
    lance.write_dataset(
        pa.table({"id": np.arange(nodes, dtype=np.int64)}),
        root / "person.lance",
        mode="overwrite",
    )
    since = rng.integers(1_262_304_000_000, 1_356_998_400_000, edges, dtype=np.int64)
    knows = pa.table(
        {
            "src": rng.integers(0, nodes, edges, dtype=np.int64),
            "dst": rng.integers(0, nodes, edges, dtype=np.int64),
            "since": since,
            "note": pa.array([f"n{i % 1000}" for i in range(edges)]),
        }
    )
    lance.write_dataset(knows, root / "knows.lance", mode="overwrite")
    (root / "_done").touch()


def child(root: Path, mode: str, repeats: int) -> dict:
    import lance
    from lance_csr import CypherEngine, GraphConfig

    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    datasets = {
        "Person": lance.dataset(root / "person.lance"),
        "KNOWS": lance.dataset(root / "knows.lance"),
    }
    nodes = datasets["Person"].count_rows()
    engine = CypherEngine(config, datasets, use_csr=(mode == "csr"))
    out = {}
    for name, hop in HOPS.items():
        for starts in sorted({1, 10, 100, 1000, nodes} & set(range(nodes + 1))):
            query = hop.format(n=starts)
            row = engine.execute(query).to_pylist()[0]
            samples = []
            for _ in range(repeats):
                t = time.perf_counter()
                engine.execute(query)
                samples.append(time.perf_counter() - t)
            out[f"{name}, {starts} starts"] = {
                **row,
                "median_ms": statistics.median(samples) * 1e3,
            }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--edges", type=int, nargs="+", default=[200_000, 2_000_000])
    parser.add_argument("--repeats", type=int, default=9)
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
                sys.exit(f"{mode} at {edges} edges failed:\n{proc.stderr[-2000:]}")
            results[mode] = json.loads(proc.stdout.strip().splitlines()[-1])
        print(f"{edges} edges{'rows':>24}{'csr ms':>10}{'join ms':>10}")
        for name, csr in results["csr"].items():
            join = results["join"][name]
            same = all(csr[k] == join[k] for k in ("rows", "checksum"))
            assert same, (edges, name, csr, join)
            times = f"{csr['median_ms']:>10.2f}{join['median_ms']:>10.2f}"
            print(f"  {name:26}{csr['rows']:>9}{times}", flush=True)


if __name__ == "__main__":
    main()
