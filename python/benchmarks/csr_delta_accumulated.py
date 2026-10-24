# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Refresh cost as CSR deltas accumulate over many small commits.

Uses a graph written by ``csr_expand.py``. On a fresh copy of the edge table,
appends ``--step`` of the edges ``--commits`` times, calling ``refresh()`` after
each, and prints each refresh's time with the delta size so far. With deltas that
are copied on every refresh, the time grows with the accumulated delta; with
shared delta layers it should stay roughly proportional to the step.

    python benchmarks/csr_delta_accumulated.py --data /tmp/csr-bench --edges 10000000
"""

import argparse
import json
import time
from pathlib import Path

import lance
import pyarrow as pa
import pyarrow.compute as pc
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--edges", type=int, default=10_000_000)
    parser.add_argument("--step", type=float, default=0.01)
    parser.add_argument("--commits", type=int, default=20)
    args = parser.parse_args()

    root = args.data / f"e{args.edges}"
    person = lance.dataset(root / "person.lance")
    nodes = person.count_rows()
    uri = root / "knows-accumulated.lance"
    base = lance.dataset(root / "knows.lance").to_table()
    lance.write_dataset(base, uri, mode="overwrite", max_rows_per_file=1_000_000)
    engine = CypherEngine(
        CONFIG, {"Person": person, "KNOWS": lance.dataset(uri)}, use_csr=True
    )

    n = int(args.edges * args.step)
    for commit in range(args.commits):
        ids = pa.array(range(commit * n, (commit + 1) * n), pa.int64())
        new = pa.table(
            {
                "src": pc.remainder(ids, nodes),
                "dst": pc.remainder(pc.add(ids, 3), nodes),
            }
        )
        lance.write_dataset(new, uri, mode="append")
        start = time.perf_counter()
        engine.refresh()
        refresh_ms = (time.perf_counter() - start) * 1e3
        stats = engine.csr_stats()["KNOWS"]
        print(
            json.dumps(
                {
                    "commit": commit + 1,
                    "update": stats["update"],
                    "delta_edges": stats["delta_edges"],
                    "refresh_ms": refresh_ms,
                }
            ),
            flush=True,
        )

    # The accumulated deltas must answer exactly like a clean build.
    fresh = CypherEngine(
        CONFIG, {"Person": person, "KNOWS": lance.dataset(uri)}, use_csr=True
    )
    query = (
        f"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id < {nodes // 100} RETURN b.id"
    )
    got, want = engine.execute(query).column(0), fresh.execute(query).column(0)
    assert got.length() == want.length()
    assert pc.sum(got).as_py() == pc.sum(want).as_py()


if __name__ == "__main__":
    main()
