# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Incremental CSR maintenance: refresh-with-delta cost against a full rebuild.

Uses the graphs written by ``csr_expand.py`` (``--data`` must point at the same
directory). For each scenario, on a fresh copy of the edge table (1M-row
fragments, plus a ``seq`` row-number column for targeted deletes):

- apply one commit (append or delete);
- time ``refresh()`` on a CSR engine built before the commit (delta path);
- time a full CSR build on the new version (what a rebuild costs);
- compare warm query latency with the delta in place against the clean build,
  checking both return identical results.

    python benchmarks/csr_delta.py --data /tmp/csr-bench --edges 1000000 10000000
"""

import argparse
import json
import statistics
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


def scenarios(edges: int):
    def append(fraction):
        def run(uri, nodes):
            n = max(1, int(edges * fraction))
            ids = pa.array(range(n), pa.int64())
            new = pa.table(
                {
                    "src": pc.remainder(ids, nodes),
                    "dst": pc.remainder(pc.add(ids, 7), nodes),
                    "seq": pc.add(ids, edges),
                }
            )
            lance.write_dataset(new, uri, mode="append")

        return run

    def delete(predicate):
        return lambda uri, nodes: lance.dataset(uri).delete(predicate)

    return {
        "append_0.01pct": append(0.0001),
        "append_1pct": append(0.01),
        "append_10pct": append(0.10),
        # Over the 25% threshold: expected to rebuild.
        "append_30pct": append(0.30),
        # seq < n: rows in the first fragment only.
        "delete_0.1pct_one_fragment": delete(f"seq < {edges // 1000}"),
        # every 100th row: touches every fragment.
        "delete_1pct_all_fragments": delete("seq % 100 = 0"),
    }


def timed(fn):
    start = time.perf_counter()
    out = fn()
    return (time.perf_counter() - start) * 1e3, out


def warm_ms(engine, query, repeats):
    samples = []
    for _ in range(repeats):
        ms, _ = timed(lambda: engine.execute(query))
        samples.append(ms)
    return statistics.median(samples)


def rows(table):
    return sorted(table.to_pylist(), key=lambda r: [str(v) for v in r.values()])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--edges", type=int, nargs="+", default=[1_000_000])
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()

    for edges in args.edges:
        root = args.data / f"e{edges}"
        person = lance.dataset(root / "person.lance")
        nodes = person.count_rows()
        base = lance.dataset(root / "knows.lance").to_table()
        base = base.append_column("seq", pa.array(range(edges), pa.int64()))
        queries = {
            "point": (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 42 RETURN b.id"
            ),
            "selective_1pct": (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) "
                f"WHERE a.id < {nodes // 100} RETURN b.id"
            ),
        }
        for name, change in scenarios(edges).items():
            uri = root / "knows-delta.lance"
            lance.write_dataset(
                base, uri, mode="overwrite", max_rows_per_file=1_000_000
            )
            datasets = {"Person": person, "KNOWS": lance.dataset(uri)}
            engine = CypherEngine(CONFIG, datasets, use_csr=True)

            change(uri, nodes)
            refresh_ms, _ = timed(engine.refresh)
            stats = engine.csr_stats()["KNOWS"]

            latest = {"Person": person, "KNOWS": lance.dataset(uri)}
            _, fresh = timed(lambda: CypherEngine(CONFIG, latest, use_csr=True))
            out = {
                "edges": edges,
                "scenario": name,
                "fragments": len(lance.dataset(uri).get_fragments()),
                "update": stats["update"],
                "delta_edges": stats["delta_edges"],
                "tombstones": stats["tombstones"],
                "refresh_ms": refresh_ms,
                "delta_apply_ms": stats["build_ms"],
                "rebuild_ms": fresh.csr_stats()["KNOWS"]["build_ms"],
            }
            for qname, query in queries.items():
                assert rows(engine.execute(query)) == rows(fresh.execute(query)), qname
                out[f"{qname}_delta_ms"] = warm_ms(engine, query, args.repeats)
                out[f"{qname}_clean_ms"] = warm_ms(fresh, query, args.repeats)
            print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
