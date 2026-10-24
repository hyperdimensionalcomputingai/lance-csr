# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Engine construction, first-query, warm-query and refresh timings.

Runs against either the upstream build (``--module lance_graph``) or this fork
(``--module lance_csr``) on the same on-disk Lance data, so the two can be compared
in separate virtual environments. Prints one JSON object per measured case.

The graph is a chain: node labels L0..Lk, and relationship Ri from Li to Li+1.
Rows per table default to 10,000 because the upstream bridge truncates Lance inputs
at 10,000 rows; larger tables would compare different answers.

    python benchmarks/engine_cache.py --module lance_csr --data /tmp/bench --tables 20
"""

import argparse
import importlib
import json
import statistics
import time
from pathlib import Path

import lance
import pyarrow as pa


def write_data(root: Path, labels: int, rows: int) -> None:
    if (root / "_done").exists():
        return
    root.mkdir(parents=True, exist_ok=True)
    ids = pa.array(range(rows), pa.int64())
    for i in range(labels):
        nodes = pa.table({"id": ids, "name": pa.array([f"n{j}" for j in range(rows)])})
        lance.write_dataset(nodes, root / f"L{i}.lance", mode="overwrite")
    for i in range(labels - 1):
        # Each node links to the node with the next id; deterministic, no RNG needed.
        dst = pa.array([(j + 1) % rows for j in range(rows)], pa.int64())
        edges = pa.table({"src": ids, "dst": dst})
        lance.write_dataset(edges, root / f"R{i}.lance", mode="overwrite")
    (root / "_done").touch()


def timed(fn):
    start = time.perf_counter()
    out = fn()
    return time.perf_counter() - start, out


def summary(samples):
    ordered = sorted(samples)
    p95 = ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]
    return {"median_ms": statistics.median(ordered) * 1e3, "p95_ms": p95 * 1e3}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module", required=True, choices=["lance_graph", "lance_csr"])
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--tables", type=int, default=20, help="total tables (>= 5)")
    parser.add_argument(
        "--inputs",
        choices=["lance", "arrow"],
        default="lance",
        help="pass Lance datasets, or tables materialized up front with to_table()",
    )
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--repeats", type=int, default=30)
    args = parser.parse_args()
    assert args.tables >= 5, "the chain query needs labels L0..L2"

    mod = importlib.import_module(args.module)
    labels = (args.tables + 1) // 2
    root = args.data / f"t{args.tables}_r{args.rows}"
    write_data(root, labels, args.rows)

    builder = mod.GraphConfig.builder()
    for i in range(labels):
        builder = builder.node_table(f"L{i}", "id")
    for i in range(labels - 1):
        builder = builder.rel_table(f"R{i}", f"L{i}", f"L{i + 1}", "src", "dst")
    config = builder.build()

    def open_datasets():
        names = [f"L{i}" for i in range(labels)] + [f"R{i}" for i in range(labels - 1)]
        sets = {n: lance.dataset(root / f"{n}.lance") for n in names}
        if args.inputs == "arrow":
            return {n: ds.to_table() for n, ds in sets.items()}
        return sets

    query = (
        "MATCH (a:L0)-[:R0]->(b:L1)-[:R1]->(c:L2) WHERE a.id < 100 RETURN c.id, c.name"
    )
    expected_rows = 100

    def emit(case, **values):
        record = {"module": args.module, "inputs": args.inputs, "tables": args.tables}
        record["rows"] = args.rows
        print(json.dumps({**record, "case": case, **values}), flush=True)

    datasets = open_datasets()
    t_build, engine = timed(lambda: mod.CypherEngine(config, datasets))
    t_first, out = timed(lambda: engine.execute(query))
    assert out.num_rows == expected_rows, out.num_rows
    emit("construct", ms=t_build * 1e3)
    emit("first_query", ms=t_first * 1e3)

    warm = []
    for _ in range(args.repeats):
        t, out = timed(lambda: engine.execute(query))
        assert out.num_rows == expected_rows
        warm.append(t)
    emit("warm_query", **summary(warm))

    # Upstream has no refresh(); its equivalent is building a new engine.
    refresh = []
    for _ in range(min(args.repeats, 10)):
        if hasattr(engine, "refresh"):
            t, _ = timed(engine.refresh)
        else:
            t, engine = timed(lambda: mod.CypherEngine(config, open_datasets()))
        refresh.append(t)
    emit("refresh_unchanged", **summary(refresh))

    # Control: no engine, inputs converted and catalog rebuilt for every query.
    rebuild = []
    for _ in range(min(args.repeats, 10)):
        q = mod.CypherQuery(query).with_config(config)
        t, out = timed(lambda q=q: q.execute(datasets))
        assert out.num_rows == expected_rows
        rebuild.append(t)
    emit("rebuild_per_query", **summary(rebuild))


if __name__ == "__main__":
    main()
