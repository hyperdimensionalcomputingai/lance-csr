"""
Disk-backed CypherEngine: pinned snapshots, checkout_latest(), and streamed results.

Requirements: an installed lance-csr wheel (or `maturin develop` in python/) and pylance,
which writes and opens the datasets.

Run:
  python examples/lance_snapshots.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import lance
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

ROWS = 50_000


def people(start: int, stop: int) -> pa.Table:
    ids = list(range(start, stop))
    return pa.table({"id": ids, "name": [f"p{i}" for i in ids]})


def main() -> None:
    root = Path(tempfile.mkdtemp())
    person_uri = root / "person.lance"
    lance.write_dataset(people(0, ROWS), person_uri, max_rows_per_file=10_000)
    lance.write_dataset(
        pa.table({"src": list(range(ROWS - 1)), "dst": list(range(1, ROWS))}),
        root / "knows.lance",
    )

    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": lance.dataset(person_uri),
            "KNOWS": lance.dataset(root / "knows.lance"),
        },
        # Optional: choose this target after measuring your own queries; checkout_latest() keeps it.
        target_partitions=2,
    )
    count = "MATCH (p:Person) RETURN count(*) AS n"

    print("rows:", engine.execute(count).column(0)[0].as_py())
    print(
        "friend of p49998:",
        engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 49998 RETURN b.name"
        ).to_pylist(),
    )

    # Writes are not visible until checkout_latest(): the engine is pinned to version 1.
    lance.write_dataset(people(ROWS, ROWS + 10), person_uri, mode="append")
    lance.dataset(person_uri).add_columns({"age": "id % 100"})  # schema change
    print("after writes, before checkout_latest():", engine.execute(count).column(0)[0].as_py())
    engine.checkout_latest()
    print(
        "after checkout_latest():",
        engine.execute(count).column(0)[0].as_py(),
        engine.dataset_versions(),
    )
    print(
        "new column:",
        engine.execute("MATCH (p:Person) WHERE p.id = 50009 RETURN p.age").to_pylist(),
    )

    # Stream a large projection batch by batch instead of building one table.
    total = 0
    for batch in engine.execute_batches("MATCH (p:Person) RETURN p.id"):
        total += batch.num_rows
    print("streamed rows:", total)


if __name__ == "__main__":
    main()
