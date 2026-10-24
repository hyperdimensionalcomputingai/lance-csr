# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Exercise the installed binary wheel outside the source tree, without Rust.

Run with ``python -I`` from a clean virtual environment. Covers both table placements
(Arrow tables in memory, Lance datasets on disk), CSR traversal, shortestPath, and
``refresh()`` picking up a new Lance version.
"""

import importlib.metadata
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import lance
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig, _internal

TWO_HOPS = (
    "MATCH (a:Person {id: 1})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
    "RETURN c.name AS name"
)
SHORTEST = (
    "MATCH (a:Person {id: 1}), (b:Person {id: 4}) "
    "MATCH p = shortestPath((a)-[:KNOWS*1..5]-(b)) RETURN length(p) AS hops"
)


def main():
    assert sys.prefix != sys.base_prefix, "Use a clean virtual environment"
    assert shutil.which("cargo") is None, "Remove Cargo from the smoke-test PATH"
    assert shutil.which("rustc") is None, "Remove rustc from the smoke-test PATH"
    # An installed wheel, not the source tree (uv's environments may layer site-packages
    # outside sys.prefix, so check for site-packages itself).
    extension = Path(_internal.__file__).resolve()
    assert "site-packages" in extension.parts, extension

    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    people = pa.table({"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Dave"]})
    knows = pa.table({"src": [1, 2, 3], "dst": [2, 3, 4]})
    with TemporaryDirectory() as root:
        on_disk = {
            "Person": lance.write_dataset(people, Path(root) / "person.lance"),
            "KNOWS": lance.write_dataset(knows, Path(root) / "knows.lance"),
        }
        for datasets in ({"Person": people, "KNOWS": knows}, on_disk):
            engine = CypherEngine(config, datasets, use_csr=True)
            assert engine.execute(TWO_HOPS).to_pylist() == [{"name": "Carol"}]
            assert engine.execute(SHORTEST).to_pylist() == [{"hops": 3}]
        # The last engine reads Lance datasets: refresh() follows an appended version.
        appended = pa.table({"src": [4], "dst": [1]})
        lance.write_dataset(appended, Path(root) / "knows.lance", mode="append")
        engine.refresh()
        got = engine.execute(
            "MATCH (a:Person {id: 4})-[:KNOWS]->(b:Person) RETURN b.id AS id"
        )
        assert got.to_pylist() == [{"id": 1}]
    print(
        f"Installed lance-csr {importlib.metadata.version('lance-csr')} wheel passed "
        f"in-memory, on-disk, CSR, shortestPath and refresh checks on Python "
        f"{sys.version.split()[0]} without Rust"
    )


if __name__ == "__main__":
    main()
