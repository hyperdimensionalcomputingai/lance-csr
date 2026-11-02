# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Stored CSR files (`persist_csr=True`).

An engine that loads a stored CSR must return exactly what a join-path engine
returns, after appends, deletes, compaction and cleanup of old versions.
"""

import os
import sys
from datetime import timedelta

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)
N = 40
PEOPLE = pa.table({"id": list(range(N)), "name": [f"p{i}" for i in range(N)]})
QUERIES = [
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id",
    "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.id, b.name",
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN count(*) AS n",
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.id, b.id, r.since",
]


def edges(pairs, start=0):
    src, dst = zip(*pairs)
    return pa.table(
        {
            "src": pa.array(src, pa.int64()),
            "dst": pa.array(dst, pa.int64()),
            "since": list(range(start, start + len(pairs))),
        }
    )


# 200 edges in 5 fragments, with duplicates.
BASE = [(i % N, (i * 7 + 1) % N) for i in range(190)] + [(i, i + 1) for i in range(10)]


def rows(table):
    return sorted(table.to_pylist(), key=lambda r: [str(v) for v in r.values()])


def stored(uri):
    folder = uri / "_lance_csr"
    return sorted(os.listdir(folder)) if folder.exists() else []


@pytest.fixture
def knows(tmp_path):
    uri = tmp_path / "knows.lance"
    lance.write_dataset(edges(BASE), uri, max_rows_per_file=40)
    return uri


def engine(uri, **kwargs):
    datasets = {"Person": PEOPLE, "KNOWS": lance.dataset(uri)}
    return CypherEngine(CONFIG, datasets, use_csr=True, persist_csr=True, **kwargs)


def check(persisted, uri):
    join = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": lance.dataset(uri)})
    for query in QUERIES:
        assert rows(persisted.execute(query)) == rows(join.execute(query)), query


def test_stored_csr_is_loaded(knows):
    first = engine(knows)
    assert first.csr_stats()["KNOWS"]["update"] == "build"
    assert stored(knows) == ["1.arrow"]
    second = engine(knows)
    s = second.csr_stats()["KNOWS"]
    assert (s["update"], s["base_version"], s["edges"]) == ("loaded", 1, len(BASE))
    check(second, knows)


def test_stored_csr_survives_compaction_and_cleanup(knows):
    engine(knows)
    lance.write_dataset(edges([(3, 4), (4, 3)], 4000), knows, mode="append")
    lance.write_dataset(edges([(5, 6), (6, 5)], 4002), knows, mode="append")
    lance.dataset(knows).optimize.compact_files(target_rows_per_fragment=40)
    lance.dataset(knows).delete("since = 0 OR since = 4003")
    # Version 1, the stored file's version, is gone after this.
    lance.dataset(knows).cleanup_old_versions(
        older_than=timedelta(0), delete_unverified=True
    )
    assert stored(knows) == ["1.arrow"]
    loaded = engine(knows)
    s = loaded.csr_stats()["KNOWS"]
    assert (s["update"], s["base_version"]) == ("loaded", 1)
    assert s["delta_edges"] > 0 and s["tombstones"] > 0
    check(loaded, knows)


def test_rebuild_stores_a_new_base(knows):
    engine(knows)
    # More than 25% of the base appended: the next start rebuilds and stores again.
    lance.write_dataset(edges(BASE[:60], 5000), knows, mode="append")
    rebuilt = engine(knows)
    assert rebuilt.csr_stats()["KNOWS"]["update"] == "build"
    assert stored(knows) == ["2.arrow"]
    check(rebuilt, knows)
    assert engine(knows).csr_stats()["KNOWS"]["update"] == "loaded"


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0,
    reason="needs POSIX permissions",
)
def test_read_only_dataset_still_builds(knows):
    os.chmod(knows, 0o555)
    try:
        read_only = engine(knows)
    finally:
        os.chmod(knows, 0o755)
    assert read_only.csr_stats()["KNOWS"]["update"] == "build"
    assert stored(knows) == []
    check(read_only, knows)


def test_delete_stored_csr(knows):
    persisted = engine(knows)
    assert stored(knows) == ["1.arrow"]
    assert persisted.delete_stored_csr() == 1
    assert not (knows / "_lance_csr").exists()
    check(persisted, knows)  # the in-memory CSR stays
    assert persisted.delete_stored_csr() == 0
    in_memory = CypherEngine(
        CONFIG, {"Person": PEOPLE, "KNOWS": lance.dataset(knows)}, use_csr=True
    )
    assert in_memory.csr_stats()["KNOWS"]["update"] == "build"
    assert stored(knows) == []


def test_pyarrow_input_is_not_stored():
    datasets = {"Person": PEOPLE, "KNOWS": edges(BASE)}
    e = CypherEngine(CONFIG, datasets, use_csr=True, persist_csr=True)
    assert e.csr_stats()["KNOWS"]["update"] == "build"


def test_persist_csr_needs_use_csr(knows):
    with pytest.raises(ValueError, match="persist_csr requires use_csr"):
        CypherEngine(CONFIG, {"KNOWS": lance.dataset(knows)}, persist_csr=True)
