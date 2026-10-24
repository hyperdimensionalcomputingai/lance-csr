# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Incremental CSR maintenance: edge commits applied as deltas and tombstones.

After each change and ``refresh()``, the CSR engine must return exactly what a
fresh join-path engine and a freshly built CSR engine return on the new data.
"""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .with_node_label("Person", "id")
    .with_relationship("KNOWS", "src", "dst")
    .build()
)
N = 40
PEOPLE = pa.table({"id": list(range(N)), "name": [f"p{i}" for i in range(N)]})

QUERIES = [
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id",
    "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.id, b.name",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 3 RETURN b.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN count(*) AS n",
]


def edges(pairs, start=0):
    src, dst = zip(*pairs) if pairs else ((), ())
    return pa.table(
        {
            "src": pa.array(src, pa.int64()),
            "dst": pa.array(dst, pa.int64()),
            # Unique per row, so a predicate can delete one of two identical edges.
            "since": list(range(start, start + len(pairs))),
        }
    )


# 200 edges in 5 fragments, including duplicates (i -> i+1 appears twice for i < 10).
BASE = [(i % N, (i * 7 + 1) % N) for i in range(190)] + [(i, i + 1) for i in range(10)]


def rows(table):
    return sorted(table.to_pylist(), key=lambda r: [str(v) for v in r.values()])


@pytest.fixture
def graph(tmp_path):
    def make(**write_kwargs):
        person = lance.write_dataset(PEOPLE, tmp_path / "person.lance")
        knows = lance.write_dataset(
            edges(BASE), tmp_path / "knows.lance", max_rows_per_file=40, **write_kwargs
        )
        engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows}, use_csr=True)
        return engine, tmp_path / "knows.lance"

    return make


def check(engine, knows_uri):
    """CSR engine (after refresh) == fresh join engine == fresh CSR engine."""
    datasets = {
        "Person": lance.dataset(knows_uri.parent / "person.lance"),
        "KNOWS": lance.dataset(knows_uri),
    }
    join = CypherEngine(CONFIG, datasets)
    fresh = CypherEngine(CONFIG, datasets, use_csr=True)
    for query in QUERIES:
        expected = rows(join.execute(query))
        assert rows(engine.execute(query)) == expected, query
        assert rows(fresh.execute(query)) == expected, query


def stats(engine):
    return engine.csr_stats()["KNOWS"]


def test_initial_build_reports_build(graph):
    engine, _ = graph()
    s = stats(engine)
    assert (s["update"], s["delta_edges"], s["tombstones"]) == ("build", 0, 0)


def test_append_applied_as_delta(graph):
    engine, uri = graph()
    lance.write_dataset(edges([(1, 2), (39, 0), (5, 5)], 1000), uri, mode="append")
    engine.refresh()
    s = stats(engine)
    assert (s["update"], s["delta_edges"], s["tombstones"]) == ("delta", 3, 0)
    assert s["edges"] == 203
    check(engine, uri)


def test_delete_applied_as_tombstones(graph):
    engine, uri = graph()
    lance.dataset(uri).delete("since % 9 = 0")
    engine.refresh()
    s = stats(engine)
    assert s["update"] == "delta" and s["tombstones"] == 23 and s["delta_edges"] == 0
    check(engine, uri)


def test_delete_one_of_duplicate_edges(graph):
    engine, uri = graph()
    # since 190..199 are the duplicated i -> i+1 edges; delete one copy of 2 -> 3.
    lance.dataset(uri).delete("since = 192")
    engine.refresh()
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 2 RETURN b.id"
    expected = sorted(d for s, d in BASE if s == 2)
    expected.remove(3)
    assert sorted(engine.execute(query).column(0).to_pylist()) == expected
    check(engine, uri)


def test_delete_whole_fragment(graph):
    engine, uri = graph()
    lance.dataset(uri).delete("since >= 40 AND since < 80")
    engine.refresh()
    assert stats(engine)["tombstones"] == 40
    check(engine, uri)


def test_update_endpoint_values(graph):
    engine, uri = graph()
    lance.dataset(uri).update({"dst": "0"}, where="since < 15")
    engine.refresh()
    assert stats(engine)["update"] == "delta"
    check(engine, uri)


def test_non_endpoint_column_change_reuses_csr(graph):
    engine, uri = graph()
    before = stats(engine)
    lance.dataset(uri).add_columns({"weight": "since * 2"})
    engine.refresh()
    after = stats(engine)
    assert after["update"] == "reused" and after["edges"] == before["edges"]
    assert after["version"] > before["version"]
    check(engine, uri)


def test_sequential_deltas_accumulate(graph):
    engine, uri = graph()
    lance.write_dataset(edges([(7, 8), (7, 9)], 2000), uri, mode="append")
    engine.refresh()
    lance.dataset(uri).delete("since = 2000 OR since = 0")
    engine.refresh()
    lance.write_dataset(edges([(7, 8)], 3000), uri, mode="append")
    engine.refresh()
    s = stats(engine)
    assert s["update"] == "delta" and s["delta_edges"] == 3 and s["tombstones"] == 2
    check(engine, uri)


def test_compaction_keeps_results(graph):
    engine, uri = graph()
    ds = lance.dataset(uri)
    ds.delete("since < 30")
    ds.optimize.compact_files(target_rows_per_fragment=1000)
    engine.refresh()
    check(engine, uri)


def test_overwrite_triggers_rebuild(graph):
    engine, uri = graph()
    lance.write_dataset(edges([(1, 2), (2, 3)]), uri, mode="overwrite")
    engine.refresh()
    s = stats(engine)
    assert (s["update"], s["edges"], s["delta_edges"]) == ("rebuild", 2, 0)
    check(engine, uri)


def test_large_delta_triggers_rebuild(graph):
    engine, uri = graph()
    many = [(i % N, (i + 3) % N) for i in range(100)]  # 50% of the base
    lance.write_dataset(edges(many, 5000), uri, mode="append")
    engine.refresh()
    s = stats(engine)
    assert (s["update"], s["edges"], s["delta_edges"]) == ("rebuild", 300, 0)
    check(engine, uri)


def test_stable_row_ids_with_deletes_and_appends(graph):
    engine, uri = graph(enable_stable_row_ids=True)
    ds = lance.dataset(uri)
    ds.delete("since % 5 = 1")
    lance.write_dataset(edges([(0, 39)], 9000), uri, mode="append")
    engine.refresh()
    assert stats(engine)["update"] == "delta"
    check(engine, uri)
