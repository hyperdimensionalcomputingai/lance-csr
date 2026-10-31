# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Incremental CSR maintenance: edge commits applied as deltas and tombstones.

After each change and ``refresh()``, the CSR engine must return exactly what a
fresh join-path engine and a freshly built CSR engine return on the new data.
"""

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
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 3 RETURN b.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN count(*) AS n",
    # Relationship columns are taken by edge row, so rows must stay aligned.
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.id, b.id, r.since",
    "MATCH (a:Person)<-[r:KNOWS]-(b:Person) WHERE a.id < 10 RETURN a.id, r.since",
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
    assert s["unindexed_edges"] == 0 and s["base_version"] == s["version"]


def test_append_applied_as_delta(graph):
    engine, uri = graph()
    lance.write_dataset(edges([(1, 2), (39, 0), (5, 5)], 1000), uri, mode="append")
    engine.refresh()
    s = stats(engine)
    assert (s["update"], s["delta_edges"], s["tombstones"]) == ("delta", 3, 0)
    assert s["edges"] == 203 and s["unindexed_edges"] == 3
    assert s["base_version"] < s["version"]
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
    # One tombstone removes an appended edge, so two of three appends are still live.
    assert s["unindexed_edges"] == 2
    check(engine, uri)


def test_optimize_folds_delta_into_base(graph):
    engine, uri = graph()
    lance.write_dataset(edges([(7, 8), (7, 9)], 2000), uri, mode="append")
    lance.dataset(uri).delete("since = 2000 OR since = 0")
    engine.refresh()
    before = stats(engine)
    assert before["update"] == "delta" and before["unindexed_edges"] == 1

    engine.optimize_csr()
    s = stats(engine)
    assert (s["update"], s["delta_edges"], s["tombstones"]) == ("rebuild", 0, 0)
    assert s["unindexed_edges"] == 0 and s["edges"] == before["edges"]
    assert s["base_version"] == s["version"] == before["version"]
    check(engine, uri)

    # Nothing left to fold, so the CSR is kept as it is.
    engine.optimize_csr()
    assert stats(engine) == s


def test_optimize_stays_on_the_pinned_version(graph):
    engine, uri = graph()
    before = rows(engine.execute(QUERIES[0]))
    lance.write_dataset(edges([(7, 8)], 2000), uri, mode="append")
    engine.optimize_csr()
    s = stats(engine)
    # A fresh build has no delta: nothing is rebuilt and the append stays unseen.
    assert s["update"] == "build" and s["version"] == 1
    assert rows(engine.execute(QUERIES[0])) == before


def test_optimize_without_csr_does_nothing():
    datasets = {"Person": PEOPLE, "KNOWS": edges(BASE)}
    engine = CypherEngine(CONFIG, datasets)
    engine.optimize_csr()
    assert engine.csr_stats() == {}
    assert len(engine.execute(QUERIES[0])) == len(BASE)


def test_compaction_keeps_results(graph):
    engine, uri = graph()
    ds = lance.dataset(uri)
    ds.delete("since < 30")
    ds.optimize.compact_files(target_rows_per_fragment=1000)
    engine.refresh()
    check(engine, uri)


def test_deltas_survive_cleanup_of_old_versions(graph):
    # Removed rows are found by row address, not by reading the old version's files.
    engine, uri = graph()
    lance.write_dataset(edges([(3, 4), (4, 3)], 4000), uri, mode="append")
    lance.write_dataset(edges([(5, 6), (6, 5)], 4002), uri, mode="append")
    engine.refresh()
    # Compaction rewrites the two small fragments; cleanup deletes their files.
    lance.dataset(uri).optimize.compact_files(target_rows_per_fragment=40)
    lance.dataset(uri).delete("since = 0 OR since = 4003")
    lance.dataset(uri).cleanup_old_versions(
        older_than=timedelta(0), delete_unverified=True
    )
    engine.refresh()
    s = stats(engine)
    assert (s["update"], s["delta_edges"], s["tombstones"]) == ("delta", 7, 5)
    check(engine, uri)


def test_non_endpoint_column_change_is_visible_through_relationship(graph):
    engine, uri = graph()
    lance.dataset(uri).add_columns({"weight": "since * 2"})
    engine.refresh()
    assert stats(engine)["update"] == "reused"
    got = engine.execute(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since, r.weight"
    ).to_pylist()
    assert len(got) == len(BASE)
    assert all(row["r.weight"] == 2 * row["r.since"] for row in got)


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
    assert s["unindexed_edges"] == 0 and s["base_version"] == s["version"]
    check(engine, uri)


def test_stable_row_ids_with_deletes_and_appends(graph):
    engine, uri = graph(enable_stable_row_ids=True)
    ds = lance.dataset(uri)
    ds.delete("since % 5 = 1")
    lance.write_dataset(edges([(0, 39)], 9000), uri, mode="append")
    engine.refresh()
    assert stats(engine)["update"] == "delta"
    check(engine, uri)
