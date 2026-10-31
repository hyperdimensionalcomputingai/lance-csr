# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Chains of WITH clauses, WITH DISTINCT, and whole nodes carried through WITH.

Uses the graph from ``test_engine_csr.py``. Its valid edges are 1->2 (twice), 1->3,
2->3, 3->3 and 5->1. Expected answers are written out by hand.
"""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine
from test_engine_csr import CONFIG, KNOWS, PEOPLE

CASES = [
    # Group per node, then filter the groups in a second WITH.
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b, count(*) AS n "
        "WITH b, n WHERE n > 1 RETURN b.id, n",
        [(2, 2), (3, 3)],
    ),
    # A whole node through WITH, then matched on again.
    (
        "MATCH (a:Person {id: 1})-[:KNOWS]->(b:Person) WITH b "
        "MATCH (b)-[:KNOWS]->(c:Person) RETURN b.id, c.id",
        [(2, 3), (2, 3), (3, 3)],
    ),
    # WITH DISTINCT removes the duplicate paths to b before expanding again.
    (
        "MATCH (a:Person {id: 1})-[:KNOWS*1..2]->(b:Person) WITH DISTINCT b "
        "MATCH (b)-[:KNOWS]->(c:Person) RETURN b.id, c.id",
        [(2, 3), (3, 3)],
    ),
    (
        "MATCH (a:Person {id: 1})-[:KNOWS]->(b:Person) WITH DISTINCT b "
        "MATCH (b)-[:KNOWS]->(c:Person) WITH c, count(*) AS n RETURN c.id, n",
        [(3, 2)],
    ),
    (
        "MATCH (a:Person) WITH a WHERE a.age > 20 WITH a.id AS id "
        "WITH id WHERE id < 5 RETURN id",
        [(2,), (3,), (4,)],
    ),
    # WHERE right after WITH filters before the next MATCH.
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH DISTINCT b WHERE b.age > 30 "
        "MATCH (b)-[:KNOWS]->(c:Person) RETURN b.id, c.id",
        [(3, 3)],
    ),
    # WITH drops `a`; the second MATCH binds a new `a` (all edges into 1, 2 or 3).
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH DISTINCT b "
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.id, b.id",
        [(1, 2), (1, 2), (1, 3), (2, 3), (3, 3), (5, 1)],
    ),
    # A camelCase scalar carried through a second WITH stays reachable.
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH a, count(b) AS bCount "
        "WITH a, bCount RETURN a.id, bCount",
        [(1, 3), (2, 1), (3, 1), (5, 1)],
    ),
    # Renaming a node keeps its properties reachable under the new name.
    (
        "MATCH (a:Person {id: 1})-[:KNOWS]->(b:Person) WITH b AS x RETURN x.name",
        [("b",), ("b",), ("c",)],
    ),
    # An unaliased property stays addressable as `b.name` after WITH.
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b.name, a.id AS aid "
        "WHERE aid = 1 RETURN b.name, aid",
        [("b", 1), ("b", 1), ("c", 1)],
    ),
]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), CASES)
def test_with_chain(use_csr, query, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    got = sorted(tuple(r.values()) for r in engine.execute(query).to_pylist())
    assert got == sorted(expected)


def test_camel_case_aggregate_alias_can_be_ordered_by():
    # Failed to plan before ("No field named edgecount"): aggregate aliases were kept as
    # written while the follow-up projection looked them up lowercased.
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS})
    got = engine.execute(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) "
        "RETURN b.id AS id, count(*) AS edgeCount ORDER BY edgeCount DESC"
    )
    assert got.to_pylist() == [
        {"id": 3, "edgecount": 3},
        {"id": 2, "edgecount": 2},
        {"id": 1, "edgecount": 1},
    ]


def test_grouped_node_columns_fetched_after_grouping():
    # With a node index (unique integer ids), `WITH b, ...` groups by b's id and looks
    # its other columns up afterwards; without one it groups by all of b's columns.
    query = (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b, count(*) AS n "
        "RETURN b.id AS id, b.name AS name, b.age AS age, n"
    )
    datasets = {"Person": PEOPLE, "KNOWS": KNOWS}
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    join = CypherEngine(CONFIG, datasets)
    assert "NodeLookupExec: key=b__id" in csr.explain(query)
    assert "NodeLookup" not in join.explain(query)
    by_id = lambda t: sorted(t.to_pylist(), key=lambda r: r["id"])  # noqa: E731
    assert by_id(csr.execute(query)) == by_id(join.execute(query))


@pytest.mark.parametrize("use_csr", [False, True])
def test_unmatched_optional_node_groups_with_null_columns(use_csr):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    got = engine.execute(
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) "
        "WITH b, count(*) AS n RETURN b.id AS id, b.name AS name, n"
    ).to_pylist()
    rows = sorted((r["id"] or 0, r["name"], r["n"]) for r in got)
    # Persons 4 and 6 match nothing: one null b for both, with null columns.
    assert rows == [(0, None, 2), (1, "a", 1), (2, "b", 2), (3, "c", 3)]


@pytest.mark.parametrize("lazy", [False, True])
def test_node_lookup_over_many_groups(tmp_path, lazy):
    # More groups than one batch. From Lance, with deleted rows, the plain grouping
    # must still match the join path.
    n = 20000
    people = pa.table(
        {
            "id": list(range(n)),
            "name": [None if i % 11 == 0 else f"p{i}" for i in range(n)],
            "age": [i % 90 for i in range(n)],
        }
    )
    knows = pa.table(
        {
            "src": pa.array([i % n for i in range(2 * n)], pa.int64()),
            "dst": pa.array([(i * 7 + 3) % n for i in range(2 * n)], pa.int64()),
        }
    )
    if lazy:
        lance.write_dataset(people, tmp_path / "p.lance", max_rows_per_file=3000)
        lance.dataset(tmp_path / "p.lance").delete("id % 13 = 0")
        people = lance.dataset(tmp_path / "p.lance")
    datasets = {"Person": people, "KNOWS": knows}
    query = (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b, count(*) AS n "
        "RETURN b.id AS id, b.name AS name, b.age AS age, n"
    )
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    # Only in-memory node tables are looked up; Lance ones keep the plain grouping.
    assert ("NodeLookupExec" in csr.explain(query)) is not lazy
    by_id = lambda t: sorted(t.to_pylist(), key=lambda r: r["id"])  # noqa: E731
    assert by_id(csr.execute(query)) == by_id(
        CypherEngine(CONFIG, datasets).execute(query)
    )
