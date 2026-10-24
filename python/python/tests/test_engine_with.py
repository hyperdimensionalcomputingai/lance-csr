# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Chains of WITH clauses, WITH DISTINCT, and whole nodes carried through WITH.

Uses the graph from ``test_engine_csr.py``. Its valid edges are 1->2 (twice), 1->3,
2->3, 3->3 and 5->1. Expected answers are written out by hand.
"""

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
