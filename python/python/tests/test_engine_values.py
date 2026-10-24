# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Nodes as values, lists of nodes, UNWIND, parameters and inferred labels.

The constructs LDBC's official IC queries use: `a = b` on nodes, `x IN list`, `collect(node)`
then `UNWIND`, inline properties that read a variable, chained comparisons, unlabeled
nodes whose label follows from a relationship's endpoints. Answers are written out by hand.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


def edges(pairs):
    src, dst = zip(*pairs)
    return pa.table(
        {"src": pa.array(src, pa.int64()), "dst": pa.array(dst, pa.int64())}
    )


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


TABLES = {
    "Person": pa.table({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]}),
    "City": pa.table({"id": [100, 101], "name": ["X", "Y"]}),
    "Post": pa.table(
        {
            "id": [10, 11, 12],
            "created": pa.array(
                [utc(2020, 1, 1), utc(2020, 6, 1), utc(2021, 1, 1)],
                pa.timestamp("us", "UTC"),
            ),
        }
    ),
    "KNOWS": edges([(1, 2), (2, 3), (3, 4), (1, 3)]),
    "LIVES_IN": edges([(1, 100), (2, 100), (3, 101), (4, 101)]),
    "HAS_CREATOR": edges([(10, 2), (11, 3), (12, 3)]),
}
CONFIG = (
    GraphConfig.builder()
    .with_node_label("Person", "id")
    .with_node_label("City", "id")
    .with_node_label("Post", "id")
    .with_relationship("KNOWS", "src", "dst", "Person", "Person")
    .with_relationship("LIVES_IN", "src", "dst", "Person", "City")
    .with_relationship("HAS_CREATOR", "src", "dst", "Post", "Person")
    .build()
)

CASES = [
    # A node compared with a node compares ids.
    (
        "MATCH (a:Person {id: 1})-[:KNOWS*1..2]->(b) WHERE NOT a = b RETURN DISTINCT b.id",
        [(2,), (3,), (4,)],
    ),
    # `p` is a Post: HAS_CREATOR goes from Post to Person.
    ("MATCH (a:Person {id: 2})<-[:HAS_CREATOR]-(p) RETURN p.id", [(10,)]),
    (
        "MATCH (x:City {name: 'X'}), (p:Person)-[:LIVES_IN]->(c) WHERE c IN [x] RETURN p.id",
        [(1,), (2,)],
    ),
    # A list of nodes, and a pattern after WITH that starts at a new node.
    (
        "MATCH (:City {name: 'Y'})<-[:LIVES_IN]-(p:Person) WITH collect(p) AS ys "
        "MATCH (a:Person)-[:KNOWS]->(b) WHERE b IN ys RETURN a.id, b.id",
        [(1, 3), (2, 3), (3, 4)],
    ),
    (
        "MATCH (:Person {id: 1})-[:KNOWS]->(b) WITH collect(b) AS bs "
        "UNWIND bs AS f RETURN f.id, f.name",
        [(2, "b"), (3, "c")],
    ),
    (
        "MATCH (:Person {id: 1})-[:KNOWS]->(b) WITH collect(DISTINCT b) AS bs "
        "UNWIND bs AS f MATCH (f)<-[:HAS_CREATOR]-(post) RETURN f.id, post.id",
        [(2, 10), (3, 11), (3, 12)],
    ),
    # An inline property that reads a variable, on an unnamed node.
    (
        "MATCH (c:City {name: 'X'}) WITH c.id AS cid "
        "MATCH (p:Person)-[:LIVES_IN]->(:City {id: cid}) RETURN p.id",
        [(1,), (2,)],
    ),
    ("MATCH (a:Person)-[:KNOWS*1..2]->(b) RETURN count(DISTINCT b) AS n", [(3,)]),
    # A scalar from WITH survives a variable-length path.
    (
        "MATCH (c:City {name: 'Y'}) WITH c.id AS cid "
        "MATCH (:Person {id: 1})-[:KNOWS*1..2]->(b)-[:LIVES_IN]->(c2) WHERE c2.id = cid "
        "RETURN DISTINCT b.id",
        [(3,), (4,)],
    ),
    # collect() of nothing is an empty list, not null.
    (
        "MATCH (a:Person) OPTIONAL MATCH (a)<-[:HAS_CREATOR]-(p:Post) "
        "WITH a, collect(p.id) AS posts RETURN a.id, size(posts)",
        [(1, 0), (2, 1), (3, 2), (4, 0)],
    ),
    # An OPTIONAL MATCH whose WHERE reads a list carried by WITH (LDBC IC5's shape).
    (
        "MATCH (:Person)-[:KNOWS]->(a:Person)-[:LIVES_IN]->(c:City) WITH DISTINCT c, a "
        "WITH c, collect(a) AS known "
        "OPTIONAL MATCH (c)<-[:LIVES_IN]-(x:Person)<-[:HAS_CREATOR]-(post:Post) "
        "WHERE x IN known RETURN c.name, count(post) AS n",
        [("X", 1), ("Y", 2)],
    ),
    # Mixed element types become strings, as Arrow lists hold one type.
    ("MATCH (p:Person {id: 1}) RETURN [p.name, p.id] AS l", [(["a", "1"],)]),
]


def rows(table):
    return sorted(tuple(r.values()) for r in table.to_pylist())


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), CASES)
def test_values(use_csr, query, expected):
    engine = CypherEngine(CONFIG, TABLES, use_csr=use_csr)
    assert rows(engine.execute(query)) == expected


def test_parameters_and_chained_comparison():
    engine = CypherEngine(CONFIG, TABLES)
    query = "MATCH (p:Post) WHERE $start <= p.created < $end RETURN p.id"
    params = {"start": utc(2020, 3, 1), "end": utc(2021, 1, 1)}
    assert rows(engine.execute(query, params)) == [(11,)]
    assert rows(
        engine.execute("MATCH (p:Person {id: $id}) RETURN p.name", {"id": 3})
    ) == [("c",)]
