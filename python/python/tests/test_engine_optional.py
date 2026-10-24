# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""OPTIONAL MATCH, and a WHERE between two reading clauses.

Uses the graph from ``test_engine_csr.py``: people 1-6 aged 20-70, with valid edges
1->2 (twice), 1->3, 2->3, 3->3 and 5->1. Expected answers are written out by hand.
"""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig
from test_engine_csr import CONFIG, KNOWS, PEOPLE

N = None
CASES = [
    # People without a match keep their row, with nulls.
    (
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.id, b.id",
        [(1, 2), (1, 2), (1, 3), (2, 3), (3, 3), (4, N), (5, 1), (6, N)],
    ),
    # The WHERE filters inside the optional part: a=5 keeps its row, with a null.
    (
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) WHERE b.age > 30 "
        "RETURN a.id, b.id",
        [(1, 3), (2, 3), (3, 3), (4, N), (5, N), (6, N)],
    ),
    (
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) "
        "RETURN a.id, count(b.id) AS n",
        [(1, 3), (2, 1), (3, 1), (4, 0), (5, 1), (6, 0)],
    ),
    # A WHERE between MATCH and OPTIONAL MATCH filters the MATCH.
    (
        "MATCH (a:Person) WHERE a.id < 3 OPTIONAL MATCH (a)<-[:KNOWS]-(c:Person) "
        "RETURN a.id, c.id",
        [(1, 5), (2, 1), (2, 1)],
    ),
    (
        "MATCH (a:Person) WHERE a.id > 4 "
        "OPTIONAL MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
        "RETURN a.id, c.id",
        [(5, 2), (5, 2), (5, 3), (6, N)],
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH DISTINCT b "
        "OPTIONAL MATCH (b)-[:KNOWS]->(c:Person {id: 1}) RETURN b.id, c.id",
        [(1, N), (2, N), (3, N)],
    ),
]


def rows(table):
    return sorted(
        (tuple(r.values()) for r in table.to_pylist()),
        key=lambda t: [(v is None, v) for v in t],
    )


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), CASES)
def test_optional_match(use_csr, query, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    got = engine.execute(query)
    assert rows(got) == sorted(expected, key=lambda t: [(v is None, v) for v in t])


def test_optional_side_expands_on_csr_and_left_joins():
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=True)
    plan = engine.explain(
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.id, b.id"
    )
    physical = plan[plan.index("physical_plan") :]
    assert "CsrExpandExec" in physical and "join_type=Left" in physical


def test_optional_match_needs_a_bound_variable():
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS})
    with pytest.raises(Exception, match="OPTIONAL MATCH"):
        engine.execute("MATCH (a:Person) OPTIONAL MATCH (x:Person) RETURN a.id, x.id")


def test_continued_path_expands_from_the_cheaper_bound_end():
    # People create 4 posts each, forums hold 1: the optional path, written from
    # `friend`, should expand from `forum` and check the creator at the end.
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("Post", "id")
        .node_table("Forum", "id")
        .rel_table("postHasCreator", "Post", "Person", "src", "dst")
        .rel_table("containerOf", "Forum", "Post", "src", "dst")
        .build()
    )
    i64 = pa.int64()
    datasets = {
        "Person": pa.table({"id": pa.array([1, 2], i64)}),
        "Post": pa.table({"id": pa.array(range(100, 108), i64)}),
        "Forum": pa.table({"id": pa.array([10, 11], i64)}),
        "postHasCreator": pa.table(
            {
                "src": pa.array(range(100, 108), i64),
                "dst": pa.array([1] * 4 + [2] * 4, i64),
            }
        ),
        "containerOf": pa.table(
            {"src": pa.array([10, 11], i64), "dst": pa.array([100, 106], i64)}
        ),
    }
    query = (
        "MATCH (friend:Person), (forum:Forum) WITH friend, forum "
        "OPTIONAL MATCH (friend)<-[:postHasCreator]-(post:Post)"
        "<-[:containerOf]-(forum) RETURN friend.id, forum.id, count(post.id) AS n"
    )
    for use_csr in (False, True):
        engine = CypherEngine(config, datasets, use_csr=use_csr)
        assert rows(engine.execute(query)) == [
            (1, 10, 1),
            (1, 11, 0),
            (2, 10, 0),
            (2, 11, 1),
        ]
    plan = engine.explain(query)
    assert "key=forum__id" in plan[plan.index("physical_plan") :]
