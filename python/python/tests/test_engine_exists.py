# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""EXISTS subqueries, pattern predicates, and the date functions.

Uses the graph from ``test_engine_csr.py``: people 1-6 aged 20, 30, ..., 70, with valid
edges 1->2 (twice), 1->3, 2->3, 3->3 and 5->1 (4->99 points at a missing person).
"""

import datetime

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig
from test_engine_csr import CONFIG, KNOWS, PEOPLE

CASES = [
    (
        "MATCH (a:Person) WHERE EXISTS { MATCH (a)-[:KNOWS]->(:Person) } RETURN a.id",
        [(1,), (2,), (3,), (5,)],
    ),
    # No match means a null mark, so NOT EXISTS keeps the row.
    (
        "MATCH (a:Person) WHERE NOT EXISTS { MATCH (a)-[:KNOWS]->(:Person) } "
        "RETURN a.id",
        [(4,), (6,)],
    ),
    # A bare pattern as a predicate.
    ("MATCH (a:Person) WHERE NOT (a)-[:KNOWS]->(:Person) RETURN a.id", [(4,), (6,)]),
    # The subquery's own WHERE.
    (
        "MATCH (a:Person) WHERE EXISTS { (a)-[:KNOWS]->(b:Person) WHERE b.age > 35 } "
        "RETURN a.id",
        [(1,), (2,), (3,)],
    ),
    # Both ends bound: two-hop pairs with no direct edge.
    (
        "MATCH (a:Person)-[:KNOWS]->(m:Person)-[:KNOWS]->(b:Person) "
        "WHERE NOT (a)-[:KNOWS]->(b) RETURN DISTINCT a.id, b.id",
        [(5, 2), (5, 3)],
    ),
    # EXISTS inside CASE in RETURN (LDBC IC7's isNew).
    (
        "MATCH (a:Person) RETURN a.id, CASE WHEN EXISTS { (a)<-[:KNOWS]-(:Person) } "
        "THEN 'followed' ELSE 'not' END AS f",
        [
            (1, "followed"),
            (2, "followed"),
            (3, "followed"),
            (4, "not"),
            (5, "not"),
            (6, "not"),
        ],
    ),
]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), CASES)
def test_exists(use_csr, query, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    assert (
        sorted(tuple(r.values()) for r in engine.execute(query).to_pylist()) == expected
    )


def test_date_part_and_epoch_millis():
    utc = datetime.timezone.utc
    people = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "birthday": pa.array(
                [datetime.date(1990, 5, 21), datetime.date(1985, 12, 3)]
            ),
            "seen": pa.array(
                [
                    datetime.datetime(1970, 1, 1, 0, 1, tzinfo=utc),
                    datetime.datetime(2000, 1, 1, tzinfo=utc),
                ],
                pa.timestamp("ns", tz="UTC"),
            ),
        }
    )
    config = GraphConfig.builder().with_node_label("Person", "id").build()
    engine = CypherEngine(config, {"Person": people})
    got = engine.execute(
        "MATCH (p:Person) RETURN p.id, date_part('month', p.birthday) AS m, "
        "date_part('day', p.birthday) AS d, to_epoch_ms(p.seen) AS ms"
    )
    assert sorted(tuple(r.values()) for r in got.to_pylist()) == [
        (1, 5, 21, 60_000),
        (2, 12, 3, 946_684_800_000),
    ]
