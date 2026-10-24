# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Arithmetic, CASE and coalesce() in RETURN, WHERE and aggregates.

Uses the graph from ``test_engine_csr.py``: people 1-6 aged 20, 30, ..., 70 and named
a-f, with valid edges 1->2 (twice), 1->3, 2->3, 3->3 and 5->1.
"""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine
from test_engine_csr import CONFIG, KNOWS, PEOPLE

CASES = [
    # `*` `/` `%` bind tighter than `+` `-`; parentheses group.
    (
        "MATCH (a:Person) WHERE a.id < 4 "
        "RETURN a.id, a.id * 10 + 1 AS x, (a.id + 1) * 2 AS y, a.age % 7 AS m",
        [(1, 11, 4, 6), (2, 21, 6, 2), (3, 31, 8, 5)],
    ),
    (
        "MATCH (a:Person) WHERE a.age - 10 > 30 AND a.id * 2 <> 10 RETURN a.id",
        [(4,), (6,)],
    ),
    # Searched CASE: first matching WHEN wins.
    (
        "MATCH (a:Person) WHERE a.id < 4 RETURN a.id, "
        "CASE WHEN a.age > 25 THEN 'old' WHEN a.age > 15 THEN 'mid' ELSE 'young' END",
        [(1, "mid"), (2, "old"), (3, "old")],
    ),
    # Simple CASE, no ELSE: unmatched is null.
    (
        "MATCH (a:Person) WHERE a.id < 4 "
        "RETURN a.id, CASE a.id WHEN 1 THEN 'one' WHEN 2 THEN 'two' END AS c",
        [(1, "one"), (2, "two"), (3, None)],
    ),
    # CASE inside an aggregate (LDBC IC3/IC4 shape).
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) "
        "RETURN a.id, sum(CASE WHEN b.age >= 40 THEN 1 ELSE 0 END) AS old",
        [(1, 1), (2, 1), (3, 1), (5, 0)],
    ),
]


def rows(table):
    return sorted(
        (tuple(r.values()) for r in table.to_pylist()),
        key=lambda t: [(v is None, v) for v in t],
    )


@pytest.mark.parametrize(("query", "expected"), CASES)
def test_expression(query, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS})
    assert rows(engine.execute(query)) == expected


def test_coalesce_takes_the_first_non_null():
    people = pa.table(
        {"id": [1, 2, 3], "name": ["a", None, None], "nick": [None, "bee", None]}
    )
    engine = CypherEngine(CONFIG, {"Person": people, "KNOWS": KNOWS})
    got = engine.execute(
        "MATCH (a:Person) RETURN a.id, coalesce(a.name, a.nick, 'none') AS n"
    )
    assert rows(got) == [(1, "a"), (2, "bee"), (3, "none")]
