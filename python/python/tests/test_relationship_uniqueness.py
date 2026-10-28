# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Relationship identity: one MATCH binds each relationship at most once (relationship
isomorphism), and a relationship variable written again is that same relationship."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)
PEOPLE = pa.table({"id": [1, 2, 3, 4], "name": ["A", "B", "C", "D"]})
# A->B and C->B: two edges into B.
KNOWS = pa.table({"src": [1, 3], "dst": [2, 2]})
RETURN = " RETURN a.name AS a, c.name AS c ORDER BY a, c"
DISTINCT = [{"a": "A", "c": "C"}, {"a": "C", "c": "A"}]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("MATCH (a:Person)-[r1:KNOWS]->(b:Person)<-[r2:KNOWS]-(c:Person)", DISTINCT),
        ("MATCH (a:Person)-[:KNOWS]->(:Person)<-[:KNOWS]-(c:Person)", DISTINCT),
        ("MATCH (a:Person)-[:KNOWS]->(b:Person), (c:Person)-[:KNOWS]->(b)", DISTINCT),
        ("MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)", DISTINCT),
        (
            "MATCH p = (a:Person)-[r1:KNOWS]->(b:Person)<-[r2:KNOWS]-(c:Person)",
            DISTINCT,
        ),
        # Separate MATCH clauses may bind the same relationship again.
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (c:Person)-[:KNOWS]->(b)",
            [{"a": a, "c": c} for a in "AC" for c in "AC"],
        ),
    ],
)
def test_one_match_binds_each_relationship_once(use_csr, query, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    assert engine.execute(query + RETURN).to_pylist() == expected


# A->B (w 10) and B->C (w 20).
CHAIN = pa.table({"src": [1, 2], "dst": [2, 3], "w": [10, 20]})
SAME = "MATCH (a:Person)-[r:KNOWS]->(b:Person), "
ENDS = " RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d ORDER BY a, c"
AB, BC = ("A", "B"), ("B", "C")


def ends(*rows):
    return [{"a": a, "b": b, "c": c, "d": d} for (a, b), (c, d) in rows]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (SAME + "(c:Person)-[r]->(d:Person)" + ENDS, ends((AB, AB), (BC, BC))),
        (SAME + "(c)-[r:KNOWS]->(d)" + ENDS, ends((AB, AB), (BC, BC))),
        (SAME + "(d:Person)<-[r]-(c:Person)" + ENDS, ends((AB, AB), (BC, BC))),
        (
            SAME + "(c:Person)-[r]-(d:Person)" + ENDS,
            ends((AB, AB), (AB, AB[::-1]), (BC, BC), (BC, BC[::-1])),
        ),
        (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) MATCH (c)-[r]->(d)" + ENDS,
            ends((AB, AB), (BC, BC)),
        ),
        (SAME + "(b)-[r]->(a) RETURN a.name AS a", []),
        (SAME + "(c)-[r {w: 10}]->(d) RETURN a.name AS a", [{"a": "A"}]),
        (SAME + "(c)-[r]->(d) RETURN r.w AS w ORDER BY w", [{"w": 10}, {"w": 20}]),
    ],
)
def test_reused_relationship_variable_is_the_same_edge(use_csr, query, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": CHAIN}, use_csr=use_csr)
    assert engine.execute(query).to_pylist() == expected


def test_reused_relationship_variable_rejects_variable_length():
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": CHAIN})
    with pytest.raises(Exception, match="variable-length segment"):
        engine.execute(SAME + "(c)-[r*1..2]->(d) RETURN c")
