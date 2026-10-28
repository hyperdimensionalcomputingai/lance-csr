# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""One MATCH binds each relationship at most once (relationship isomorphism)."""

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
