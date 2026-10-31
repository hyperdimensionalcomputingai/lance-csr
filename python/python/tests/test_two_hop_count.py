# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""`count(*)` of two-hop paths computed from per-node degrees.

Each count must equal the join plan's, which `count(*) + 0` forces, on random graphs
with self-loops, parallel edges, null endpoints and ids, dangling edges, and nulls in
filtered columns.
"""

import random

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("FOLLOWS", "Person", "Person", "src", "dst")
    .rel_table("LIKES", "Person", "Person", "src", "dst")
    .build()
)

CHAIN = "MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)"
QUERIES = [
    f"{CHAIN} RETURN count(*) AS n",
    f"{CHAIN} WHERE b.age < 50 AND c.age > 25 RETURN count(*) AS n",
    f"{CHAIN} WHERE a.age > 30 AND r1.w <> 2 AND r2.w IS NOT NULL RETURN count(*) AS n",
    f"{CHAIN} WHERE a.age < b.age OR b.age IS NULL RETURN count(c) AS n",
    "MATCH (a:Person)-[:FOLLOWS]->(:Person {age: 40})-[:FOLLOWS]->(c:Person) "
    "RETURN count(*) AS n",
    "MATCH (c:Person)<-[:FOLLOWS]-(b:Person)<-[:FOLLOWS]-(a:Person) "
    "WHERE b.age >= 20 RETURN count(*) AS n",
    "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:LIKES]->(c:Person) RETURN count(*) AS n",
    "MATCH (a:Person)-[:FOLLOWS]->(b:Person)<-[:LIKES]-(c:Person) "
    "WHERE c.age > 30 RETURN count(b) AS n",
]


def graph(seed):
    rng = random.Random(seed)
    n = 30
    ids = list(range(n)) + [None]
    people = pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "age": [rng.choice([None, *range(0, 80, 5)]) for _ in ids],
        }
    )

    def edges(count):
        # Few ids, so self-loops and parallel edges are common; 99 dangles.
        ends = [None, 99, *range(n)]
        pairs = [(rng.choice(ends), rng.choice(ends)) for _ in range(count)]
        loops = rng.sample(range(n), 8)
        pairs += [(v, v) for v in loops for _ in range(rng.randint(1, 3))]
        pairs += pairs[: count // 4]
        return pa.table(
            {
                "src": pa.array([p[0] for p in pairs], pa.int64()),
                "dst": pa.array([p[1] for p in pairs], pa.int64()),
                "w": [rng.choice([None, 1, 2, 3]) for _ in pairs],
            }
        )

    return {"Person": people, "FOLLOWS": edges(200), "LIKES": edges(100)}


@pytest.mark.parametrize("seed", range(5))
@pytest.mark.parametrize("use_csr", [False, True])
def test_two_hop_count_matches_join_plan(seed, use_csr):
    engine = CypherEngine(CONFIG, graph(seed), use_csr=use_csr)
    for query in QUERIES:
        assert "__paths" in engine.explain(query), query
        join = query.replace(" AS n", " + 0 AS n")
        assert "__paths" not in engine.explain(join), join
        got, want = engine.execute(query), engine.execute(join)
        assert got.to_pylist() == want.to_pylist(), query
        assert got.schema == want.schema, query
