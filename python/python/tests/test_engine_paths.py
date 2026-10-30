# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""shortestPath / allShortestPaths, path functions, and list values.

Uses the graph from ``test_engine_csr.py``: people 1-6 named a-f, with valid edges 1->2
(twice), 1->3, 2->3, 3->3 and 5->1; 4->99 points at a missing person.
"""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine
from test_engine_csr import CONFIG, KNOWS, PEOPLE

CASES = [
    # Directed: 5->1->3 (2 hops) beats 5->1->2->3.
    (
        "MATCH (a:Person {id: 5}), (b:Person {id: 3}) "
        "MATCH p = shortestPath((a)-[:KNOWS*1..5]->(b)) RETURN length(p), nodes(p)",
        [(2, [5, 1, 3])],
    ),
    # Undirected, every shortest path: the duplicate 1->2 edge gives two paths 2-1-5,
    # as Cypher returns one path per distinct sequence of relationships.
    (
        "MATCH (a:Person {id: 2}), (b:Person {id: 5}) "
        "MATCH p = allShortestPaths((a)-[:KNOWS*1..5]-(b)) RETURN length(p), nodes(p)",
        [(2, [2, 1, 5]), (2, [2, 1, 5])],
    ),
    # No path: MATCH drops the row, OPTIONAL MATCH keeps it with a null path.
    (
        "MATCH (a:Person {id: 1}), (b:Person {id: 6}) "
        "MATCH p = shortestPath((a)-[:KNOWS*1..5]-(b)) RETURN length(p)",
        [],
    ),
    (
        "MATCH (a:Person {id: 1}), (b:Person {id: 6}) "
        "OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*1..5]-(b)) "
        "RETURN coalesce(length(p), -1) AS l",
        [(-1,)],
    ),
    # The hop limit applies: 5 reaches 3 only in 2 hops.
    (
        "MATCH (a:Person {id: 5}), (b:Person {id: 3}) "
        "MATCH p = shortestPath((a)-[:KNOWS*1..1]->(b)) RETURN length(p)",
        [],
    ),
    # Lists: range (inclusive), size, indexing (0-based), list literals, toString.
    (
        "MATCH (a:Person {id: 5}), (b:Person {id: 3}) "
        "MATCH p = shortestPath((a)-[:KNOWS*1..5]->(b)) WITH nodes(p) AS ns "
        "UNWIND range(0, size(ns) - 2) AS i "
        "RETURN i, ns[i] AS x, ns[i + 1] AS y, [toString(ns[i]), 'to'] AS label",
        [(0, 5, 1, ["5", "to"]), (1, 1, 3, ["1", "to"])],
    ),
]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), CASES)
def test_paths(use_csr, query, expected):
    # Without the CSR, the search builds its adjacency from a scan of the relationship.
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    assert (
        sorted(tuple(r.values()) for r in engine.execute(query).to_pylist()) == expected
    )


def test_collect_skips_nulls():
    people = pa.table({"id": [1, 2, 3], "nick": ["x", None, None], "team": [1, 1, 2]})
    config = CONFIG
    engine = CypherEngine(config, {"Person": people, "KNOWS": KNOWS})
    got = engine.execute(
        "MATCH (a:Person) RETURN a.team AS team, coalesce(collect(a.nick), []) AS nicks"
    )
    assert sorted((r["team"], r["nicks"]) for r in got.to_pylist()) == [
        (1, ["x"]),
        (2, []),
    ]


def test_search_without_csr_reads_every_edge_partition():
    # A 20,000-edge chain splits into several in-memory partitions; the hops searched
    # here live only in the last one. Reading just the first partition found no path.
    n = 20_000
    people = pa.table({"id": pa.array(range(n + 1), pa.int64())})
    knows = pa.table(
        {
            "src": pa.array(range(n), pa.int64()),
            "dst": pa.array(range(1, n + 1), pa.int64()),
        }
    )
    engine = CypherEngine(CONFIG, {"Person": people, "KNOWS": knows})
    got = engine.execute(
        f"MATCH (a:Person {{id: {n - 10}}}), (b:Person {{id: {n - 5}}}) "
        "MATCH p = shortestPath((a)-[:KNOWS*1..30]->(b)) RETURN length(p) AS l"
    )
    assert got.to_pylist() == [{"l": 5}]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize("direction", ["outgoing", "incoming", "undirected"])
def test_all_shortest_paths_keeps_every_meeting_node(use_csr, direction):
    people = pa.table({"id": list(range(1, 10))})
    knows = pa.table(
        {
            "src": [1, 1, 1, 1, 2, 2, 3, 3, 4, 5, 5],
            "dst": [2, 2, 3, 9, 4, 4, 4, 4, 5, 5, 6],
        }
    )
    engine = CypherEngine(CONFIG, {"Person": people, "KNOWS": knows}, use_csr=use_csr)
    pattern = {
        "outgoing": "(a)-[:KNOWS*1..3]->(b)",
        "incoming": "(b)<-[:KNOWS*1..3]-(a)",
        "undirected": "(a)-[:KNOWS*1..3]-(b)",
    }[direction]
    got = engine.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 5}) "
        f"MATCH p = allShortestPaths({pattern}) RETURN length(p) AS l, nodes(p) AS ns"
    ).to_pylist()
    paths = [[1, 2, 4, 5]] * 4 + [[1, 3, 4, 5]] * 2
    if direction == "incoming":
        paths = [p[::-1] for p in paths]
    assert sorted((r["l"], r["ns"]) for r in got) == sorted((3, p) for p in paths)


@pytest.mark.parametrize("use_csr", [False, True])
def test_shortest_paths_shares_a_source_across_targets(use_csr):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    got = engine.execute(
        "MATCH (a:Person {id: 5}), (b:Person) "
        "MATCH p = allShortestPaths((a)-[:KNOWS*1..3]->(b)) "
        "RETURN b.id AS target, length(p) AS l, nodes(p) AS ns"
    ).to_pylist()
    assert sorted((r["target"], r["l"], r["ns"]) for r in got) == [
        (1, 1, [5, 1]),
        (2, 2, [5, 1, 2]),
        (2, 2, [5, 1, 2]),
        (3, 2, [5, 1, 3]),
    ]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("target", "hops", "missing"), [(3, 2, False), (6, -1, True)])
@pytest.mark.parametrize(
    "projection",
    [
        "RETURN CASE p IS NULL WHEN true THEN -1 ELSE length(p) END AS hops, "
        "p IS NULL AS missing",
        "RETURN CASE WHEN p IS NOT NULL THEN length(p) ELSE -1 END AS hops, "
        "p IS NULL AS missing",
        "WITH p AS q RETURN CASE q IS NULL WHEN true THEN -1 "
        "ELSE length(q) END AS hops, q IS NULL AS missing",
        "WITH p AS q WHERE q IS NOT NULL OR q IS NULL "
        "RETURN coalesce(length(q), -1) AS hops, q IS NULL AS missing",
    ],
)
def test_shortest_path_null_checks_keep_compact_plan(
    use_csr, target, hops, missing, projection
):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    query = (
        f"MATCH (a:Person {{id: 5}}), (b:Person {{id: {target}}}) "
        "OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*1..5]-(b)) " + projection
    )
    assert engine.execute(query).to_pylist() == [{"hops": hops, "missing": missing}]
    plan = engine.explain(query)
    assert "ShortestPathExec" in plan
    assert "PathMatchExec" not in plan
