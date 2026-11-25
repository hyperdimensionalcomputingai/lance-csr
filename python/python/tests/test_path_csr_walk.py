# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Named paths walking the CSR return what the edge inventory returns."""

import lance
import numpy as np
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .node_table("P", "id")
    .node_table("C", "id")
    .rel_table("R", "P", "P", "src", "dst")
    .rel_table("S", "P", "P", "src", "dst")
    .rel_table("L", "P", "C", "src", "dst")
    .build()
)

# Parallel edges (0->1), self-loops (2, 5), a dangling endpoint (42) and C keys that
# collide with P keys. S has `day` but no `w`; L has neither.
PAIRS = [(a, (a * 3 + k) % 8) for a in range(8) for k in range(3)]
PAIRS += [(0, 1), (0, 1), (2, 2), (5, 5), (7, 42)]
DATA = {
    "P": pa.table({"id": list(range(8)), "name": [f"p{i}" for i in range(8)]}),
    "C": pa.table({"id": [0, 1, 2], "name": ["c0", "c1", "c2"]}),
    "R": pa.table(
        {
            "src": [a for a, _ in PAIRS],
            "dst": [b for _, b in PAIRS],
            "day": [(a + b) % 3 for a, b in PAIRS],
            "w": [float(i) for i in range(len(PAIRS))],
        }
    ),
    "S": pa.table(
        {"src": [1, 2, 3, 3, 6], "dst": [2, 3, 1, 3, 0], "day": [0, 1, 0, 2, 0]}
    ),
    "L": pa.table({"src": [0, 1, 2, 3, 0], "dst": [0, 1, 2, 0, 2]}),
}

PATH = (
    "RETURN a.id AS a, length(p) AS n, [x IN nodes(p) | x.id] AS ids, "
    "[x IN nodes(p) | labels(x)[0]] AS labels, "
    "[e IN relationships(p) | e.day] AS days, "
    "[e IN relationships(p) | e.w] AS ws, [e IN relationships(p) | keys(e)] AS keys, "
    "[e IN relationships(p) | e.src] AS srcs, [e IN relationships(p) | e.dst] AS dsts"
)
QUERIES = [
    f"MATCH p = (a:P)-[:R*1..3]->(b:P) {PATH}",
    f"MATCH p = (a:P)<-[:R*1..3]-(b:P) {PATH}",
    f"MATCH p = (a:P)-[:R*1..3]-(b:P) {PATH}",
    f"MATCH p = (a:P)-[:R|S*0..2]-(b:P) {PATH}",
    f"MATCH p = (a:P)-[:R|S*1..3]->(b:P) "
    f"WHERE all(e IN relationships(p) WHERE e.day = 0) {PATH}",
    f"MATCH p = (a:P)-[:R|S*1..2 {{w: 3.0}}]-(b:P) {PATH}",
    # S has no `w`, so its edges fail the first and pass the second.
    f"MATCH p = (a:P)-[:R|S*1..3]-(b:P) "
    f"WHERE all(e IN relationships(p) WHERE e.w < 20 AND e.day IN [0, 2]) {PATH}",
    f"MATCH p = (a:P)-[:R|S*1..2]->(b:P) "
    f"WHERE none(e IN relationships(p) WHERE e.w > 5 OR e.day = 1) {PATH}",
    f"MATCH p = (a:P)-[:R {{day: 2}}]->(b:P)-[:R {{day: 2}}]->(c:P) {PATH}",
    f"MATCH p = (c:C)<-[:L]-(a:P)-[:R*1..2]-(b:P) {PATH}",
    f"MATCH p = (a:P)-[:R*2..2]->(a) {PATH}",
    "MATCH p = (a:P)-[e:R]->(b:P) RETURN a.id AS a, e.day AS d, e.w AS w, b.id AS b",
    "MATCH (a:P)-[:R|S*1..2]-(b:P) RETURN a.id AS a, b.id AS b",
]


def rows(engine, query):
    return sorted(
        repr(sorted(row.items())) for row in engine.execute(query).to_pylist()
    )


def lance_tables(tmp_path, tables, deleted):
    datasets = {
        name: lance.write_dataset(table, tmp_path / name, max_rows_per_file=5)
        for name, table in tables.items()
    }
    for name, predicate in deleted.items():
        datasets[name].delete(predicate)
        datasets[name] = lance.dataset(tmp_path / name)
    return datasets


@pytest.mark.parametrize("lazy", [False, True], ids=["memory", "lance"])
def test_csr_walk_matches_edge_inventory(tmp_path, lazy):
    data = DATA
    if lazy:
        data = lance_tables(
            tmp_path, DATA, {"R": "day = 2 AND src = 3", "S": "src = 6"}
        )
    csr = CypherEngine(CONFIG, data, use_csr=True)
    inventory = CypherEngine(CONFIG, data)
    for query in QUERIES:
        assert "csr walk" in csr.explain(query), query
        assert rows(csr, query) == rows(inventory, query), query


def test_filtered_walk_checks_reached_edges_before_switching_to_a_mask(tmp_path):
    # 30,000 edges switch to a whole-column mask after 117 checked: the start node's
    # 30 edges are fetched one by one, later frames use the mask.
    rng = np.random.default_rng(0)
    n, m = 1_000, 30_000
    data = lance_tables(
        tmp_path,
        {
            "P": pa.table({"id": np.arange(n)}),
            "R": pa.table(
                {
                    "src": np.repeat(np.arange(n), m // n),
                    "dst": rng.integers(0, n, m),
                    "day": rng.integers(0, 10, m),
                }
            ),
        },
        {"R": "dst = 7"},
    )
    config = (
        GraphConfig.builder()
        .node_table("P", "id")
        .rel_table("R", "P", "P", "src", "dst")
        .build()
    )
    query = (
        "MATCH p = (a:P {id: 0})-[:R*1..3]->(b:P) "
        "WHERE all(e IN relationships(p) WHERE e.day = 3) "
        "RETURN [x IN nodes(p) | x.id] AS ids, [e IN relationships(p) | e.day] AS days"
    )
    expected = rows(CypherEngine(config, data), query)
    assert len(expected) > 3
    assert rows(CypherEngine(config, data, use_csr=True), query) == expected


def test_shortest_paths_keep_the_edge_inventory():
    engine = CypherEngine(CONFIG, DATA, use_csr=True)
    query = "MATCH p = shortestPath((a:P {id: 0})-[:R*1..3]->(b:P {id: 4})) RETURN p"
    assert "csr walk" not in engine.explain(query)
    assert "edge inventory" in engine.explain(query)


@pytest.mark.parametrize("lazy", [False, True], ids=["memory", "lance"])
@pytest.mark.parametrize("use_csr", [False, True], ids=["inventory", "csr"])
def test_trail_and_shortest_path_agree_on_relationship_identity(
    tmp_path, lazy, use_csr
):
    # A trail walks the CSR and a shortest path uses the inventory: both must name the
    # same edge the same way, even when Lance row addresses skip deleted rows.
    config = (
        GraphConfig.builder()
        .node_table("P", "id")
        .rel_table("R", "P", "P", "src", "dst")
        .build()
    )
    data = {
        "P": pa.table({"id": list(range(8))}),
        "R": pa.table({"src": list(range(7)), "dst": list(range(1, 8))}),
    }
    if lazy:
        data = lance_tables(tmp_path, data, {"R": "src = 0"})
    engine = CypherEngine(config, data, use_csr=use_csr)
    query = (
        "MATCH p = (a:P {id: 4})-[:R]->(b:P {id: 5}) "
        "MATCH q = shortestPath((a)-[:R*1..3]->(b)) "
        "RETURN relationships(p)[0] = relationships(q)[0] AS same, p = q AS same_path"
    )
    assert engine.execute(query).to_pylist() == [{"same": True, "same_path": True}]


@pytest.mark.parametrize("use_csr", [False, True], ids=["inventory", "csr"])
def test_path_values_are_built_only_when_read(use_csr):
    engine = CypherEngine(CONFIG, DATA, use_csr=use_csr)
    path = "MATCH p = (a:P {id: 0})-[:R*1..3]->(b:P)"
    expected = engine.execute(f"{path} RETURN count(p) AS n").to_pylist()
    unread = ["count(*)", "count(b.id)", "count(DISTINCT b.id)"]
    read = ["count(p)", "sum(length(p))", "count(nodes(p))", "count(relationships(p))"]
    for ret, node in [(r, "endpoints") for r in unread] + [(r, "p") for r in read]:
        query = f"{path} RETURN {ret} AS n"
        assert f"PathMatchExec: {node}" in engine.explain(query), query
    for ret in ["count(*)", "count(b.id)"]:
        assert engine.execute(f"{path} RETURN {ret} AS n").to_pylist() == expected
    ids = f"{path} RETURN b.id AS b, length(p) AS n"
    assert rows(engine, f"{path} RETURN b.id AS b") == sorted(
        repr([("b", row["b"])]) for row in engine.execute(ids).to_pylist()
    )


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(
    "condition",
    [
        "e.day >= 1",
        "e.day <> 1 AND e.w < 15",
        "e.w > $w OR e.day IN [0]",
        "e.w IS NULL",
        "NOT e.day = 2",
    ],
)
def test_relationship_conditions_prune_only_failing_paths(use_csr, condition):
    # Under OR, all() and none() are checked only after the walk.
    engine = CypherEngine(CONFIG, DATA, use_csr=use_csr)
    match = "MATCH p = (a:P)-[:R|S*1..3]-(b:P) "
    for w in [5.0, 10.0]:  # a second binding reuses the cached plan
        for kind in ["all", "none"]:
            pruned = f"{kind}(e IN relationships(p) WHERE {condition})"
            got = engine.execute(f"{match} WHERE {pruned} {PATH}", {"w": w})
            kept = f"{match} WHERE {pruned} OR a.id IS NULL {PATH}"
            want = engine.execute(kept, {"w": w})
            assert sorted(map(repr, got.to_pylist())) == sorted(
                map(repr, want.to_pylist())
            ), (pruned, w)
