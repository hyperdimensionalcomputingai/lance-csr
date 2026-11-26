# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Relationship-type alternation (``[:A|B]``) and multi-label nodes (``(m:A:B)``).

A small LDBC-shaped graph: posts and comments created by people, comments replying to
posts or comments. Expected answers are written out by hand.
"""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, DirNamespace, GraphConfig, SqlEngine

RELS = {
    "postHasCreator": ("Post", "Person", [10, 11], [1, 2]),
    "commentHasCreator": ("Comment", "Person", [20, 21], [1, 1]),
    "replyOfPost": ("Comment", "Post", [20], [10]),
    "replyOfComment": ("Comment", "Comment", [21], [20]),
}
TABLES = {
    "Person": pa.table({"id": [1, 2], "name": ["a", "b"]}),
    "Post": pa.table(
        {"id": [10, 11], "content": ["p10", "p11"], "imagefile": ["i10", None]}
    ),
    "Comment": pa.table({"id": [20, 21], "content": ["c20", "c21"], "score": [7, 9]}),
} | {
    name: pa.table({"src": pa.array(s, pa.int64()), "dst": pa.array(d, pa.int64())})
    for name, (_, _, s, d) in RELS.items()
}
TABLES["replyOfPost"] = TABLES["replyOfPost"].append_column("page", pa.array([7]))


def config():
    builder = GraphConfig.builder()
    for label in ("Person", "Post", "Comment"):
        builder = builder.node_table(label, "id")
    for name, (src, dst, _, _) in RELS.items():
        builder = builder.rel_table(name, src, dst, "src", "dst")
    return builder.build()


@pytest.fixture(params=["arrow", "lance"])
def datasets(request, tmp_path):
    if request.param == "arrow":
        return TABLES
    return {
        name: lance.write_dataset(table, tmp_path / f"{name}.lance")
        for name, table in TABLES.items()
    }


def rows(table):
    return sorted(tuple(r.values()) for r in table.to_pylist())


CASES = [
    (
        "MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m) "
        "RETURN p.id, m.id, m.content",
        [(1, 10, "p10"), (1, 20, "c20"), (1, 21, "c21"), (2, 11, "p11")],
    ),
    # A property only posts have is null for comments.
    (
        "MATCH (p:Person {id: 1})<-[:postHasCreator|:commentHasCreator]-(m) "
        "RETURN m.id, m.imagefile",
        [(10, "i10"), (20, None), (21, None)],
    ),
    # The middle node's label follows from both relationships' types (LDBC IC8 shape).
    (
        "MATCH (p:Person {id: 1})<-[:postHasCreator|commentHasCreator]-(:Post:Comment)"
        "<-[:replyOfPost|replyOfComment]-(c:Comment) RETURN c.id",
        [(20,), (21,)],
    ),
    ("MATCH (m:Post:Comment) RETURN count(*) AS n", [(4,)]),
    (
        "MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m) "
        "RETURN p.id, count(*) AS n",
        [(1, 3), (2, 1)],
    ),
]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), CASES)
def test_alternation(datasets, use_csr, query, expected):
    engine = CypherEngine(config(), datasets, use_csr=use_csr)
    assert rows(engine.execute(query)) == sorted(expected)


def test_alternation_branches_use_csr(datasets):
    engine = CypherEngine(config(), datasets, use_csr=True)
    plan = engine.explain(
        "MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m) RETURN m.id"
    )
    physical = plan[plan.index("physical_plan") :]
    assert physical.count("CsrExpandExec") == 2 and "UnionExec" in physical


@pytest.mark.parametrize("use_csr", [False, True])
def test_variable_length_alternation_follows_endpoint_labels(datasets, use_csr):
    # Comment 21 replies to comment 20, which replies to post 10: one and two hops.
    engine = CypherEngine(config(), datasets, use_csr=use_csr)
    got = engine.execute(
        "MATCH (c:Comment)-[:replyOfComment|replyOfPost*1..3]->(p:Post) "
        "RETURN c.id, p.id"
    )
    assert rows(got) == [(20, 10), (21, 10)]


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize("minimum", [0, 1])
def test_variable_length_mixed_labels_keep_identity_separate(use_csr, minimum):
    builder = GraphConfig.builder().node_table("Tag", "id").node_table("TagClass", "id")
    cfg = (
        builder.rel_table("hasType", "Tag", "TagClass", "src", "dst")
        .rel_table("isSubclassOf", "TagClass", "TagClass", "src", "dst")
        .build()
    )
    tables = {
        "Tag": pa.table({"id": [1, 2]}),
        "TagClass": pa.table({"id": [1, 2, 3], "name": ["one", "two", "three"]}),
        "hasType": pa.table({"src": [1, 1, 2, 1], "dst": [1, 1, 2, 99]}),
        "isSubclassOf": pa.table({"src": [1, 2], "dst": [2, 3]}),
    }
    engine = CypherEngine(cfg, tables, use_csr=use_csr)
    query = (
        f"MATCH (t:Tag {{id: 1}})-[:hasType|isSubclassOf*{minimum}..3]->(c:TagClass) "
        "RETURN c.id, c.name"
    )
    assert rows(engine.execute(query)) == [
        (1, "one"),
        (1, "one"),
        (2, "two"),
        (2, "two"),
        (3, "three"),
        (3, "three"),
    ]
    if use_csr:
        plan = engine.explain(query)
        physical = plan[plan.index("physical_plan") :]
        assert physical.count("CsrExpandExec") == 1
        assert "UnionExec" not in physical
    # Equal integer keys in different labels never form a zero-hop path.
    identity = (
        "MATCH (t:Tag {id: 1})-[:hasType|isSubclassOf*0..0]->(c:TagClass) RETURN c.id"
    )
    assert rows(engine.execute(identity)) == []
    same_label = (
        "MATCH (t:TagClass {id: 1})-[:hasType|isSubclassOf*0..2]->(c:TagClass) "
        "RETURN c.id"
    )
    assert rows(engine.execute(same_label)) == [(1,), (2,), (3,)]


@pytest.mark.parametrize("use_csr", [False, True])
def test_undirected_bounded_alternation(datasets, use_csr):
    engine = CypherEngine(config(), datasets, use_csr=use_csr)
    types = "postHasCreator|commentHasCreator|replyOfComment"
    path = f"(p:Post {{id: 10}})-[:{types}*1..3]-(q:Comment)"
    query = f"MATCH {path} RETURN q.id, q.score"
    # Two trails reach each comment: via the creator and via the other comment.
    expected = [(20, 7), (20, 7), (21, 9), (21, 9)]
    assert rows(engine.execute(query)) == expected
    assert rows(engine.execute(f"MATCH pth = {path} RETURN q.id, q.score")) == expected
    physical = engine.explain(query).split("physical_plan", 1)[1]
    operator = "CsrExpandExec" if use_csr else "PathMatchExec: endpoints"
    assert physical.count(operator) == 1
    assert "UnionExec" not in physical

    # The application carries its starting nodes through UNWIND and collects endpoints.
    carried = (
        "MATCH (s:Post {id: 10}) WITH collect(s) AS seeds UNWIND seeds AS p "
        f"OPTIONAL MATCH (p)-[:{types}*1..3]-(q:Comment) "
        "WITH collect(DISTINCT q) AS endpoints UNWIND endpoints AS c RETURN c.id"
    )
    assert rows(engine.execute(carried)) == [(20,), (21,)]

    # Post 11 has one way out. Reusing that edge is not a trail.
    query = (
        "MATCH (p:Post {id: 11}) "
        "OPTIONAL MATCH (p)-[:postHasCreator|commentHasCreator*1..2]-(q:Post) "
        "RETURN p.id AS p, q.id AS q"
    )
    assert engine.execute(query).to_pylist() == [{"p": 11, "q": None}]


@pytest.mark.parametrize("binding", ["", "p = "])
def test_undirected_alternation_bound_start_label(binding):
    cfg = (
        GraphConfig.builder()
        .node_table("X", "id")
        .node_table("Y", "id")
        .rel_table("A", "Y", "Y", "src", "dst")
        .rel_table("B", "Y", "Y", "src", "dst")
        .build()
    )
    tables = {
        "X": pa.table({"id": [1]}),
        "Y": pa.table({"id": [1, 2, 3]}),
        "A": pa.table({"src": [1], "dst": [2]}),
        "B": pa.table({"src": [3], "dst": [2]}),
    }
    engine = CypherEngine(cfg, tables)
    path = f"MATCH {binding}(s:Y)-[:A|B*1..2]-(q:Y) RETURN q.id"
    assert rows(engine.execute(f"MATCH (s:Y {{id: 1}}) {path}")) == [(2,), (3,)]
    with pytest.raises(
        ValueError, match="Variable 's' already has label 'X', cannot redefine as 'Y'"
    ):
        engine.execute(f"MATCH (s:X {{id: 1}}) {path}")


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(
    ("edges", "pattern", "expected"),
    [
        ([(1, 2), (1, 2)], "-[:{}*1..2]-", [1, 1, 2, 2]),
        ([(1, 1)], "-[:{}*1..2]->", [1]),
        ([(1, 1)], "-[:{}*1..2]-", [1]),
        ([(1, 2), (2, 1)], "-[:{}*1..4]->", [1, 2]),
        ([(1, 2), (2, 3), (3, 1)], "-[:{}*4..4]-", []),
        ([(1, 2), (2, 1), (1, 2)], "<-[:{}*3..3]-", []),
        ([(1, 2), (2, 1), (1, 2)], "-[:{}*3..3]->", [2, 2]),
    ],
    ids=[
        "parallel",
        "self-loop",
        "undirected-loop",
        "cycle",
        "triangle",
        "incoming",
        "distinct-parallel",
    ],
)
def test_each_relationship_is_used_once_per_trail(use_csr, edges, pattern, expected):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("A", "N", "N", "src", "dst")
        .rel_table("B", "N", "N", "src", "dst")
        .build()
    )
    src, dst = zip(*edges)
    a = pa.table({"src": src, "dst": dst})
    tables = {"N": pa.table({"id": [1, 2, 3]}), "A": a, "B": a.slice(0, 0)}
    engine = CypherEngine(cfg, tables, use_csr=use_csr)
    for types in ("A", "A|B", "A|a"):
        for binding in ("", "p = "):
            path = f"{binding}(s:N {{id: 1}}){pattern.format(types)}(q:N)"
            assert (
                sorted(
                    engine.execute(f"MATCH {path} RETURN q.id").column(0).to_pylist()
                )
                == expected
            ), path


@pytest.mark.parametrize("use_csr", [False, True])
def test_trails_keep_typed_endpoints_separate(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("X", "id")
        .node_table("Y", "id")
        .rel_table("A", "X", "Y", "src", "dst")
        .rel_table("B", "Y", "X", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        cfg,
        {
            "X": pa.table({"id": [1, 2]}),
            "Y": pa.table({"id": [1, 2]}),
            "A": pa.table({"src": [1, 1, 2], "dst": [2, 2, 2]}),
            "B": pa.table({"src": [2], "dst": [1]}),
        },
        use_csr=use_csr,
    )
    for binding in ("", "p = "):
        for pattern, label, expected in [
            ("-[:A*1..3]->", "X", []),
            ("-[:A*1..3]->", "Y", [2, 2]),
            ("-[:A*2..2]-", "X", [1, 1, 2, 2]),
            ("-[:A|B*3..3]->", "Y", [2, 2]),
        ]:
            path = f"{binding}(s:X {{id: 1}}){pattern}(q:{label})"
            assert (
                sorted(
                    engine.execute(f"MATCH {path} RETURN q.id").column(0).to_pylist()
                )
                == expected
            )

    for endpoint in ("q", "q:X:Y"):
        query = f"MATCH (s:X {{id:1}})-[:A|B*3..3]->({endpoint}) RETURN q.id"
        assert rows(engine.execute(query)) == [(2,), (2,)]


@pytest.mark.parametrize("use_csr", [False, True])
def test_trail_boundaries_and_carried_scalars(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("A", "N", "N", "src", "dst")
        .rel_table("B", "N", "N", "src", "dst")
        .build()
    )
    a = pa.table({"src": [1, 2, 1], "dst": [2, 1, 2], "_edge_rank": [1, 2, 3]})
    engine = CypherEngine(
        cfg,
        {"N": pa.table({"id": [1, 2]}), "A": a, "B": a.slice(0, 0)},
        use_csr=use_csr,
    )
    for binding in ("", "p = "):
        path = f"{binding}(s:N {{id: 1}})-[:A*2..2]->(:N)-[:A]->(q:N)"
        assert rows(engine.execute(f"MATCH {path} RETURN q.id")) == [(2,), (2,)]
    # A new MATCH can use the same edges again. A scalar supplied by WITH survives both.
    query = (
        "WITH 99 AS __trail_rank_0 MATCH (s:N {id:1})-[:A|B*3..3]->(q:N) "
        "MATCH (q)-[:A|B*2..2]->(b:N) RETURN __trail_rank_0, b.id"
    )
    assert rows(engine.execute(query)) == [(99, 2)] * 4
    query = "MATCH (s:N {id:1})-[:A*1..3 {_edge_rank: 1}]-(q:N) RETURN q.id"
    assert rows(engine.execute(query)) == [(2,)]


def test_terminal_batches_resume_across_high_degree_and_missing_nodes():
    # Unmatched endpoints span multiple yield intervals before the first row.
    missing = list(range(3000, 5500))
    endpoints = list(range(2, 1091))
    dst = missing + endpoints
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("A", "N", "N", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        cfg,
        {
            "N": pa.table({"id": [0, 1, *endpoints]}),
            "A": pa.table({"src": [0, *([1] * len(dst))], "dst": [1, *dst]}),
        },
        use_csr=True,
    )
    query = "MATCH (s:N {id:0})-[:A*2..2]->(q:N) RETURN q.id"
    assert "CsrExpandExec" in engine.explain(query)
    result = engine.execute(query)
    assert sorted(result.column(0).to_pylist()) == endpoints


def test_trail_join_uses_stored_rows_across_lance_versions(tmp_path):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("A", "N", "N", "src", "dst")
        .build()
    )
    path = tmp_path / "edges.lance"
    a = pa.table(
        {
            "src": [1, 1, 1],
            "dst": [2, 2, 2],
            "n": [1, 2, 3],
            "__lance_csr_edge_ordinal": [0, 0, 0],
        }
    )
    dataset = lance.write_dataset(a, path, max_rows_per_file=2)
    dataset.delete("n = 2")
    nodes = pa.table({"id": [1, 2]})
    query = "MATCH (s:N {id:1})-[:A*2..2]-(q:N) RETURN q.id"
    for source in (a.filter(pa.array([True, False, True])), lance.dataset(path)):
        engine = CypherEngine(cfg, {"N": nodes, "A": source}, use_csr=False)
        assert rows(engine.execute(query)) == [(1,), (1,)]
        assert "WindowAggExec" not in engine.explain(query)
    lance.write_dataset(a.slice(0, 1), path, mode="append")
    assert rows(engine.execute(query)) == [(1,), (1,)]
    engine.checkout_latest()
    assert rows(engine.execute(query)) == [(1,)] * 6

    # A user column that would collide with Lance's hidden physical address
    # selects the generic window-based fallback. Its values are all identical.
    colliding = a.append_column("_ROWADDR", pa.array([0, 0, 0]))
    fallback = lance.write_dataset(colliding, tmp_path / "custom.lance")
    engine = CypherEngine(cfg, {"N": nodes, "A": fallback}, use_csr=False)
    assert "WindowAggExec" in engine.explain(query)
    assert rows(engine.execute(query)) == [(1,)] * 6


def test_undirected_alternation_parallel_edges_and_zero_hops():
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("A", "N", "N", "src", "dst")
        .rel_table("B", "N", "N", "src", "dst")
        .build()
    )
    tables = {
        "N": pa.table({"id": [1, 2]}),
        "A": pa.table({"src": [2, 2], "dst": [1, 1]}),
        "B": pa.table({"src": [2], "dst": [2]}),
    }
    engine = CypherEngine(cfg, tables)
    query = "MATCH (s:N {id: $seed})-[:A|B*0..2]-(q:N) RETURN q.id"
    # One zero-hop match, two parallel edges, two returns via different edges,
    # and one way per arriving edge to traverse the self-loop.
    assert rows(engine.execute(query, {"seed": 1})) == [(1,)] * 3 + [(2,)] * 4
    comp = "MATCH (s:N {id: 1}) RETURN [(s)-[:A|B*1..2]-(q:N {id: $t}) | q.id] AS ids"
    for target, expected in [(2, [2] * 4), (1, [1] * 2)]:
        assert engine.execute(comp, {"t": target}).to_pylist() == [{"ids": expected}]
    other = engine.execute(query, {"seed": 2})
    assert rows(other) == rows(
        engine.execute(query.replace("MATCH ", "MATCH p = "), {"seed": 2})
    )


@pytest.mark.parametrize(
    "predicate",
    [
        "(s)-[:A|B*1..2]-(:N {id: $target})",
        "EXISTS { MATCH (s)-[:A|B*1..2]-(:N {id: $target}) }",
    ],
)
def test_undirected_alternation_predicate_rebinds_parameters(predicate):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("A", "N", "N", "src", "dst")
        .rel_table("B", "N", "N", "src", "dst")
        .build()
    )
    tables = {
        "N": pa.table({"id": [1, 2, 3, 4]}),
        "A": pa.table({"src": [2], "dst": [1]}),
        "B": pa.table({"src": [2], "dst": [3]}),
    }
    engine = CypherEngine(cfg, tables)
    query = f"MATCH (s:N {{id: 1}}) WHERE {predicate} RETURN s.id"
    for target, expected in [(3, [(1,)]), (4, []), (2, [(1,)])]:
        assert rows(engine.execute(query, {"target": target})) == expected


def test_collect_distinct(datasets):
    engine = CypherEngine(config(), datasets)
    got = engine.execute(
        "MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m) "
        "RETURN p.id AS id, collect(DISTINCT p.name) AS names, count(*) AS n"
    )
    assert sorted((r["id"], r["names"], r["n"]) for r in got.to_pylist()) == [
        (1, ["a"], 3),
        (2, ["b"], 1),
    ]


@pytest.mark.parametrize("use_csr", [False, True])
def test_anonymous_relationships_return_all_types(datasets, use_csr):
    engine = CypherEngine(config(), datasets, use_csr=use_csr)
    result = engine.execute("MATCH (a)-[]-(b) RETURN a,b")
    assert result.num_rows == 12
    labels = {row["a"]["__lance_csr_identity"]["label"] for row in result.to_pylist()}
    assert labels == {"Person", "Post", "Comment"}
    paths = engine.execute("MATCH path = (a)-[]-(b) RETURN path")
    assert paths.num_rows == 12
    assert all(row["path"]["length"] == 1 for row in paths.to_pylist())

    empty_properties = dict.fromkeys(("name", "content", "imagefile", "score"))
    expected = {
        (label, row["id"]): {**empty_properties, **row}
        for label in ("Person", "Post", "Comment")
        for row in TABLES[label].to_pylist()
    }

    def check_node(node):
        identity = node["__lance_csr_identity"]
        key = (identity["label"], identity["id"])
        assert node["__lance_csr_properties"] == expected[key]

    def check_page(edge):
        kind = edge["__lance_csr_identity"]["label"]
        assert edge["__lance_csr_properties"]["page"] == (
            7 if kind == "replyOfPost" else None
        )

    for row in result.to_pylist():
        check_node(row["a"])
        check_node(row["b"])
    for row in paths.to_pylist():
        for node in row["path"]["node_records"]:
            check_node(node)
        edge = row["path"]["relationships"][0]
        check_page(edge)
        check_node(edge["__lance_csr_start"])
        check_node(edge["__lance_csr_end"])

    edges = engine.execute("MATCH (a)-[r]-(b) RETURN r").to_pylist()
    assert len(edges) == 12
    for row in edges:
        check_page(row["r"])


@pytest.mark.parametrize("dtype", [pa.uint64(), pa.int32()])
def test_arrow_csr_ignores_user_rowaddr_for_edge_identity(dtype):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .build()
    )
    data = {
        "N": pa.table({"id": [1, 2, 3]}),
        "R": pa.table(
            {"src": [1, 2], "dst": [2, 3], "_rowaddr": pa.array([0, 0], dtype)}
        ),
    }
    engine = CypherEngine(cfg, data, use_csr=True)
    query = "MATCH (:N {id:1})-[:R*2..2]->(b:N) RETURN b.id"
    assert rows(engine.execute(query)) == [(3,)]


@pytest.mark.parametrize("use_csr", [False, True])
def test_overlapping_trail_with_conflicting_node_property_types(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("X", "id")
        .node_table("Y", "id")
        .rel_table("R", "X", "Y", "src", "dst")
        .build()
    )
    data = {
        "X": pa.table({"id": pa.array([1, 2], pa.int32())}),
        "Y": pa.table({"id": pa.array([10], pa.int64())}),
        "R": pa.table({"src": pa.array([1, 2], pa.int32()), "dst": [10, 10]}),
    }
    engine = CypherEngine(cfg, data, use_csr=use_csr)
    query = "MATCH (:X {id:1})-[:R*1..1]->(:Y)<-[:R]-(q:X) RETURN q.id"
    assert rows(engine.execute(query)) == [(2,)]


@pytest.mark.parametrize("use_csr", [False, True])
def test_overlapping_trail_with_duplicate_node_keys(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        cfg,
        {
            "N": pa.table({"id": [1, 2, 3, 4, 4]}),
            "R": pa.table({"src": [1, 2], "dst": [2, 3]}),
        },
        use_csr=use_csr,
    )
    query = "MATCH (a:N {id:1})-[:R*1..1]->(b:N)-[:R]->(c:N) RETURN c.id"
    assert rows(engine.execute(query)) == [(3,)]


@pytest.mark.parametrize("use_csr", [False, True])
def test_unrelated_duplicate_cannot_enable_edge_reuse(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .build()
    )
    data = {
        "N": pa.table({"id": [1, 2, 9, 9]}),
        "R": pa.table({"src": [1], "dst": [2]}),
    }
    engine = CypherEngine(cfg, data, use_csr=use_csr)
    query = "MATCH (a:N {id:1})-[:R*1..1]-(b:N)-[:R]-(c:N) RETURN c.id"
    assert rows(engine.execute(query)) == []
    assert rows(engine.execute(query.replace("MATCH ", "MATCH (:N {id:2}), "))) == []


def test_unreachable_alternative_cannot_disable_indexed_trail():
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .node_table("C", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .rel_table("T", "N", "N", "src", "dst")
        .rel_table("S", "C", "C", "src", "dst")
        .build()
    )
    for ids in ([9], [9, 9], ["nine", "nine"]):
        engine = CypherEngine(
            cfg,
            {
                "N": pa.table({"id": [1, 2]}),
                "C": pa.table({"id": ids}),
                "R": pa.table({"src": [1], "dst": [1]}),
                "T": pa.table({"src": [2], "dst": [2]}),
                "S": pa.table({"src": [ids[0]], "dst": [ids[0]]}),
            },
            use_csr=True,
        )
        prefix = "MATCH (:N {id:1})-[:S|R|T*1..6]->(b:N)"
        assert "CsrExpandExec" in engine.explain(prefix + " RETURN b.id")
        assert "TrailWalkExec" in engine.explain(prefix + "-[:R]->(c:N) RETURN c.id")
        assert rows(engine.execute(prefix + " RETURN b.id")) == [(1,)]
        assert rows(engine.execute(prefix + "-[:R]->(c:N) RETURN c.id")) == []


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize("end", ["Tag", "C"])
def test_alternating_trail_that_closes_on_its_start(use_csr, end):
    cfg = (
        GraphConfig.builder()
        .node_table("Tag", "id")
        .node_table("C", "id")
        .rel_table("H", "Tag", "C", "src", "dst")
        .rel_table("S", "C", end, "src", "dst")
        .build()
    )
    engine = CypherEngine(
        cfg,
        {
            "Tag": pa.table({"id": [1]}),
            "C": pa.table({"id": [2]}),
            "H": pa.table({"src": [1], "dst": [2]}),
            "S": pa.table({"src": [2], "dst": [1 if end == "Tag" else 2]}),
        },
        use_csr=use_csr,
    )
    cycles = [(1,), (2,)] if end == "Tag" else [(2,)]
    for low, expected in [(1, cycles), (0, sorted(cycles + [(1,), (2,)]))]:
        q = f"MATCH (a)-[:H|S*{low}..2]->(a) RETURN a.id"
        assert rows(engine.execute(q)) == expected


@pytest.mark.parametrize("use_csr", [False, True])
def test_repeated_type_in_trail_alternation(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .build()
    )
    data = {
        "N": pa.table({"id": [1, 2, 3]}),
        "R": pa.table({"src": [1, 1, 2], "dst": [2, 2, 3]}),
    }
    engine = CypherEngine(cfg, data, use_csr=use_csr)
    for tail in [" RETURN b.id", "-[:R]-(c:N) RETURN b.id, c.id"]:
        pattern = "MATCH (:N {id:1})-[:R*1..2]->(b:N)"
        expected = rows(engine.execute(pattern + tail))
        assert rows(engine.execute(pattern.replace(":R*", ":R|r*") + tail)) == expected


@pytest.mark.parametrize("on_disk", [False, True])
def test_overlapping_trail_one_shot_and_graph_table(on_disk, tmp_path):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .build()
    )
    data = {
        "N": pa.table({"id": [1, 2, 3, 3]}),
        "R": pa.table({"src": [1], "dst": [2]}),
    }
    if on_disk:
        data = {
            name: lance.write_dataset(table, tmp_path / f"{name}.lance")
            for name, table in data.items()
        }
    pattern = "MATCH (:N {id:1})-[:R*1..1]->(:N)<-[:R]-(q:N)"
    # The same stored edge cannot be used for the return hop.
    query = CypherQuery(pattern + " RETURN q.id").with_config(cfg)
    assert rows(query.execute(data)) == []
    if on_disk:
        assert rows(query.execute_with_namespace(DirNamespace(str(tmp_path)))) == []
    sql = f"SELECT * FROM GRAPH_TABLE ({pattern} COLUMNS (q.id AS id))"
    engine = SqlEngine(data, cfg, use_csr=False)
    assert rows(engine.execute(sql)) == []
    assert rows(engine.execute(sql)) == []


def test_unrelated_duplicate_in_new_snapshot_preserves_trail(tmp_path):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .build()
    )
    nodes = lance.write_dataset(pa.table({"id": [1, 2, 3]}), tmp_path / "nodes")
    edges = pa.table({"src": [1], "dst": [2]})
    engine = CypherEngine(cfg, {"N": nodes, "R": edges}, use_csr=False)
    query = "MATCH (:N {id:1})-[:R*1..1]->(:N)<-[:R]-(q:N) RETURN q.id"
    assert rows(engine.execute(query)) == []
    lance.write_dataset(pa.table({"id": [3]}), tmp_path / "nodes", mode="append")
    assert rows(engine.execute(query)) == []
    engine.checkout_latest()
    assert rows(engine.execute(query)) == []


@pytest.mark.parametrize("use_csr", [False, True])
def test_overlapping_trail_with_unrelated_endpoint_label(use_csr):
    cfg = (
        GraphConfig.builder()
        .node_table("X", "id")
        .node_table("Y", "id")
        .node_table("Z", "id")
        .rel_table("R", "X", "Y", "src", "dst")
        .build()
    )
    tables = {
        "X": pa.table({"id": [1]}),
        "Y": pa.table({"id": [2]}),
        # The numerical ID also occurs in X, but R is only mapped from X to Y.
        "Z": pa.table({"id": [1]}),
        "R": pa.table({"src": [1], "dst": [2]}),
    }
    engine = CypherEngine(cfg, tables, use_csr=use_csr)
    query = "MATCH (:Z)-[:R*1..1]->(:Y)<-[:R]-(q:X) RETURN q.id"
    assert rows(engine.execute(query)) == []


@pytest.mark.parametrize("use_csr", [True, False])
@pytest.mark.parametrize("types", ["R", "R|S"])
def test_trail_followed_by_same_type_hop_uses_local_expansion(use_csr, types):
    cfg = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .rel_table("S", "N", "N", "src", "dst")
        .build()
    )
    # The parallel edges are distinct. At the middle node the next hop must
    # remember which one reached it, including when returning to node 1.
    engine = CypherEngine(
        cfg,
        {
            "S": pa.table({"src": [3], "dst": [1]}),
            "N": pa.table({"id": [1, 2, 3, 4]}),
            "R": pa.table({"src": [1, 1, 2, 3], "dst": [2, 2, 3, 4]}),
        },
        use_csr=use_csr,
    )
    query = f"MATCH (:N {{id:1}})-[:{types}*1..2]-(f:N)-[:R]-(g:N) RETURN f.id, g.id"
    expected = [(2, 1), (2, 1), (2, 3), (2, 3), (3, 4), (3, 4)]
    if types == "R|S":
        # The S row can be followed by R even when both have the same row ordinal.
        expected += [(3, 2), (3, 4), (2, 1), (2, 1)]
    assert rows(engine.execute(query)) == sorted(expected)
    assert "PathMatchExec" not in engine.explain(query)

    reverse = f"MATCH (g:N)-[:R]-(f:N)-[:{types}*1..2]-(p:N {{id:1}}) RETURN f.id, g.id"
    assert rows(engine.execute(reverse)) == sorted(expected)
    if use_csr:
        assert "TrailWalkExec: start=p__id" in engine.explain(reverse)
