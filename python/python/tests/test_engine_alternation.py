# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Relationship-type alternation (``[:A|B]``) and multi-label nodes (``(m:A:B)``).

A small LDBC-shaped graph: posts and comments created by people, comments replying to
posts or comments. Expected answers are written out by hand.
"""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

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
    assert physical.count("PathMatchExec: endpoints") == 1
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
