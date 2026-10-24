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
    "Comment": pa.table({"id": [20, 21], "content": ["c20", "c21"]}),
} | {
    name: pa.table({"src": pa.array(s, pa.int64()), "dst": pa.array(d, pa.int64())})
    for name, (_, _, s, d) in RELS.items()
}


def config(endpoints=True):
    builder = GraphConfig.builder()
    for label in ("Person", "Post", "Comment"):
        builder = builder.with_node_label(label, "id")
    for name, (src, dst, _, _) in RELS.items():
        labels = (src, dst) if endpoints else (None, None)
        builder = builder.with_relationship(name, "src", "dst", *labels)
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


def test_label_choice_needs_endpoint_labels(datasets):
    # Without endpoint labels, `m` could be a Post or a Comment for either type.
    engine = CypherEngine(config(endpoints=False), datasets)
    with pytest.raises(Exception, match="endpoint labels"):
        engine.execute(
            "MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m:Post:Comment) "
            "RETURN m.id"
        )


def test_endpoint_labels_must_be_given_together():
    with pytest.raises(ValueError, match="together"):
        GraphConfig.builder().with_node_label("Post", "id").with_relationship(
            "r", "src", "dst", "Post"
        )


@pytest.mark.parametrize("use_csr", [False, True])
def test_variable_length_alternation_follows_endpoint_labels(datasets, use_csr):
    # Comment 21 replies to comment 20, which replies to post 10: one and two hops.
    engine = CypherEngine(config(), datasets, use_csr=use_csr)
    got = engine.execute(
        "MATCH (c:Comment)-[:replyOfComment|replyOfPost*1..3]->(p:Post) "
        "RETURN c.id, p.id"
    )
    assert rows(got) == [(20, 10), (21, 10)]


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
