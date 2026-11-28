# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Entity functions retain graph binding information and map null semantics."""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True], ids=["join", "csr"])
def engine(request):
    config = (
        GraphConfig.builder()
        .node_table("Person", "person_id")
        .node_table("City", "person_id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .rel_table("VISITS", "Person", "City", "src", "dst")
        .build()
    )
    return CypherEngine(
        config,
        {
            "Person": pa.table(
                {
                    "person_id": [1, 2, 3],
                    "name": ["Alice", None, "Carol"],
                    "age": [30, None, 0],
                    "missing": pa.nulls(3),
                }
            ),
            "City": pa.table({"person_id": [1], "name": ["London"]}),
            "KNOWS": pa.table({"src": [1, 2], "dst": [2, 3], "since": [2020, None]}),
            "VISITS": pa.table({"src": [1], "dst": [1]}),
        },
        use_csr=request.param,
    )


def test_node_keys_labels_and_with_alias(engine):
    result = engine.execute(
        "MATCH (p:person) WITH p AS person "
        "RETURN person.person_id AS id, keys(person) AS keys, labels(person) AS labels "
        "ORDER BY id"
    ).to_pylist()
    assert [sorted(row["keys"]) for row in result] == [
        ["age", "name", "person_id"],
        ["person_id"],
        ["age", "name", "person_id"],
    ]
    assert [row["labels"] for row in result] == [["Person"]] * 3


def test_optional_node_and_relationship_keys(engine):
    result = engine.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS]->(q:Person) "
        "RETURN p.person_id AS id, keys(q) AS node_keys, labels(q) AS labels, "
        "keys(r) AS edge_keys ORDER BY id"
    ).to_pylist()
    for row in result:
        for key in ("node_keys", "edge_keys"):
            if row[key] is not None:
                row[key] = sorted(row[key])
    assert result == [
        {
            "id": 1,
            "node_keys": ["person_id"],
            "labels": ["Person"],
            "edge_keys": ["dst", "since", "src"],
        },
        {
            "id": 2,
            "node_keys": ["age", "name", "person_id"],
            "labels": ["Person"],
            "edge_keys": ["dst", "src"],
        },
        {"id": 3, "node_keys": None, "labels": None, "edge_keys": None},
    ]


def test_relationship_keys_with_alias(engine):
    result = engine.execute(
        "MATCH (:Person)-[r:KNOWS]->(:Person) WITH r AS edge "
        "RETURN edge.src AS src, keys(edge) AS keys ORDER BY src"
    ).to_pylist()
    assert [sorted(row["keys"]) for row in result] == [
        ["dst", "since", "src"],
        ["dst", "src"],
    ]


def test_relationship_label_keeps_the_typed_hop(engine):
    match = "MATCH (p:Person)-[r:KNOWS|VISITS]->() "
    ordinary = engine.explain(match + "RETURN p.person_id, r.src")
    inspected = engine.explain(match + "RETURN p.person_id, r.src, type(r), labels(r)")
    assert "PathMatchExec" not in inspected
    assert ("CsrExpandExec" in inspected) == ("CsrExpandExec" in ordinary)


@pytest.mark.parametrize("value", [{}, {"MixedKey": None, "name": "Alice"}, None])
def test_map_keys_keep_null_values(engine, value):
    result = engine.execute(
        "WITH $value AS map RETURN keys(map) AS keys", {"value": value}
    )
    keys = result.to_pylist()[0]["keys"]
    if value is None:
        assert keys is None
    else:
        assert sorted(keys) == sorted(value)


def test_labels_null_and_keys_literal(engine):
    result = engine.execute(
        "WITH null AS value RETURN labels(value) AS labels, keys({x: null}) AS keys"
    ).to_pylist()
    assert result == [{"labels": None, "keys": ["x"]}]


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (p:Person:City) RETURN labels(p) AS labels",
        "MATCH (p:Person:City) WITH p AS x RETURN labels(x) AS labels",
        "MATCH (:Person)-[:KNOWS|VISITS]->(p) RETURN labels(p) AS labels",
    ],
)
def test_labels_reject_mixed_label_bindings(engine, query):
    with pytest.raises((ValueError, NotImplementedError), match="mixed-label binding"):
        engine.execute(query)


@pytest.mark.parametrize(
    "query",
    [
        "RETURN keys(1) AS keys",
        "RETURN labels(1) AS labels",
        "RETURN labels({x: 1}) AS labels",
        "MATCH (p:Person) RETURN type(p)",
        "RETURN type(1)",
        "RETURN type({label: 'spoof'})",
        "RETURN type()",
        "RETURN type(null, null)",
        "MATCH (:Person)-[r:KNOWS*1..2]->(:Person) RETURN type(r)",
        "MATCH (:Person)-[r:KNOWS*1..1]->(:Person) WITH r AS x RETURN labels(x)",
        "MATCH p=(:Person)-[:KNOWS*1..2]->(:Person) "
        "RETURN type(head(relationships(p)))",
        "MATCH p=(:Person)-[:KNOWS*1..2]->(:Person) "
        "UNWIND relationships(p) AS edge RETURN labels(edge)",
        "RETURN keys() AS keys",
        "RETURN labels(null, null) AS labels",
    ],
)
def test_entity_function_invalid_arguments(engine, query):
    with pytest.raises((ValueError, RuntimeError)):
        engine.execute(query)


@pytest.fixture(params=["arrow", "lance"])
def interest_graph(request, tmp_path):
    builder = GraphConfig.builder().node_table("Entity", "id")
    tables = {"Entity": pa.table({"id": [1, 2, 3]})}
    for kind, src, fraction in [
        ("Owns_Minerals", 1, 0.5),
        ("Holds_WI", 2, 1.0),
        ("Holds_ORRI", 1, None),
    ]:
        builder = builder.rel_table(kind, "Entity", "Entity", "src", "dst")
        tables[kind] = pa.table(
            {
                "src": [src],
                "dst": [3],
                "fraction": pa.array([fraction], type=pa.float64()),
                "type": ["user property"],
                "label": ["another property"],
            }
        )
    sources = (
        tables
        if request.param == "arrow"
        else {k: lance.write_dataset(v, tmp_path / k) for k, v in tables.items()}
    )
    return builder.build(), sources


@pytest.mark.parametrize("csr", [False, True])
@pytest.mark.parametrize(
    ("pattern", "kinds"),
    [
        (":owns_minerals", ["Owns_Minerals"]),
        (":owns_minerals|holds_orri", ["Holds_ORRI", "Owns_Minerals"]),
        ("", ["Holds_ORRI", "Owns_Minerals", "Holds_WI"]),
    ],
)
def test_fixed_relationship_labels(interest_graph, csr, pattern, kinds):
    config, sources = interest_graph
    engine = CypherEngine(config, sources, use_csr=csr)
    result = engine.execute(
        f"MATCH (h:Entity)-[r{pattern}]->(:Entity {{id: 3}}) "
        "WITH h.id AS holder, r AS edge "
        "RETURN holder, type(edge) AS kind, labels(edge) AS labels, "
        "edge.fraction AS fraction, edge.type AS property ORDER BY holder, kind"
    ).to_pylist()
    interests = {
        "Owns_Minerals": (1, 0.5),
        "Holds_ORRI": (1, None),
        "Holds_WI": (2, 1.0),
    }
    assert result == [
        {
            "holder": interests[k][0],
            "kind": k,
            "labels": [k],
            "fraction": interests[k][1],
            "property": "user property",
        }
        for k in kinds
    ]
    projected = engine.execute(
        f"MATCH (h:Entity)-[r{pattern}]->(:Entity {{id: 3}}) "
        "WITH h.id AS holder, type(r) AS kind, labels(r) AS labels "
        "RETURN kind, labels ORDER BY holder, kind"
    ).to_pylist()
    assert projected == [{"kind": k, "labels": [k]} for k in kinds]


@pytest.mark.parametrize("csr", [False, True])
def test_relationship_label_filter_and_sort(interest_graph, csr):
    config, sources = interest_graph
    engine = CypherEngine(config, sources, use_csr=csr)
    result = engine.execute(
        "MATCH (:Entity)-[r:Owns_Minerals|Holds_WI|Holds_ORRI]->(:Entity) "
        "WHERE type(r) <> 'Holds_WI' WITH r AS e "
        "RETURN e.fraction AS fraction ORDER BY type(e)"
    ).to_pylist()
    assert result == [{"fraction": None}, {"fraction": 0.5}]
    query = (
        "MATCH (:Entity)-[r:Holds_WI|Owns_Minerals]->(:Entity) "
        "WHERE type(r) = $kind RETURN r.fraction AS fraction"
    )
    for kind, fraction in [("Holds_WI", 1.0), ("Owns_Minerals", 0.5)]:
        assert engine.execute(query, {"kind": kind}).to_pylist() == [
            {"fraction": fraction}
        ]


def test_relationship_keys_and_with_scope(interest_graph):
    config, sources = interest_graph
    engine = CypherEngine(config, sources)
    result = engine.execute(
        "MATCH (:Entity)<-[r:Holds_ORRI|Holds_WI]-(:Entity) "
        "WITH r AS e WHERE head(labels(e)) = 'Holds_ORRI' "
        "RETURN keys(e) AS keys, type(e) AS kind"
    ).to_pylist()
    assert result[0]["kind"] == "Holds_ORRI"
    assert sorted(result[0]["keys"]) == ["dst", "label", "src", "type"]
    assert len(result) == 1
    # Rebinding r to a scalar ends its relationship scope.
    with pytest.raises((ValueError, RuntimeError)):
        engine.execute(
            "MATCH (:Entity)-[r:Owns_Minerals]->(:Entity) WITH 42 AS r RETURN type(r)"
        )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [("type(r)", "KNOWS"), ("labels(r)", ["KNOWS"])],
)
def test_unaliased_relationship_group_key(engine, expression, expected):
    result = engine.execute(
        f"MATCH (:Person)-[r:KNOWS]->(:Person) RETURN {expression}, count(*) AS n"
    )
    assert result.column_names == [expression, "n"]
    assert result.to_pylist() == [{expression: expected, "n": 2}]


def test_unaliased_aggregate_over_relationship_type(engine):
    result = engine.execute(
        "MATCH (:Person)-[r:KNOWS|VISITS]->() RETURN count(DISTINCT type(r))"
    )
    # Nested function arguments use "expr" in generated aggregate column names.
    assert result.to_pylist() == [{"count(DISTINCT expr)": 2}]


def test_relationship_label_groups_and_simultaneous_renames(interest_graph):
    config, sources = interest_graph
    engine = CypherEngine(config, sources)
    rows = engine.execute(
        "MATCH (:Entity)-[r:Holds_WI|Owns_Minerals]->(:Entity) "
        "WITH r AS e, 42 AS __lance_csr_type__e "
        "RETURN type(e) AS KiNd, count(*) AS n, __lance_csr_type__e AS marker "
        "ORDER BY kind"
    ).to_pylist()
    assert rows == [
        {"kind": "Holds_WI", "n": 1, "marker": 42},
        {"kind": "Owns_Minerals", "n": 1, "marker": 42},
    ]
    assert engine.execute(
        "MATCH (:Entity)-[r:Holds_WI|Owns_Minerals]->(:Entity) "
        "RETURN DISTINCT type(r) AS kind ORDER BY kind"
    ).to_pylist() == [{"kind": "Holds_WI"}, {"kind": "Owns_Minerals"}]


def test_type_null_and_flat_relationship_columns(interest_graph):
    config, sources = interest_graph
    engine = CypherEngine(config, sources)
    assert engine.execute("RETURN type(null) AS kind").to_pylist() == [{"kind": None}]
    assert engine.execute("RETURN 9 AS __lance_csr_type__r").to_pylist() == [
        {"__lance_csr_type__r": 9}
    ]
    assert engine.execute(
        "MATCH (:Entity)-[r:Owns_Minerals]->(:Entity) "
        "WITH r AS edge, 9 AS `@type__edge` "
        "RETURN `@type__edge` AS marker, type(edge) AS kind"
    ).to_pylist() == [{"marker": 9, "kind": "Owns_Minerals"}]
    assert engine.execute(
        "MATCH (:Entity)-[r:Owns_Minerals]->(:Entity) "
        "RETURN r.type AS __lance_csr_type__r, type(r) AS kind"
    ).to_pylist() == [{"__lance_csr_type__r": "user property", "kind": "Owns_Minerals"}]
    row = engine.execute(
        "MATCH (:Entity)-[r:Holds_ORRI|Holds_WI]->(:Entity) "
        "RETURN r, type(r) AS kind ORDER BY kind LIMIT 1"
    ).to_pylist()[0]
    assert row == {
        "r__src": 1,
        "r__dst": 3,
        "r__type": "user property",
        "r__label": "another property",
        "r__fraction": None,
        "kind": "Holds_ORRI",
    }


@pytest.mark.parametrize("pattern", [":Owns_Minerals", ":Owns_Minerals|Holds_ORRI", ""])
@pytest.mark.parametrize("csr", [False, True])
def test_optional_relationship_labels(interest_graph, csr, pattern):
    config, sources = interest_graph
    engine = CypherEngine(config, sources, use_csr=csr)
    result = engine.execute(
        f"MATCH (h:Entity) OPTIONAL MATCH (h)-[r{pattern}]->(:Entity) "
        "WITH h.id AS holder, r AS e "
        "RETURN holder, labels(e) AS labels, type(e) AS kind ORDER BY holder, kind"
    ).to_pylist()
    expected = [(1, "Owns_Minerals"), (2, None), (3, None)]
    if pattern != ":Owns_Minerals":
        expected.insert(0, (1, "Holds_ORRI"))
    if not pattern:
        expected[-2] = (2, "Holds_WI")
    assert result == [
        {"holder": holder, "labels": [kind] if kind else None, "kind": kind}
        for holder, kind in expected
    ]


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (n) RETURN n",
        "MATCH (n) WHERE n.name = 'Alice' RETURN n.name",
        "MATCH (n) RETURN count(*) AS c",
    ],
)
def test_unlabeled_node_asks_for_a_label(engine, query):
    with pytest.raises(ValueError, match=r"add a label, e\.g\. `\(n:Person\)`"):
        engine.execute(query)
