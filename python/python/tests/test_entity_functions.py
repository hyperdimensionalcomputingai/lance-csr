# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Entity functions retain graph binding information and map null semantics."""

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
        "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN labels(r) AS labels",
        "RETURN keys() AS keys",
        "RETURN labels(null, null) AS labels",
    ],
)
def test_entity_function_invalid_arguments(engine, query):
    with pytest.raises((ValueError, RuntimeError)):
        engine.execute(query)
