# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Container parameters share literal semantics without losing integer IDs."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig

BIG = 19791209300775
CONFIG = GraphConfig.builder().node_table("Person", "id").build()
TABLES = {
    "Person": pa.table(
        {
            "id": [1, BIG],
            "embedding": pa.array([[1, 0], [0, 1]], type=pa.list_(pa.float32(), 2)),
        }
    )
}


@pytest.fixture(params=[False, True], ids=["join", "csr"])
def engine(request):
    return CypherEngine(CONFIG, TABLES, use_csr=request.param)


def test_list_ids_and_streams(engine):
    query = "UNWIND $ids AS id MATCH (p:Person {id: id}) RETURN p.id AS id ORDER BY id"
    params = {"ids": [1, BIG]}
    expected = [{"id": 1}, {"id": BIG}]
    assert engine.execute(query, params).to_pylist() == expected
    assert engine.execute_batches(query, params).read_all().to_pylist() == expected
    assert (
        engine.execute(
            "MATCH (p:Person) WHERE p.id IN $ids RETURN p.id AS id ORDER BY id", params
        ).to_pylist()
        == expected
    )
    assert "Projection" in engine.explain(query, params)


@pytest.mark.parametrize(
    "value",
    [
        [],
        [None],
        [1, BIG],
        [[1, BIG], []],
        {},
        {"name": "a", "ids": [1, BIG], "missing": None},
    ],
)
def test_container_return_and_parameter_roundtrip(engine, value):
    assert engine.execute("RETURN $value AS value", {"value": value}).to_pylist() == [
        {"value": value}
    ]
    query = (
        CypherQuery("RETURN $value AS value")
        .with_config(CONFIG)
        .with_parameter("value", value)
    )
    assert query.parameters() == {"value": value}
    assert query.execute(TABLES).to_pylist() == [{"value": value}]


def test_map_field_and_comprehension(engine):
    assert engine.execute(
        "WITH $value AS m RETURN m.name AS name, m.ids AS ids",
        {"value": {"name": "a", "ids": [BIG]}},
    ).to_pylist() == [{"name": "a", "ids": [BIG]}]
    assert engine.execute(
        "RETURN [x IN $ids WHERE x > 1 | x] AS ids", {"ids": [1, BIG]}
    ).to_pylist() == [{"ids": [BIG]}]


@pytest.mark.parametrize("vector", [[1, 0], [1.0, 0.0]])
def test_vector_parameter(engine, vector):
    query = (
        "MATCH (p:Person) RETURN p.id AS id, "
        "vector_distance(p.embedding, $v, l2) AS d ORDER BY id"
    )
    assert engine.execute(query, {"v": vector}).equals(
        engine.execute(query.replace("$v", "[1.0, 0.0]"))
    )


def test_mixed_list_follows_literal_coercion(engine):
    assert engine.execute("RETURN $v AS v", {"v": [1, "a"]}).to_pylist() == [
        {"v": ["1", "a"]}
    ]


@pytest.mark.parametrize(
    "value", [{1: "bad"}, [object()], {"nested": {"bad"}}, [2**64], 2**64]
)
def test_invalid_container_values(engine, value):
    with pytest.raises(ValueError):
        engine.execute("RETURN $v AS v", {"v": value})


def test_recursive_container_is_rejected(engine):
    values = []
    values.append(values)
    with pytest.raises(ValueError, match="nesting|cyclic"):
        engine.execute("RETURN $v AS v", {"v": values})
