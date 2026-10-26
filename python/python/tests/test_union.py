# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Language UNION has independent scopes and a strict output-column contract."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig

CONFIG = (
    GraphConfig.builder().node_table("Person", "id").node_table("City", "id").build()
)
TABLES = {"Person": pa.table({"id": [1, 2, 2]}), "City": pa.table({"id": [2, 3]})}


@pytest.fixture(params=[False, True])
def engine(request):
    return CypherEngine(CONFIG, TABLES, use_csr=request.param)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("RETURN 1 AS n UNION RETURN 1 AS n", [1]),
        ("RETURN 1 AS n UNION ALL RETURN 1 AS n", [1, 1]),
        ("RETURN 1 AS n UNION ALL RETURN 1 AS n UNION RETURN 2 AS n", [1, 2]),
        ("RETURN 1 AS n UNION RETURN 1 AS n UNION ALL RETURN 1 AS n", [1, 1]),
        (
            "MATCH (p:Person) RETURN p.id AS n UNION MATCH (p:City) RETURN p.id AS n",
            [1, 2, 3],
        ),
        (
            "MATCH (p:Person) RETURN p.id AS n UNION ALL "
            "MATCH (p:City) RETURN p.id AS n",
            [1, 2, 2, 2, 3],
        ),
        ("RETURN NULL AS n UNION RETURN NULL AS n", [None]),
        ("RETURN NULL AS n UNION ALL RETURN 1 AS n", [None, 1]),
        ("RETURN 1 AS n LIMIT 0 UNION RETURN 2 AS n", [2]),
        (
            "UNWIND [1, 2] AS n RETURN n ORDER BY n DESC LIMIT 1 "
            "UNION ALL RETURN 3 AS n",
            [2, 3],
        ),
        ("RETURN 'UNION ALL' AS n UNION RETURN 'UNION' AS n", ["UNION", "UNION ALL"]),
    ],
)
def test_union_results(engine, query, expected):
    actual = engine.execute(query).column(0).to_pylist()
    assert sorted(actual, key=lambda v: (v is not None, v)) == sorted(
        expected, key=lambda v: (v is not None, v)
    )
    assert engine.execute_batches(query).read_all().num_rows == len(expected)


def test_numeric_promotion_and_parameters(engine):
    query = "RETURN $a AS n UNION ALL RETURN $b AS n"
    assert sorted(engine.execute(query, {"a": 1, "b": 2.5}).column(0).to_pylist()) == [
        1.0,
        2.5,
    ]
    one_shot = (
        CypherQuery(query)
        .with_config(CONFIG)
        .with_parameter("a", 1)
        .with_parameter("b", 2)
    )
    assert sorted(one_shot.execute(TABLES).column(0).to_pylist()) == [1, 2]


@pytest.mark.parametrize(
    "query",
    [
        "RETURN 1 AS x UNION RETURN 2 AS y",
        "RETURN 1 AS x UNION RETURN 2 AS x, 3 AS y",
        "RETURN 1 AS x UNION RETURN 'a' AS x",
        "WITH 1 AS x RETURN x UNION RETURN x",
        "RETURN 1 AS x UNION ALL",
    ],
)
def test_invalid_union(engine, query):
    with pytest.raises(ValueError):
        engine.execute(query)


def test_union_query_metadata():
    query = CypherQuery(
        "MATCH (p:Person) RETURN p.id AS n UNION MATCH (c:City) RETURN c.id AS n"
    )
    assert set(query.node_labels()) == {"City", "Person"}
    assert query.variables() == ["c", "p"]
    relationships = CypherQuery(
        "MATCH (p:Person)-[:X]->(c:City) RETURN p.id AS n "
        "UNION MATCH (c:City)-[:Y]->(p:Person) RETURN p.id AS n"
    )
    assert set(relationships.relationship_types()) == {"X", "Y"}


def test_union_nullability_does_not_depend_on_first_batch(engine):
    for query in [
        "RETURN null AS n UNION ALL RETURN 1 AS n",
        "RETURN 1 AS n UNION ALL RETURN null AS n",
        "RETURN $a AS n UNION ALL RETURN $b AS n",
    ]:
        for _ in range(5):
            table = engine.execute(query, {"a": None, "b": 1})
            assert table.schema.field("n").nullable
            assert sorted(table.column(0).to_pylist(), key=lambda n: n is None) == [
                1,
                None,
            ]
            stream = engine.execute_batches(query, {"a": None, "b": 1}).read_all()
            assert stream.schema.field("n").nullable
            assert sorted(stream.column(0).to_pylist(), key=lambda n: n is None) == [
                1,
                None,
            ]
