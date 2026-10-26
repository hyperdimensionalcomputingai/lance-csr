# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Integer list values retain precision; only vector operations convert to float32."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

LARGE_ID = 19791209300775


@pytest.fixture(params=[False, True], ids=["join", "csr"])
def engine(request):
    people = pa.table(
        {
            "id": [1131, LARGE_ID],
            "ids": pa.array([[1131, LARGE_ID], [1, 2]], type=pa.list_(pa.int64())),
            "embedding": pa.array(
                [[1, 0, 1], [0, 1, 0]], type=pa.list_(pa.float32(), 3)
            ),
        }
    )
    config = GraphConfig.builder().node_table("Person", "id").build()
    return CypherEngine(config, {"Person": people}, use_csr=request.param)


def test_unwind_large_ids(engine):
    result = engine.execute(
        f"UNWIND [1131, {LARGE_ID}] AS id "
        "MATCH (p:Person {id: id}) RETURN p.id AS id ORDER BY id"
    )
    assert result.to_pylist() == [{"id": 1131}, {"id": LARGE_ID}]
    assert result.schema.field("id").type == pa.int64()


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("RETURN [1, 2, 3] AS l", [1, 2, 3]),
        (f"WITH [1131, {LARGE_ID}] AS l RETURN l", [1131, LARGE_ID]),
        (f"RETURN [x IN [1131, {LARGE_ID}] | x] AS l", [1131, LARGE_ID]),
        (f"RETURN [-{LARGE_ID}, -1, 0] AS l", [-LARGE_ID, -1, 0]),
    ],
)
def test_integer_list_values(engine, query, expected):
    result = engine.execute(query)
    assert result.column(0).to_pylist() == [expected]
    assert pa.types.is_int64(result.schema.field(0).type.value_type)


def test_integer_list_column_comparison(engine):
    result = engine.execute(
        f"MATCH (p:Person) WHERE p.ids = [1131, {LARGE_ID}] RETURN p.id AS id"
    )
    assert result.to_pylist() == [{"id": 1131}]


@pytest.mark.parametrize("metric", ["l2", "cosine", "dot"])
@pytest.mark.parametrize("function", ["vector_distance", "vector_similarity"])
def test_integer_query_vector(engine, metric, function):
    query = (
        f"MATCH (p:Person) RETURN p.id AS id, "
        f"{function}(p.embedding, {{vector}}, {metric}) AS score ORDER BY id"
    )
    integer = engine.execute(query.format(vector="[1, 0, 1]"))
    decimal = engine.execute(query.format(vector="[1.0, 0.0, 1.0]"))
    assert integer.equals(decimal)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("RETURN 1 AS n", [{"n": 1}]),
        ("RETURN NULL AS n", [{"n": None}]),
        ("RETURN [] AS l", [{"l": []}]),
        ("RETURN count(*) AS n", [{"n": 1}]),
        ("RETURN 1 AS n LIMIT 0", []),
        ("RETURN 1 AS n SKIP 1", []),
        ("WITH 2 AS n RETURN n + 1 AS n", [{"n": 3}]),
        ("UNWIND [] AS n RETURN n", []),
    ],
)
def test_singleton_queries(engine, query, expected):
    assert engine.execute(query).to_pylist() == expected


def test_singleton_parameters_and_unknown_variables(engine):
    assert engine.execute("RETURN $n AS n", {"n": LARGE_ID}).to_pylist() == [
        {"n": LARGE_ID}
    ]
    with pytest.raises(
        ValueError, match="(?i)(unknown|undefined|not defined|not found)"
    ):
        engine.execute("RETURN missing AS n")
