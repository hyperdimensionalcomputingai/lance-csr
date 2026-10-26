# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Repeated execution must bind current values before filters reach CSR scans."""

from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True])
def engine(request):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    return CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1, 2, 3], "age": [20, 30, 40]}),
            "KNOWS": pa.table({"src": [1, 1, 2], "dst": [2, 3, 3]}),
        },
        use_csr=request.param,
    )


def test_cached_target_filters_bind_each_value(engine):
    query = (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) "
        "WHERE a.id = $start AND b.age >= $minimum RETURN b.id AS id ORDER BY id"
    )
    for minimum, ids in [(20, [2, 3]), (35, [3]), (45, []), (20, [2, 3])]:
        params = {"start": 1, "minimum": minimum}
        expected = [{"id": value} for value in ids]
        assert engine.execute(query, params).to_pylist() == expected
        assert engine.execute_batches(query, params).read_all().to_pylist() == expected


def test_cached_parameter_types_and_container_fallback(engine):
    query = "MATCH (p:Person) WHERE p.id = 1 RETURN $value AS value"
    for value in [1, 2, None, 1.5, "Ada", "Bob", True, False, [1, 2], [3], {"x": 1}]:
        assert engine.execute(query, {"value": value}).to_pylist() == [{"value": value}]
    membership = "MATCH (p:Person) WHERE p.id IN $ids RETURN p.id AS id ORDER BY id"
    for ids in [[1, 2], [3], []]:
        assert engine.execute(membership, {"ids": ids}).to_pylist() == [
            {"id": value} for value in ids
        ]


def test_unaliased_parameters_keep_existing_column_names(engine):
    query = "MATCH (p:Person) WHERE p.id = 1 RETURN $value"
    first = engine.execute(query, {"value": 1})
    second = engine.execute(query, {"value": 2})
    assert first.column_names == ["expr"]
    assert second.column_names == ["expr"]
    assert first.column(0).to_pylist() == [1]
    assert second.column(0).to_pylist() == [2]


def test_cached_queries_keep_concurrent_parameters_separate(engine):
    query = "MATCH (p:Person) WHERE p.id = $id RETURN p.age AS age"
    engine.execute(query, {"id": 1})

    def execute(person_id):
        return engine.execute(query, {"id": person_id}).to_pylist()

    ids = [1, 2, 3] * 8
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(execute, ids))
    assert results == [[{"age": 10 * (person_id + 1)}] for person_id in ids]


def test_cached_query_still_rejects_missing_or_wrong_types(engine):
    query = "MATCH (p:Person) WHERE p.id = $id RETURN p.age AS age"
    engine.execute(query, {"id": 1})
    with pytest.raises(ValueError, match="[Pp]arameter"):
        engine.execute(query)
    with pytest.raises(ValueError, match="Type mismatch"):
        engine.execute(query, {"id": True})


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (p:Person) RETURN p.id AS id ORDER BY id LIMIT $count",
        "MATCH (p:Person) WITH p ORDER BY p.id LIMIT $count RETURN p.id AS id",
        "MATCH (p:Person) WITH p WITH p ORDER BY p.id LIMIT $count RETURN p.id AS id",
    ],
)
def test_parameterized_limits_replan_each_count(engine, query):
    for count in [1, 3, 0, 2]:
        expected = [{"id": value} for value in range(1, count + 1)]
        assert engine.execute(query, {"count": count}).to_pylist() == expected
        assert (
            engine.execute_batches(query, {"count": count}).read_all().to_pylist()
            == expected
        )
