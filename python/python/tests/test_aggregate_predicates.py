# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Aggregate predicates must preserve empty-input and null semantics."""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig


@pytest.fixture(params=["arrow", "lance"])
def graph(request, tmp_path):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("Post", "id")
        .rel_table("likePost", "Person", "Post", "src", "dst")
        .build()
    )
    tables = {
        "Person": pa.table(
            {
                "id": [1, 2, 3],
                "firstname": ["Bill", "Bill", "Other"],
                "lastname": ["Moore", "Moore", "Person"],
                "score": [None, 2, 2],
            }
        ),
        "Post": pa.table({"id": [1]}),
        "likePost": pa.table({"src": [1, 1, 2], "dst": [1, 1, 1]}),
    }
    sources = (
        tables
        if request.param == "arrow"
        else {
            name: lance.write_dataset(table, tmp_path / f"{name}.lance")
            for name, table in tables.items()
        }
    )
    return config, tables, sources


def execute(graph, mode, query):
    config, tables, sources = graph
    if mode == "legacy":
        return CypherQuery(query).with_config(config).execute(tables)
    engine = CypherEngine(config, sources, use_csr="csr" in mode)
    if mode.startswith("stream"):
        return engine.execute_batches(query).read_all()
    return engine.execute(query)


@pytest.mark.parametrize("mode", ["legacy", "join", "csr", "stream-join", "stream-csr"])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("MATCH (p:Person) RETURN count(DISTINCT p.id) > 0 AS x", [{"x": True}]),
        (
            "MATCH (p:Person) WHERE p.id = 99 RETURN count(DISTINCT p.id) AS x",
            [{"x": 0}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 99 RETURN count(DISTINCT p.id) > 0 AS x",
            [{"x": False}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 1 RETURN count(DISTINCT p.id) > 0 AS x",
            [{"x": True}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 99 "
            "RETURN NOT (count(DISTINCT p.id) > 0) AS x",
            [{"x": True}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 99 "
            "RETURN CASE WHEN count(*) > 0 THEN 1 ELSE 0 END AS x",
            [{"x": 0}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 99 "
            "RETURN count(DISTINCT p.id) > 0 OR count(*) = 0 AS x",
            [{"x": True}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 99 RETURN sum(p.score) IS NULL AS x",
            [{"x": True}],
        ),
        (
            "MATCH (p:Person) WHERE p.id = 99 "
            "WITH count(DISTINCT p.id) > 0 AS x RETURN x",
            [{"x": False}],
        ),
        (
            "MATCH (p:Person) RETURN count(DISTINCT p.score) AS x, count(p.score) AS y",
            [{"x": 1, "y": 2}],
        ),
        (
            "MATCH (p:Post)<-[:likePost]-(p2:Person) "
            'WHERE p2.firstname = "Bill" AND p2.lastname = "Moore" AND p.id = 2 '
            "RETURN COUNT(DISTINCT p.id) > 0 AS liked",
            [{"liked": False}],
        ),
        (
            "MATCH (p:Post)<-[:likePost]-(p2:Person) "
            'WHERE p2.firstname = "Bill" AND p2.lastname = "Moore" AND p.id = 1 '
            "RETURN COUNT(DISTINCT p.id) AS liked",
            [{"liked": 1}],
        ),
    ],
)
def test_aggregate_predicates(graph, mode, query, expected):
    result = execute(graph, mode, query)
    assert result.to_pylist() == expected
    for field, value in zip(result.schema, expected[0].values()):
        assert field.type == (pa.bool_() if isinstance(value, bool) else pa.int64())


@pytest.mark.parametrize("use_csr", [False, True])
def test_cached_aggregate_predicates(graph, use_csr):
    config, _, sources = graph
    engine = CypherEngine(config, sources, use_csr=use_csr)
    query = "MATCH (p:Person) WHERE p.id = $id RETURN count(DISTINCT p.id) > 0 AS x"
    for value, expected in [(1, True), (99, False), (2, True), (99, False)]:
        result = (
            engine.execute_batches(query, {"id": value}).read_all()
            if value == 99
            else engine.execute(query, {"id": value})
        )
        assert result.to_pylist() == [{"x": expected}]
