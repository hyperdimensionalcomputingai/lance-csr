# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""SqlEngine with a graph: GRAPH_TABLE on the CSR, parameters, Lance pushdown."""

import time

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig, SqlEngine

PEOPLE = pa.table(
    {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carol", "Dan"],
        "age": [28, 34, 29, 42],
    }
)
KNOWS = pa.table({"src_id": [1, 1, 2, 3, 4], "dst_id": [2, 3, 3, 4, 1]})

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
    .build()
)

FRIENDS_OF_FRIENDS = """
    SELECT g.a, g.c, p.age
    FROM GRAPH_TABLE (
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
        WHERE a.age < $max_age
        COLUMNS (a.name AS a, c.name AS c, c.id AS c_id)
    ) g
    JOIN person p ON p.id = g.c_id
    WHERE p.age > $min_age
    ORDER BY g.a, g.c
"""


@pytest.fixture(params=["arrow", "lance"])
def datasets(request, tmp_path):
    tables = {"Person": PEOPLE, "KNOWS": KNOWS}
    if request.param == "arrow":
        return tables
    return {
        name: lance.write_dataset(table, tmp_path / f"{name}.lance")
        for name, table in tables.items()
    }


@pytest.mark.parametrize("use_csr", [True, False])
def test_graph_table_with_parameters(datasets, use_csr):
    engine = SqlEngine(datasets, CONFIG, use_csr=use_csr)
    result = engine.execute(FRIENDS_OF_FRIENDS, {"max_age": 40, "MIN_AGE": 28})
    assert result.to_pylist() == [
        {"a": "Alice", "c": "Carol", "age": 29},
        {"a": "Alice", "c": "Dan", "age": 42},
        {"a": "Bob", "c": "Dan", "age": 42},
    ]
    # The cached template binds new values.
    result = engine.execute(FRIENDS_OF_FRIENDS, {"max_age": 30, "min_age": 0})
    assert [(r["a"], r["c"]) for r in result.to_pylist()] == [
        ("Alice", "Carol"),
        ("Alice", "Dan"),
        ("Carol", "Alice"),
    ]
    plan = engine.explain(FRIENDS_OF_FRIENDS, {"max_age": 30, "min_age": 0})
    assert ("CsrExpandExec" in plan) == use_csr


def test_from_engine_shares_the_snapshot(datasets):
    cypher = CypherEngine(CONFIG, datasets, use_csr=True)
    engine = SqlEngine.from_engine(cypher)
    result = engine.execute(
        "SELECT count(*) AS n FROM graph_table("
        "'MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.name = $name RETURN a.id')",
        {"name": "Carol"},
    )
    assert result.to_pylist() == [{"n": 2}]
    assert cypher.csr_stats()["KNOWS"]["edges"] == 5
    engine.checkout_latest()


def test_container_parameters_inside_graph_table(datasets):
    engine = SqlEngine(datasets, CONFIG)
    result = engine.execute(
        "SELECT * FROM GRAPH_TABLE (MATCH (a:Person) WHERE a.id IN $ids "
        "COLUMNS (a.name AS name)) ORDER BY name",
        {"ids": [3, 1]},
    )
    assert result.to_pylist() == [{"name": "Alice"}, {"name": "Carol"}]


def test_plain_sql_pushes_filters_into_lance(tmp_path):
    datasets = {
        "Person": lance.write_dataset(PEOPLE, tmp_path / "person.lance"),
        "KNOWS": lance.write_dataset(KNOWS, tmp_path / "knows.lance"),
    }
    query = "SELECT name FROM person WHERE age > $age ORDER BY name"
    for engine in (SqlEngine(datasets), SqlEngine(datasets, CONFIG)):
        result = engine.execute(query, {"age": 30})
        assert result.to_pylist() == [{"name": "Bob"}, {"name": "Dan"}]
        assert "full_filter=age > Int64(30)" in engine.explain(query, {"age": 30})


@pytest.mark.parametrize(
    "query",
    [
        "CREATE TABLE t AS SELECT 1",
        "INSERT INTO person VALUES (9, 'Eve', 1)",
        "SET datafusion.execution.batch_size = 1",
        "SELECT * FROM GRAPH_TABLE (MATCH (a:Person))",
        "SELECT * FROM person WHERE age > $missing",
    ],
)
def test_rejects_writes_bad_graph_tables_and_missing_values(query):
    engine = SqlEngine({"Person": PEOPLE, "KNOWS": KNOWS}, CONFIG)
    with pytest.raises(ValueError):
        engine.execute(query)


def test_graph_table_needs_a_config():
    engine = SqlEngine({"Person": PEOPLE})
    with pytest.raises(ValueError, match="GRAPH_TABLE needs a graph"):
        engine.execute("SELECT * FROM GRAPH_TABLE (MATCH (a:Person) COLUMNS (a.id))")
    with pytest.raises(ValueError, match="use_csr requires a config"):
        SqlEngine({"Person": PEOPLE}, use_csr=True)


def test_timeout():
    engine = SqlEngine({"Person": PEOPLE, "KNOWS": KNOWS}, CONFIG, use_csr=True)
    with pytest.raises(TimeoutError):
        engine.execute(FRIENDS_OF_FRIENDS, {"max_age": 40, "min_age": 0}, timeout=0)
    # Nested-loop joins (no equality to hash on) check the deadline while they run.
    big = pa.table({"x": list(range(2000))})
    slow = SqlEngine({"a": big, "b": big, "c": big})
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        slow.execute(
            "SELECT count(*) FROM a JOIN b ON a.x < b.x JOIN c ON b.x < c.x",
            timeout=0.2,
        )
    assert time.monotonic() - started < 5
    with pytest.raises(ValueError):
        engine.execute("SELECT 1", timeout=-1)
    assert engine.execute("SELECT 1 AS n", timeout=5).to_pylist() == [{"n": 1}]
