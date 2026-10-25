# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""The optional partition target preserves query results and snapshot behavior."""

import inspect

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture
def graph():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    return config, {
        "Person": pa.table({"id": [1, 2, 3, 4]}),
        "KNOWS": pa.table({"src": [1, 1, 2], "dst": [2, 3, 4]}),
    }


@pytest.mark.parametrize("partitions", [None, 1, 2])
@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize("storage", ["arrow", "lance"])
def test_partition_targets_preserve_queries_and_refresh(
    graph, partitions, use_csr, storage, tmp_path
):
    config, data = graph
    if storage == "lance":
        data = {
            name: lance.write_dataset(table, tmp_path / f"{name}.lance")
            for name, table in data.items()
        }
    engine = CypherEngine(config, data, use_csr, target_partitions=partitions)
    query = (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = $start "
        "RETURN b.id AS id ORDER BY id"
    )
    for start, ids in [(1, [2, 3]), (2, [4]), (1, [2, 3])]:
        expected = [{"id": value} for value in ids]
        assert engine.execute(query, {"start": start}).to_pylist() == expected
        assert (
            engine.execute_batches(query, {"start": start}).read_all().to_pylist()
            == expected
        )
    assert engine.execute(
        "MATCH (p:Person) RETURN count(*) AS n, sum(p.id) AS total"
    ).to_pylist() == [{"n": 4, "total": 10}]
    scan = "MATCH (p:Person) RETURN p.id AS id ORDER BY id"
    reader = engine.execute_batches(scan)
    if storage == "lance":
        lance.write_dataset(
            pa.table({"id": [5]}), tmp_path / "Person.lance", mode="append"
        )
    engine.refresh()
    assert reader.read_all().to_pylist() == [{"id": value} for value in range(1, 5)]
    count = 5 if storage == "lance" else 4
    assert engine.execute(scan).to_pylist() == [
        {"id": value} for value in range(1, count + 1)
    ]


@pytest.mark.parametrize("value", [True, False, 1.0, "1", [], {}])
def test_partition_target_rejects_non_integers(graph, value):
    config, data = graph
    with pytest.raises(TypeError, match="target_partitions"):
        CypherEngine(config, data, target_partitions=value)


@pytest.mark.parametrize("value", [0, -1, -(1 << 128), 1 << 128])
def test_partition_target_rejects_nonpositive_and_overflow(graph, value):
    config, data = graph
    with pytest.raises(ValueError, match="target_partitions"):
        CypherEngine(config, data, target_partitions=value)


def test_partition_target_is_keyword_only_and_default_is_none(graph):
    config, data = graph
    parameter = inspect.signature(CypherEngine).parameters["target_partitions"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None
    with pytest.raises(TypeError):
        CypherEngine(config, data, False, 1)
    assert CypherEngine(config, data, False).execute("RETURN 1 AS n").to_pylist() == [
        {"n": 1}
    ]
