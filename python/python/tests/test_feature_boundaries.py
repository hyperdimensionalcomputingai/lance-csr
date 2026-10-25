# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Catalog, path and template boundaries across the prerequisite feature set."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig


@pytest.fixture(params=[False, True])
def graph(request):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    data = {
        "Person": pa.table(
            {
                "id": [10, 20, 30, 40],
                "name": ["A", "B", "C", "D"],
                "age": [20, 30, 40, 50],
            }
        ),
        "KNOWS": pa.table({"src": [10, 10, 10, 20], "dst": [20, 20, 30, 40]}),
    }
    return CypherEngine(config, data, use_csr=request.param), config, data


def test_rich_path_parameters_bind_freshly(graph):
    engine, _, _ = graph
    query = (
        "MATCH path = (a:Person {id: $start})-[:KNOWS]->(b:Person) "
        "RETURN nodes(path) AS ids ORDER BY ids"
    )
    for start, expected in [
        (10, [[10, 20], [10, 20], [10, 30]]),
        (20, [[20, 40]]),
        (10, [[10, 20], [10, 20], [10, 30]]),
    ]:
        assert engine.execute(query, {"start": start}).column(0).to_pylist() == expected


def test_rich_path_reader_retains_snapshot_after_refresh(graph, tmp_path):
    import lance

    original, config, data = graph
    paths = {name: str(tmp_path / f"{name}.lance") for name in data}
    datasets = {
        name: lance.write_dataset(table, paths[name]) for name, table in data.items()
    }
    engine = CypherEngine(config, datasets, use_csr=bool(original.csr_stats()))
    query = (
        "MATCH path = (a:Person {id: $start})-[:KNOWS]->(b:Person) "
        "RETURN nodes(path) AS ids ORDER BY ids"
    )
    old = engine.execute_batches(query, {"start": 20})
    lance.write_dataset(
        pa.table({"src": [20], "dst": [30]}), paths["KNOWS"], mode="append"
    )
    engine.refresh()
    assert engine.execute(query, {"start": 20}).column(0).to_pylist() == [
        [20, 30],
        [20, 40],
    ]
    assert old.read_all().column(0).to_pylist() == [[20, 40]]
    assert engine.execute(query, {"start": 10}).column(0).to_pylist() == [
        [10, 20],
        [10, 20],
        [10, 30],
    ]


def test_union_rich_paths_and_one_shot_queries(graph):
    engine, config, data = graph
    body = "MATCH p = (a:Person {id: 20})-[:KNOWS]->(b:Person) RETURN p"
    expected = engine.execute(body).to_pylist()
    assert len(expected) == 1
    assert engine.execute(body + " UNION " + body).to_pylist() == expected
    assert engine.execute(body + " UNION ALL " + body).to_pylist() == expected * 2
    assert CypherQuery(body).with_config(config).execute(data).to_pylist() == expected


def test_union_parameterized_counts_replan_in_each_branch(graph):
    engine, _, _ = graph
    text = (
        "RETURN 0 AS id UNION ALL MATCH (p:Person) "
        "RETURN p.id AS id ORDER BY id LIMIT $count"
    )
    for count in [1, 3, 0, 2]:
        expected = [{"id": value} for value in [0, *[10, 20, 30, 40][:count]]]
        actual = engine.execute(text, {"count": count}).to_pylist()
        assert sorted(actual, key=lambda row: row["id"]) == expected


def test_later_union_branch_keeps_catalog_type_checks(graph):
    engine, _, _ = graph
    with pytest.raises(ValueError, match="Type mismatch"):
        engine.execute("RETURN 0 AS x UNION MATCH (p:Person) RETURN p.name + 1 AS x")


def test_metadata_includes_later_reading_stages():
    query = CypherQuery(
        "MATCH (a:Person) WITH a MATCH (a)-[:LIVES]->(b:Place) RETURN b"
    )
    assert set(query.node_labels()) == {"Person", "Place"}
    assert query.relationship_types() == ["LIVES"]
    assert query.variables() == ["a", "b"]
