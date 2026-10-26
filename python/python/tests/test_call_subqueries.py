# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Read-only CALL uses explicit imports and evaluates its body per outer row."""

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


def test_call_top_k_preserves_node_alias_and_properties(graph):
    engine, config, data = graph
    query = (
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) "
        "RETURN f AS friend ORDER BY friend.age DESC LIMIT 1 } "
        "RETURN p.id AS id, friend.name AS name ORDER BY id"
    )
    expected = [{"id": 10, "name": "C"}, {"id": 20, "name": "D"}]
    assert engine.execute(query).to_pylist() == expected
    assert engine.execute_batches(query).read_all().to_pylist() == expected
    assert CypherQuery(query).with_config(config).execute(data).to_pylist() == expected
    assert "Apply" in engine.explain(query)


def test_call_aggregates_and_duplicates(graph):
    engine, _, _ = graph
    query = (
        "MATCH (p:Person) UNWIND [1, 1] AS duplicate CALL { WITH p "
        "MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS n } "
        "RETURN p.id AS id, n ORDER BY id"
    )
    assert engine.execute(query).to_pylist() == [
        {"id": person, "n": count}
        for person, count in [(10, 3), (20, 1), (30, 0), (40, 0)]
        for _ in range(2)
    ]
    query = (
        "MATCH (p:Person) WHERE p.id = 10 CALL { WITH p "
        "MATCH (p)-[:KNOWS]->(f:Person) RETURN f.id AS id } RETURN id ORDER BY id"
    )
    assert engine.execute(query).to_pylist() == [{"id": 20}, {"id": 20}, {"id": 30}]


def test_call_nested_parameters_are_fresh_and_do_not_collide(graph):
    engine, _, _ = graph
    query = (
        "UNWIND [1, 2] AS x CALL { WITH x CALL { WITH x "
        "RETURN x + $__call_1_0 AS y } RETURN y + 1 AS z } "
        "CALL { WITH z RETURN z * 2 AS answer } RETURN x, answer ORDER BY x"
    )
    for value in [10, 20, 10]:
        assert engine.execute(query, {"__call_1_0": value}).to_pylist() == [
            {"x": x, "answer": (x + value + 1) * 2} for x in [1, 2]
        ]


def test_call_nullable_scalar_import_and_second_with(graph):
    engine, _, _ = graph
    query = (
        "UNWIND [null, 2] AS x CALL { WITH x WITH coalesce(x, 7) AS value "
        "RETURN value + 1 AS answer } RETURN answer ORDER BY answer"
    )
    assert engine.execute(query).to_pylist() == [{"answer": 3}, {"answer": 8}]


def test_call_export_can_start_outer_traversal(graph):
    engine, _, _ = graph
    query = (
        "CALL { MATCH (p:Person) WHERE p.id = 10 RETURN p AS person } "
        "MATCH (person)-[:KNOWS]->(f:Person) RETURN f.name AS name ORDER BY name"
    )
    assert engine.execute(query).to_pylist() == [
        {"name": "B"},
        {"name": "B"},
        {"name": "C"},
    ]


def test_call_empty_outer_and_early_reader_close(graph):
    engine, _, _ = graph
    assert (
        engine.execute(
            "MATCH (p:Person) WHERE p.id < 0 "
            "CALL { RETURN 1 / 0 AS invalid } RETURN invalid"
        ).num_rows
        == 0
    )
    reader = engine.execute_batches(
        "MATCH (p:Person) CALL { WITH p UNWIND range(1, 10000) AS n RETURN n } RETURN n"
    )
    assert reader.read_next_batch().num_rows > 0
    reader.close()
    assert engine.execute("RETURN 1 AS ok").to_pylist() == [{"ok": 1}]


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("UNWIND [1] AS x CALL { RETURN x AS y } RETURN y", "Undefined variable"),
        ("UNWIND [1] AS x CALL { RETURN 2 AS x } RETURN x", "collides"),
        ("CALL { RETURN 1 + 2 } RETURN 1", "need an alias"),
        ("CALL { WITH missing RETURN missing AS x } RETURN x", "undefined variable"),
        (
            "UNWIND [1] AS x CALL { WITH x + 1 AS y RETURN y } RETURN y",
            "importing WITH",
        ),
        (
            "CALL { UNWIND [1] AS local RETURN 2 AS y } RETURN local",
            "Undefined variable",
        ),
    ],
)
def test_call_scope_errors(graph, query, message):
    engine, _, _ = graph
    with pytest.raises(ValueError, match=message):
        engine.execute(query)


def test_call_optional_entity_import_and_relationship_export(graph):
    engine, _, _ = graph
    query = (
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
        "CALL { WITH f RETURN f.name AS name } "
        "RETURN p.id AS id, name ORDER BY id, name"
    )
    assert engine.execute(query).to_pylist() == [
        {"id": 10, "name": "B"},
        {"id": 10, "name": "B"},
        {"id": 10, "name": "C"},
        {"id": 20, "name": "D"},
        {"id": 30, "name": None},
        {"id": 40, "name": None},
    ]
    query = (
        "CALL { MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN r AS edge } "
        "CALL { WITH edge RETURN edge.dst AS destination } "
        "RETURN destination ORDER BY destination"
    )
    assert engine.execute(query).to_pylist() == [
        {"destination": 20},
        {"destination": 20},
        {"destination": 30},
        {"destination": 40},
    ]


def test_call_preserves_container_imports(graph):
    engine, _, _ = graph
    assert engine.execute(
        "UNWIND [{values: [1, 2]}, {values: [3]}] AS item "
        "CALL { WITH item UNWIND item.values AS n RETURN sum(n) AS total } "
        "RETURN total ORDER BY total"
    ).to_pylist() == [{"total": 3}, {"total": 3}]


def test_call_scalar_import_does_not_import_entity_shaped_names(graph):
    engine, _, _ = graph
    assert engine.execute(
        "WITH 1 AS x, 2 AS x__other CALL { WITH x RETURN x AS y } RETURN y"
    ).to_pylist() == [{"y": 1}]


def test_call_parameterized_child_limit_is_per_row_and_fresh(graph):
    engine, _, _ = graph
    query = (
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) "
        "RETURN f.id AS friend ORDER BY friend LIMIT $take } "
        "RETURN p.id AS id, friend ORDER BY id, friend"
    )
    for take, expected in [
        (1, [{"id": 10, "friend": 20}, {"id": 20, "friend": 40}]),
        (0, []),
        (
            2,
            [
                {"id": 10, "friend": 20},
                {"id": 10, "friend": 20},
                {"id": 20, "friend": 40},
            ],
        ),
    ]:
        assert engine.execute(query, {"take": take}).to_pylist() == expected


@pytest.mark.parametrize(("union", "repeats"), [("UNION", 1), ("UNION ALL", 2)])
def test_call_union_distinct_is_per_outer_row(graph, union, repeats):
    engine, _, _ = graph
    query = (
        "UNWIND [1, 1, 2] AS x CALL { WITH x RETURN x + $value AS n "
        f"{union} WITH x RETURN x + $value AS n }} RETURN x, n ORDER BY x"
    )
    for value in [10, 20, 10]:
        expected = [{"x": x, "n": x + value} for x in [1, 1, 2] for _ in range(repeats)]
        assert engine.execute(query, {"value": value}).to_pylist() == expected
        assert (
            engine.execute_batches(query, {"value": value}).read_all().to_pylist()
            == expected
        )


def test_call_union_branches_import_different_variables_without_leaking(graph):
    engine, _, _ = graph
    assert engine.execute(
        "WITH 2 AS x, 3 AS y CALL { WITH x RETURN x AS n "
        "UNION ALL WITH y RETURN y AS n } RETURN n ORDER BY n"
    ).to_pylist() == [{"n": 2}, {"n": 3}]
    assert engine.execute(
        "WITH 3 AS y CALL { RETURN 2 AS n "
        "UNION ALL WITH y RETURN y AS n } RETURN n ORDER BY n"
    ).to_pylist() == [{"n": 2}, {"n": 3}]
    for query in [
        "WITH 2 AS x CALL { WITH x RETURN x AS n UNION RETURN x AS n } RETURN n",
        "WITH 2 AS x CALL { RETURN x AS n UNION WITH x RETURN x AS n } RETURN n",
        "WITH 2 AS x CALL { WITH x RETURN x AS n UNION WITH x RETURN x + 1 } RETURN n",
    ]:
        with pytest.raises(ValueError):
            engine.execute(query)


@pytest.mark.parametrize("union", ["UNION", "UNION ALL"])
def test_call_union_nodes_and_per_branch_top_k(graph, union):
    engine, _, _ = graph
    query = (
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) "
        "RETURN f AS friend ORDER BY friend.age DESC LIMIT 1 "
        f"{union} WITH p MATCH (p)-[:KNOWS]->(f:Person) "
        "RETURN f AS friend ORDER BY friend.age ASC LIMIT 1 } "
        "RETURN p.id AS id, friend.name AS name ORDER BY id, name"
    )
    expected = [
        {"id": 10, "name": "B"},
        {"id": 10, "name": "C"},
        {"id": 20, "name": "D"},
    ]
    if union == "UNION ALL":
        expected.append({"id": 20, "name": "D"})
    assert engine.execute(query).to_pylist() == expected


def test_call_union_aggregates_and_empty_branches(graph):
    engine, _, _ = graph
    assert engine.execute(
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) "
        "RETURN count(f) AS n UNION ALL WITH p MATCH (p)-[:KNOWS]->(f:Person) "
        "WHERE f.id < 0 RETURN count(f) AS n } RETURN p.id AS id, n ORDER BY id, n"
    ).to_pylist() == [
        {"id": person, "n": n}
        for person, count in [(10, 3), (20, 1), (30, 0), (40, 0)]
        for n in [0, count]
    ]
    assert engine.execute(
        "UNWIND [1, 2] AS x CALL { WITH x UNWIND [] AS empty RETURN x AS n "
        "UNION ALL WITH x RETURN x AS n } RETURN n ORDER BY n"
    ).to_pylist() == [{"n": 1}, {"n": 2}]


def test_call_in_later_union_branch_uses_fresh_parameters(graph):
    engine, _, _ = graph
    query = "RETURN 0 AS n UNION ALL CALL { RETURN $value AS n } RETURN n"
    for value in [1, 2, 1]:
        assert sorted(
            row["n"] for row in engine.execute(query, {"value": value}).to_pylist()
        ) == [0, value]


def test_call_union_schema_validation_uses_each_branch_catalog(graph):
    engine, _, _ = graph
    with pytest.raises(ValueError, match="Type mismatch"):
        engine.execute(
            "MATCH (p:Person) CALL { WITH p RETURN p.age AS n "
            "UNION ALL WITH p RETURN p.name + 1 AS n } RETURN n"
        )


def test_call_union_rejects_mixed_node_label_exports():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("City", "id")
        .build()
    )
    engine = CypherEngine(
        config, {"Person": pa.table({"id": [1]}), "City": pa.table({"id": [1]})}
    )
    with pytest.raises(ValueError, match="same labels"):
        engine.execute(
            "CALL { MATCH (p:Person) RETURN p AS entity "
            "UNION MATCH (c:City) RETURN c AS entity } RETURN entity.id"
        )


def test_call_union_coerces_numeric_and_null_output_schema(graph):
    engine, _, _ = graph
    query = (
        "UNWIND [1, 2] AS x CALL { WITH x RETURN x AS n UNION ALL "
        "RETURN $value AS n } RETURN n ORDER BY n"
    )
    for value, expected in [(2.5, [1.0, 2.0, 2.5, 2.5]), (None, [1, 2, None, None])]:
        table = engine.execute(query, {"value": value})
        expected_type = pa.float64() if value is not None else pa.int64()
        assert table.schema.field("n").type == expected_type
        assert table.column(0).to_pylist() == expected
        reader = engine.execute_batches(query, {"value": value})
        assert reader.schema.field("n").type == expected_type
        assert reader.read_all().column(0).to_pylist() == expected


def test_call_imported_shortest_path_keeps_relationship_records(graph):
    engine, _, _ = graph
    query = (
        "MATCH path = allShortestPaths((a:Person {id: 10})-[:KNOWS*..3]->"
        "(b:Person {id: 40})) CALL { WITH path "
        "RETURN [edge IN relationships(path) | endNode(edge).id] AS targets, "
        "reduce(total = 0, edge IN relationships(path) | "
        "total + endNode(edge).id) AS total } "
        "RETURN targets, total ORDER BY targets"
    )
    assert engine.execute(query).to_pylist() == [
        {"targets": [20, 40], "total": 60},
        {"targets": [20, 40], "total": 60},
    ]


def test_call_union_shares_path_occurrence_identity(graph):
    engine, _, _ = graph
    query = (
        "CALL { MATCH path = (a:Person {id: 10})-[:KNOWS]->(b:Person) "
        "RETURN path UNION MATCH path = (a:Person {id: 10})-[:KNOWS]->(b:Person) "
        "RETURN path } WITH collect(path) AS paths UNWIND paths AS path "
        "RETURN length(path) AS hops, [n IN nodes(path) | n.name] AS names, "
        "[r IN relationships(path) | endNode(r).id] AS targets ORDER BY targets"
    )
    assert engine.execute(query).to_pylist() == [
        {"hops": 1, "names": ["A", "B"], "targets": [20]},
        {"hops": 1, "names": ["A", "B"], "targets": [20]},
        {"hops": 1, "names": ["A", "C"], "targets": [30]},
    ]


def test_call_keeps_path_alias_across_nested_union(graph):
    engine, _, _ = graph
    query = (
        "MATCH path = (a:Person {id: 20})-[:KNOWS]->(b:Person) "
        "CALL { WITH path CALL { WITH path RETURN path AS value "
        "UNION ALL WITH path RETURN path AS value LIMIT 0 } RETURN value AS result } "
        "RETURN length(result) AS hops, [n IN nodes(result) | labels(n)] AS labels"
    )
    assert engine.execute(query).to_pylist() == [
        {"hops": 1, "labels": [["Person"], ["Person"]]}
    ]


def test_call_empty_children_respect_query_deadline(graph):
    engine, _, _ = graph
    with pytest.raises(TimeoutError):
        engine.execute(
            "UNWIND range(1, 1000000) AS x "
            "CALL { WITH x UNWIND [] AS n RETURN n } RETURN n",
            timeout=0.02,
        )
    assert engine.execute("RETURN 1 AS ok").to_pylist() == [{"ok": 1}]


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


def test_lambda_types_shadow_and_restore_outer_bindings(graph):
    engine, _, _ = graph
    for initial, expected in [(0, 3), (0.5, 3.5), (None, None), (0, 3)]:
        rows = engine.execute(
            "WITH 'outer' AS total RETURN total AS outer, "
            "reduce(total = $initial, item IN [1, 2] | total + item) AS value",
            {"initial": initial},
        ).to_pylist()
        assert rows == [{"outer": "outer", "value": expected}]
    assert engine.execute(
        "WITH 'outer' AS item RETURN all(item IN [1, 2] WHERE item > 0) AS valid, "
        "item AS outer"
    ).to_pylist() == [{"valid": True, "outer": "outer"}]
    with pytest.raises(ValueError, match="Type mismatch"):
        engine.execute(
            "RETURN reduce(total = 0, item IN ['text'] | total + item) AS value"
        )


def test_shortest_path_alias_import_demands_relationship_values(graph):
    engine, _, _ = graph
    query = (
        "MATCH path = allShortestPaths((a:Person {id: 10})-[:KNOWS*..3]->"
        "(b:Person {id: 40})) WITH path AS carried "
        "CALL { WITH carried RETURN relationships(carried) AS edges } "
        "RETURN [edge IN edges | endNode(edge).id] AS targets ORDER BY targets"
    )
    assert engine.execute(query).to_pylist() == [
        {"targets": [20, 40]},
        {"targets": [20, 40]},
    ]


def test_shortest_path_alias_survives_call_union(graph):
    engine, _, _ = graph
    query = (
        "MATCH path = allShortestPaths((a:Person {id: 10})-[:KNOWS*..3]->"
        "(b:Person {id: 40})) WITH path AS carried "
        "CALL { WITH carried RETURN carried AS result "
        "UNION WITH carried RETURN carried AS result } "
        "RETURN [edge IN relationships(result) | endNode(edge).id] AS targets "
        "ORDER BY targets"
    )
    assert engine.execute(query).to_pylist() == [
        {"targets": [20, 40]},
        {"targets": [20, 40]},
    ]
