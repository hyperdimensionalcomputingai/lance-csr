# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Actual edge records survive path values, including duplicate endpoint pairs."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True], ids=["scan", "csr"])
def engine(request):
    people = pa.table({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]})
    edges = pa.table(
        {"src": [1, 1, 2, 3, 3], "dst": [2, 2, 3, 3, 99], "weight": [5, 7, 11, 13, 17]}
    )
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    return CypherEngine(
        config, {"Person": people, "KNOWS": edges}, use_csr=request.param
    )


def test_named_path_records(engine):
    rows = engine.execute(
        "MATCH p = (a:Person {id: 1})-[:KNOWS]->(b:Person)"
        "-[:KNOWS]->(c:Person {id: 3}) "
        "RETURN length(p) AS length, nodes(p) AS ids, "
        "[n IN nodes(p) | n.name] AS names, "
        "[r IN relationships(p) | r.weight] AS weights"
    ).to_pylist()
    assert sorted(rows, key=lambda r: r["weights"]) == [
        {"length": 2, "ids": [1, 2, 3], "names": ["a", "b", "c"], "weights": [5, 11]},
        {"length": 2, "ids": [1, 2, 3], "names": ["a", "b", "c"], "weights": [7, 11]},
    ]


@pytest.mark.parametrize("search", ["shortestPath", "allShortestPaths"])
def test_shortest_path_survives_collect_unwind(engine, search):
    rows = engine.execute(
        f"MATCH p = {search}((a:Person {{id: 1}})-[:KNOWS*0..]->(b:Person {{id: 3}})) "
        "WITH collect(p) AS paths UNWIND paths AS p "
        "RETURN nodes(p) AS ids, [r IN relationships(p) | r.weight] AS weights, "
        "[n IN nodes(p) | n.name] AS names"
    ).to_pylist()
    expected = [[5, 11], [7, 11]]
    assert len(rows) == (2 if search == "allShortestPaths" else 1)
    assert all(r["ids"] == [1, 2, 3] and r["names"] == ["a", "b", "c"] for r in rows)
    assert all(r["weights"] in expected for r in rows)
    if search == "allShortestPaths":
        assert sorted(r["weights"] for r in rows) == expected


def test_reverse_path_keeps_stored_relationship_endpoints(engine):
    rows = engine.execute(
        "MATCH p = (b:Person {id: 2})<-[r:KNOWS]-(a:Person {id: 1}) "
        "UNWIND relationships(p) AS edge RETURN nodes(p) AS ids, "
        "edge.weight AS weight, startNode(edge).id AS source, "
        "endNode(edge).id AS target"
    ).to_pylist()
    assert sorted(rows, key=lambda r: r["weight"]) == [
        {"ids": [2, 1], "weight": 5, "source": 1, "target": 2},
        {"ids": [2, 1], "weight": 7, "source": 1, "target": 2},
    ]


@pytest.mark.parametrize("shortest", [False, True])
def test_zero_path_records(engine, shortest):
    pattern = "(a:Person {id: 3})-[:KNOWS*0..0]->(b:Person {id: 3})"
    if shortest:
        pattern = f"shortestPath({pattern})"
    rows = engine.execute(
        f"MATCH p = {pattern} RETURN p, length(p) AS length, "
        "nodes(p) AS ids, relationships(p) AS edges"
    ).to_pylist()
    assert len(rows) == 1
    assert rows[0]["length"] == 0
    assert rows[0]["ids"] == [3]
    assert rows[0]["edges"] == []
    assert rows[0]["p"]["node_records"][0]["__lance_graph_properties"]["name"] == "c"


def test_named_trails_do_not_reuse_a_relationship(engine):
    rows = engine.execute(
        "MATCH p = (a:Person {id: 3})-[:KNOWS*1..3]->(b:Person) "
        "RETURN nodes(p) AS ids, [r IN relationships(p) | r.weight] AS weights"
    ).to_pylist()
    assert rows == [{"ids": [3, 3], "weights": [13]}]


def test_optional_path_null_values(engine):
    rows = engine.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 4}) "
        "OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*0..]->(b)) "
        "RETURN p, relationships(p) AS edges, nodes(p) AS ids"
    ).to_pylist()
    assert rows == [{"p": None, "edges": None, "ids": None}]


def test_path_entities_have_properties_and_labels(engine):
    rows = engine.execute(
        "MATCH p = (a:Person {id: 1})-[:KNOWS]->(b:Person) "
        "UNWIND relationships(p) AS r "
        "RETURN keys(r) AS properties, labels(startNode(r)) AS labels"
    ).to_pylist()
    assert rows == [{"properties": ["dst", "src", "weight"], "labels": ["Person"]}] * 2


def test_fast_shortest_plan_retained(engine):
    query = (
        "MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*0..]->(b:Person {id: 3})) "
        "RETURN [n IN nodes(p) | n.id] AS ids"
    )
    assert "PathMatch" not in engine.explain(query)
    assert engine.execute(query).to_pylist() == [{"ids": [1, 2, 3]}]


def test_mixed_label_path_identity():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("Place", "id")
        .rel_table("LIVES", "Person", "Place", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1], "name": ["person"]}),
            "Place": pa.table({"id": [1], "name": ["place"]}),
            "LIVES": pa.table({"src": [1], "dst": [1]}),
        },
    )
    rows = engine.execute(
        "MATCH p = (a:Person)-[:LIVES]->(b:Place) RETURN nodes(p) AS ids, "
        "[n IN nodes(p) | n.name] AS names, [n IN nodes(p) | labels(n)] AS labels"
    ).to_pylist()
    assert rows == [
        {"ids": [1, 1], "names": ["person", "place"], "labels": [["Person"], ["Place"]]}
    ]


def test_named_variable_alternation_tracks_labels():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("Place", "id")
        .rel_table("LIVES", "Person", "Place", "src", "dst")
        .rel_table("INSIDE", "Place", "Place", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1], "name": ["person"]}),
            "Place": pa.table({"id": [1, 2], "name": ["place", "country"]}),
            "LIVES": pa.table({"src": [1], "dst": [1]}),
            "INSIDE": pa.table({"src": [1], "dst": [2]}),
        },
        use_csr=True,
    )
    query = (
        "MATCH p = (a:Person)-[:LIVES|INSIDE*0..20]->(b:Place) "
        "RETURN nodes(p) AS ids, [n IN nodes(p) | n.name] AS names ORDER BY ids"
    )
    assert engine.execute(query).to_pylist() == [
        {"ids": [1, 1], "names": ["person", "place"]},
        {"ids": [1, 1, 2], "names": ["person", "place", "country"]},
    ]
    assert engine.explain(query).count("PathMatch:") == 1


def test_identical_parallel_occurrences_are_distinct():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("R", "Person", "Person", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1, 2]}),
            "R": pa.table({"src": [1, 1], "dst": [2, 2], "weight": [5, 5]}),
        },
    )
    rows = engine.execute(
        "MATCH p = allShortestPaths((a:Person {id: 1})"
        "-[:R*]->(b:Person {id: 2})) RETURN p"
    ).to_pylist()
    relationships = [r["p"]["relationships"][0] for r in rows]
    assert len(relationships) == 2
    assert len({r["__lance_graph_identity"]["id"] for r in relationships}) == 2
    assert [r["__lance_graph_properties"]["weight"] for r in relationships] == [5, 5]


@pytest.mark.parametrize("bad_type", [pa.string(), pa.uint64()])
def test_rich_path_rejects_unsupported_ids(bad_type):
    values = ["1", "2"] if pa.types.is_string(bad_type) else [1, 2]
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("R", "Person", "Person", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": pa.table({"id": pa.array(values, type=bad_type)}),
            "R": pa.table(
                {
                    "src": pa.array(values[:1], type=bad_type),
                    "dst": pa.array(values[1:], type=bad_type),
                }
            ),
        },
    )
    with pytest.raises(NotImplementedError, match="unsupported type"):
        engine.execute("MATCH p = (a:Person)-[:R]->(b:Person) RETURN p")


def test_mixed_path_rejects_conflicting_property_types():
    config = (
        GraphConfig.builder()
        .node_table("A", "id")
        .node_table("B", "id")
        .rel_table("R", "A", "B", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "A": pa.table({"id": [1], "name": ["name"]}),
            "B": pa.table({"id": [1], "name": [99]}),
            "R": pa.table({"src": [1], "dst": [1]}),
        },
    )
    with pytest.raises(ValueError, match="conflicting Arrow types"):
        engine.execute("MATCH p = (a:A)-[:R]->(b:B) RETURN p")


def test_path_inventory_keeps_lance_snapshot(tmp_path):
    import lance

    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("R", "Person", "Person", "src", "dst")
        .build()
    )
    people_path, edges_path = (
        str(tmp_path / "people.lance"),
        str(tmp_path / "edges.lance"),
    )
    lance.write_dataset(
        pa.table({"id": [1, 2], "name": ["old", "target"]}), people_path
    )
    lance.write_dataset(pa.table({"src": [1], "dst": [2], "weight": [5]}), edges_path)
    engine = CypherEngine(
        config,
        {"Person": lance.dataset(people_path), "R": lance.dataset(edges_path)},
        use_csr=True,
    )
    query = (
        "MATCH p = (a:Person {id: 1})-[:R]->(b:Person) "
        "RETURN [n IN nodes(p) | n.name] AS names, "
        "[r IN relationships(p) | r.weight] AS weights"
    )
    reader = engine.execute_batches(query)
    lance.write_dataset(
        pa.table({"src": [1], "dst": [2], "weight": [7]}), edges_path, mode="overwrite"
    )
    engine.refresh()
    assert reader.read_all().to_pylist() == [
        {"names": ["old", "target"], "weights": [5]}
    ]
    assert engine.execute(query).to_pylist() == [
        {"names": ["old", "target"], "weights": [7]}
    ]


def test_bound_relationship_endpoints_survive_collect(engine):
    rows = engine.execute(
        "MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person {id: 2}) "
        "WITH collect(r) AS rs UNWIND rs AS edge "
        "RETURN edge.weight AS weight, keys(edge) AS properties, "
        "startNode(edge).name AS source, endNode(edge).name AS target, "
        "labels(endNode(edge)) AS labels ORDER BY weight"
    ).to_pylist()
    assert rows == [
        {
            "weight": weight,
            "properties": ["dst", "src", "weight"],
            "source": "a",
            "target": "b",
            "labels": ["Person"],
        }
        for weight in [5, 7]
    ]


def test_entity_metadata_does_not_shadow_properties():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("R", "Person", "Person", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": pa.table(
                {"id": [1, 2], "nullable": pa.array([None, None], pa.string())}
            ),
            "R": pa.table({"src": [1], "dst": [2], "__lance_graph_identity": [42]}),
        },
    )
    rows = engine.execute(
        "MATCH p = (a:Person)-[:R]->(b:Person) UNWIND relationships(p) AS r "
        "RETURN r.__lance_graph_identity AS user_value, keys(r) AS properties, "
        "keys(startNode(r)) AS node_properties, keys({nullable: null}) AS map_keys"
    ).to_pylist()
    assert rows == [
        {
            "user_value": 42,
            "properties": ["__lance_graph_identity", "dst", "src"],
            "node_properties": ["id"],
            "map_keys": ["nullable"],
        }
    ]


def test_multiple_path_operators_share_occurrence_identity(engine):
    rows = engine.execute(
        "MATCH p = (a:Person {id: 1})-[:KNOWS]->(b:Person {id: 2}) "
        "MATCH q = (a)-[:KNOWS]->(b) "
        "RETURN head(relationships(p)) AS first, head(relationships(q)) AS second"
    ).to_pylist()
    assert len(rows) == 4
    ids = {}
    for row in rows:
        for edge in row.values():
            weight = edge["__lance_graph_properties"]["weight"]
            identity = edge["__lance_graph_identity"]
            assert ids.setdefault(weight, identity) == identity
    assert ids[5] != ids[7]


@pytest.mark.parametrize("use_csr", [False, True])
def test_unmodified_ic14_with_real_relationship_values(use_csr):
    from pathlib import Path

    # Exact query text from the benchmark's Neo4j IC14, including collect(path),
    # relationship endpoints, nested correlated pattern comprehensions, and reduce.
    query = (Path(__file__).parent / "queries" / "ic14.cypher").read_text()
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("Comment", "id")
        .node_table("Post", "id")
        .rel_table("knows", "Person", "Person", "src", "dst")
        .rel_table("commentHasCreator", "Comment", "Person", "src", "dst")
        .rel_table("postHasCreator", "Post", "Person", "src", "dst")
        .rel_table("replyOfPost", "Comment", "Post", "src", "dst")
        .rel_table("replyOfComment", "Comment", "Comment", "src", "dst")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1, 2, 3, 4]}),
            "Comment": pa.table({"id": [10, 11, 12, 13]}),
            "Post": pa.table({"id": [20, 21]}),
            "knows": pa.table({"src": [1, 1, 2, 1, 4], "dst": [2, 2, 3, 4, 3]}),
            "commentHasCreator": pa.table(
                {"src": [10, 11, 12, 13], "dst": [1, 2, 2, 4]}
            ),
            "postHasCreator": pa.table({"src": [20, 21], "dst": [2, 3]}),
            "replyOfPost": pa.table({"src": [10, 11, 13], "dst": [20, 21, 21]}),
            "replyOfComment": pa.table({"src": [12], "dst": [10]}),
        },
        use_csr=use_csr,
    )
    rows = engine.execute(query, {"person1Id": 1, "person2Id": 3}).to_pylist()
    assert [(r["personidsinpath"], r["pathweight"]) for r in rows] == [
        ([1, 2, 3], 2.5),
        ([1, 2, 3], 2.5),
        ([1, 4, 3], 1.0),
    ]


@pytest.mark.parametrize("storage", ["arrow", "lance"])
@pytest.mark.parametrize("shortest", [False, True])
def test_rich_inventory_reads_every_partition(tmp_path, storage, shortest):
    import lance

    n = 20_000
    people = pa.table({"id": pa.array(range(n + 1), pa.int64())})
    edges = pa.table(
        {
            "src": pa.array(range(n), pa.int64()),
            "dst": pa.array(range(1, n + 1), pa.int64()),
        }
    )
    datasets = {"Person": people, "R": edges}
    if storage == "lance":
        datasets = {
            name: lance.write_dataset(table, tmp_path / f"{name}.lance")
            for name, table in datasets.items()
        }
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("R", "Person", "Person", "src", "dst")
        .build()
    )
    engine = CypherEngine(config, datasets, use_csr=True)
    pattern = f"(a:Person {{id: {n - 10}}})-[:R*1..5]->(b:Person {{id: {n - 5}}})"
    if shortest:
        pattern = f"allShortestPaths({pattern})"
    rows = engine.execute(f"MATCH p = {pattern} RETURN p").to_pylist()
    assert len(rows) == 1
    assert rows[0]["p"]["nodes"] == list(range(n - 10, n - 4))
    assert len(rows[0]["p"]["relationships"]) == 5


@pytest.mark.parametrize(
    ("suffix", "rich"),
    [
        ("WITH p RETURN nodes(p) AS ids", False),
        ("WITH p AS q WITH q AS r RETURN nodes(r) AS ids", False),
        ("WITH p AS q RETURN [n IN nodes(q) | n.id] AS ids", False),
        (
            "WITH p AS q WITH collect(q) AS ps UNWIND ps AS r RETURN nodes(r) AS ids",
            True,
        ),
    ],
)
def test_path_pass_through_only_materializes_consumed_values(engine, suffix, rich):
    query = (
        "MATCH p = allShortestPaths((a:Person {id: 1})-[:KNOWS*]->(b:Person {id: 3})) "
        + suffix
    )
    assert engine.execute(query).to_pylist() == [{"ids": [1, 2, 3]}] * 2
    assert ("PathMatchExec" in engine.explain(query)) is rich


@pytest.mark.parametrize(
    "suffix", ["RETURN relationships(q) AS edges", "RETURN q AS path"]
)
def test_path_alias_retains_actual_relationship_values(engine, suffix):
    query = (
        "MATCH p = allShortestPaths((a:Person {id: 1})-[:KNOWS*]->(b:Person {id: 3})) "
        "WITH p AS q " + suffix
    )
    rows = engine.execute(query).to_pylist()
    edges = [row.get("edges", row.get("path", {}).get("relationships")) for row in rows]
    assert sorted(
        [r["__lance_graph_properties"]["weight"] for r in path] for path in edges
    ) == [
        [5, 11],
        [7, 11],
    ]
    assert "PathMatchExec" in engine.explain(query)


@pytest.mark.parametrize("storage", ["arrow", "lance"])
def test_rich_path_rejects_ambiguous_node_ids(tmp_path, storage):
    import lance

    tables = {
        "Person": pa.table({"id": [1, 1, 2], "name": ["one", "duplicate", "two"]}),
        "R": pa.table({"src": [1], "dst": [2]}),
    }
    if storage == "lance":
        tables = {
            name: lance.write_dataset(table, tmp_path / f"{name}.lance")
            for name, table in tables.items()
        }
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("R", "Person", "Person", "src", "dst")
        .build()
    )
    engine = CypherEngine(config, tables, use_csr=True)
    with pytest.raises(
        Exception, match="unique node IDs within label 'Person'; duplicate ID 1"
    ):
        engine.execute("MATCH p = (a:Person)-[:R]->(b:Person) RETURN p")
