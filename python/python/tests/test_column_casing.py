# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Catalog scans preserve physical spelling while Cypher names ignore case."""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig


@pytest.fixture(params=["arrow", "lance"])
def graph(request, tmp_path):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "sourceid", "targetid")
        .build()
    )
    tables = {
        "Person": pa.table(
            {
                "ID": [1, 2, 3],
                "firstName": ["Bill", "Alice", "Carol"],
                "isMarried": [True, False, None],
            }
        ),
        "KNOWS": pa.table(
            {"sourceID": [1, 2], "targetID": [2, 3], "createdAt": [2001, 2002]}
        ),
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


@pytest.mark.parametrize("mode", ["legacy", "join", "csr", "stream-join", "stream-csr"])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "MATCH (p:Person) RETURN p.isMarried AS married ORDER BY p.id",
            [{"married": True}, {"married": False}, {"married": None}],
        ),
        (
            "MATCH (p:PERSON) WHERE p.ISMARRIED = true RETURN p.FIRSTNAME AS name",
            [{"name": "Bill"}],
        ),
        ("MATCH (p:Person {firstName: 'Alice'}) RETURN p.id AS id", [{"id": 2}]),
        (
            "MATCH (p:Person) RETURN p.id AS id ORDER BY id",
            [{"id": 1}, {"id": 2}, {"id": 3}],
        ),
        (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN r.createdAt AS year ORDER BY year",
            [{"year": 2001}, {"year": 2002}],
        ),
        (
            "MATCH (a:Person)-[r:KNOWS {CREATEDAT: 2002}]->(b:Person) "
            "RETURN b.firstName AS name",
            [{"name": "Carol"}],
        ),
        (
            "MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id = 2 "
            "RETURN b.id AS id ORDER BY id",
            [{"id": 1}, {"id": 3}],
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 1 "
            "RETURN b.firstName AS name",
            [{"name": "Alice"}],
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person {FIRSTNAME: 'Alice'}) "
            "RETURN a.id AS id",
            [{"id": 1}],
        ),
        (
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) WHERE a.id = 1 "
            "RETURN b.firstName AS name ORDER BY name",
            [{"name": "Alice"}, {"name": "Carol"}],
        ),
        (
            "MATCH (a:Person) WHERE a.id = 3 "
            "OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN b.firstName AS name",
            [{"name": None}],
        ),
        (
            "MATCH (p:Person) WITH p.firstName AS fullName "
            "RETURN fullName ORDER BY fullName",
            [{"fullname": "Alice"}, {"fullname": "Bill"}, {"fullname": "Carol"}],
        ),
        (
            "MATCH (p:Person) RETURN count(p.id) AS numPeople ORDER BY numPeople",
            [{"numpeople": 3}],
        ),
    ],
)
def test_physical_column_casing(graph, mode, query, expected):
    config, tables, sources = graph
    if mode == "legacy":
        result = CypherQuery(query).with_config(config).execute(tables)
    else:
        engine = CypherEngine(config, sources, use_csr="csr" in mode)
        result = (
            engine.execute_batches(query).read_all()
            if mode.startswith("stream")
            else engine.execute(query)
        )
    assert result.to_pylist() == expected
    assert result.column_names == list(expected[0])
    assert tables["Person"].column_names == ["ID", "firstName", "isMarried"]
