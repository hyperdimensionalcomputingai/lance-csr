# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""The graph config builder: node and relationship tables."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

TABLES = {
    "Person": pa.table({"id": [1, 2], "name": ["a", "b"]}),
    "Company": pa.table({"id": [10], "name": ["c"]}),
    "WORKS_AT": pa.table({"src": [1, 2], "dst": [10, 10]}),
}
QUERY = "MATCH (p:Person)-[:WORKS_AT]->(c) RETURN p.name, c.name ORDER BY p.name"


def answer(config):
    return CypherEngine(config, TABLES).execute(QUERY).to_pylist()


def test_tables_configure_the_graph():
    config = (
        GraphConfig.builder()
        .node_table("Person", primary_key="id")
        .node_table("Company", primary_key="id")
        .rel_table(
            "WORKS_AT", from_table="Person", to_table="Company", src="src", dst="dst"
        )
        .build()
    )
    # `c` needs no label: WORKS_AT's to_table makes it a Company.
    assert answer(config) == [
        {"p.name": "a", "c.name": "c"},
        {"p.name": "b", "c.name": "c"},
    ]


def test_rel_table_endpoints_must_be_node_tables():
    builder = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("WORKS_AT", "Person", "Company", "src", "dst")
    )
    with pytest.raises(Exception, match="Company"):
        builder.build()
