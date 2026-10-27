# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""A final semicolon terminates one query without enabling multiple statements."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig


@pytest.mark.parametrize("mode", ["legacy", "join", "csr", "stream"])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("RETURN 1 AS n;", [{"n": 1}]),
        ("RETURN null AS n;", [{"n": None}]),
        ("RETURN 1 AS n UNION ALL RETURN 1 AS n;", [{"n": 1}, {"n": 1}]),
        ("MATCH (p:Person) RETURN p.id AS id ORDER BY id LIMIT 1;", [{"id": 1}]),
        (
            "MATCH (p:Person) RETURN p.id AS id ORDER BY id SKIP 1 LIMIT 1 ; \n\t",
            [{"id": 2}],
        ),
        (
            "MATCH (p:Person) WITH p.id AS id RETURN id ORDER BY id;",
            [{"id": 1}, {"id": 2}],
        ),
        ("MATCH (p:Person) WHERE p.id = 1 RETURN 'a;b' AS text;", [{"text": "a;b"}]),
        ("MATCH (p:Person) RETURN count(*) AS n;", [{"n": 2}]),
    ],
)
def test_optional_query_semicolon(mode, query, expected):
    config = GraphConfig.builder().node_table("Person", "id").build()
    tables = {"Person": pa.table({"id": [1, 2]})}
    if mode == "legacy":
        result = CypherQuery(query).with_config(config).execute(tables)
    else:
        engine = CypherEngine(config, tables, use_csr=mode in ("csr", "stream"))
        result = (
            engine.execute_batches(query).read_all()
            if mode == "stream"
            else engine.execute(query)
        )
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "query",
    [
        ";",
        "MATCH (p:Person) RETURN p.id;;",
        "MATCH (p:Person) RETURN p.id; MATCH (p:Person) RETURN p.id",
        "MATCH (p:Person) RETURN p.id; garbage",
    ],
)
def test_semicolon_rejects_additional_statements(query):
    with pytest.raises(ValueError):
        CypherQuery(query)
