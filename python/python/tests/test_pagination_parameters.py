# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Pagination parameters resolve before planning and never wrap or truncate."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig

CONFIG = GraphConfig.builder().node_table("Person", "id").build()
TABLES = {"Person": pa.table({"id": [1, 2, 3, 4]})}


@pytest.fixture(params=[False, True])
def engine(request):
    return CypherEngine(CONFIG, TABLES, use_csr=request.param)


@pytest.mark.parametrize(
    ("query", "params", "expected"),
    [
        (
            "MATCH (p:Person) RETURN p.id AS id ORDER BY id SKIP $skip LIMIT $take",
            {"skip": 1, "take": 2},
            [{"id": 2}, {"id": 3}],
        ),
        (
            "MATCH (p:Person) RETURN p.id AS id ORDER BY id LIMIT $take SKIP $skip",
            {"skip": 1, "take": 2},
            [{"id": 2}, {"id": 3}],
        ),
        (
            "MATCH (p:Person) WITH p ORDER BY p.id LIMIT $take RETURN p.id AS id",
            {"take": 2},
            [{"id": 1}, {"id": 2}],
        ),
        (
            "MATCH (p:Person) WITH p ORDER BY p.id LIMIT $first "
            "WITH p LIMIT $second RETURN p.id AS id",
            {"first": 3, "second": 1},
            [{"id": 1}],
        ),
        ("RETURN 1 AS n LIMIT $take", {"take": 0}, []),
        ("RETURN 1 AS n SKIP $skip", {"skip": 1}, []),
    ],
)
def test_parameterized_pagination(engine, query, params, expected):
    assert engine.execute(query, params).to_pylist() == expected
    assert engine.execute_batches(query, params).read_all().to_pylist() == expected
    one_shot = CypherQuery(query).with_config(CONFIG)
    for name, value in params.items():
        one_shot = one_shot.with_parameter(name, value)
    assert one_shot.execute(TABLES).to_pylist() == expected
    assert engine.explain(query, params)


@pytest.mark.parametrize("clause", ["LIMIT", "SKIP"])
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"n": None},
        {"n": -1},
        {"n": 1.5},
        {"n": True},
        {"n": "2"},
        {"n": []},
        {"n": 2**64},
    ],
)
def test_invalid_pagination(engine, clause, params):
    with pytest.raises(ValueError):
        engine.execute(f"RETURN 1 AS n {clause} $n", params)
