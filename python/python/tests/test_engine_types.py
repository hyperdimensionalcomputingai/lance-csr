# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Schema-aware errors retain the expression, operand types and lexical scope."""

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True])
def engine(request):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    return CypherEngine(
        config,
        {
            "Person": pa.table(
                {
                    "id": [1, 2],
                    "name": ["Ada", "Bob"],
                    "age": pa.array([30, 40], pa.int32()),
                    "created": [datetime(2020, 1, 1, tzinfo=timezone.utc)] * 2,
                }
            ),
            "KNOWS": pa.table({"src": [1], "dst": [2], "label": ["friend"]}),
        },
        use_csr=request.param,
    )


@pytest.mark.parametrize(
    ("query", "expression", "types"),
    [
        (
            "MATCH (p:Person) WHERE p.name = 1 RETURN p.id",
            "p.name = 1",
            ("Utf8", "Int64"),
        ),
        ("MATCH (p:Person) RETURN p + 1", "p + 1", ("Node", "Int64")),
        (
            "MATCH (p:Person) WITH p.name AS value RETURN value * 2",
            "value * 2",
            ("Utf8", "Int64"),
        ),
        (
            "MATCH (p:Person) WITH collect(p.name) AS names UNWIND names AS name "
            "RETURN name / 2",
            "name / 2",
            ("Utf8", "Int64"),
        ),
        (
            "MATCH (p:Person) RETURN [p IN [p.name] | p + 1]",
            "p + 1",
            ("Utf8", "Int64"),
        ),
        (
            "MATCH (p:Person)-[r:KNOWS]->(q:Person) WHERE r.label = 1 RETURN q.id",
            "r.label = 1",
            ("Utf8", "Int64"),
        ),
    ],
)
def test_type_errors_name_expression_and_operands(engine, query, expression, types):
    with pytest.raises(ValueError, match="Type mismatch") as error:
        engine.execute(query)
    assert expression in str(error.value)
    assert all(name in str(error.value) for name in types)


def test_numeric_temporal_and_null_coercions(engine):
    result = engine.execute(
        "MATCH (p:Person) WHERE p.created >= $since "
        "RETURN p.age + 0.5 AS adjusted, p.age + null AS missing ORDER BY adjusted",
        {"since": datetime(2020, 1, 1, tzinfo=timezone.utc)},
    )
    assert result.to_pylist() == [
        {"adjusted": 30.5, "missing": None},
        {"adjusted": 40.5, "missing": None},
    ]


def test_comprehension_shadow_does_not_change_outer_type(engine):
    result = engine.execute(
        "MATCH (p:Person) RETURN [p IN [p.age] | p + 1] AS ages, p.name AS name "
        "ORDER BY name"
    )
    assert result.to_pylist() == [
        {"ages": [31], "name": "Ada"},
        {"ages": [41], "name": "Bob"},
    ]
