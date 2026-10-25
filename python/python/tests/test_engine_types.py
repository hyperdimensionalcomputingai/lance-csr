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


@pytest.mark.parametrize(
    "query",
    [
        "RETURN left('12', 1) + 1",
        "RETURN abs(1) = '1'",
        "WITH sqrt(4) AS value RETURN value = '2'",
        "WITH split('1,2', ',') AS values UNWIND values AS value RETURN value + 1",
        "RETURN last(reverse(['1'])) + 1",
        "WITH tail(['1', '2']) AS values UNWIND values AS value RETURN value + 1",
        "UNWIND keys({one: 1}) AS key RETURN key = 1",
        "MATCH (p:Person) UNWIND labels(p) AS label RETURN label = 1",
    ],
)
def test_scalar_return_types_reject_string_numeric_coercion(engine, query):
    with pytest.raises(ValueError, match="Type mismatch"):
        engine.execute(query)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("RETURN abs(-1) + sqrt(4) AS value", [3.0]),
        ("RETURN round(null) + 1 AS value", [None]),
        ("RETURN abs(null) = '1' AS value", [None]),
        ("RETURN left('猫😀', 1) = '猫' AS value", [True]),
        ("RETURN reverse('猫😀') = '😀猫' AS value", [True]),
        (
            "WITH split('猫,😀', ',') AS values UNWIND values AS value "
            "RETURN value AS value ORDER BY value",
            ["猫", "😀"],
        ),
        ("RETURN last(reverse([1, 2])) + 1 AS value", [2]),
        (
            "WITH tail([1, 2, 3]) AS values UNWIND values AS value "
            "RETURN value + 0.5 AS value ORDER BY value",
            [2.5, 3.5],
        ),
        ("RETURN left(null, 1) AS value", [None]),
    ],
)
def test_scalar_return_types_preserve_values_and_nulls(engine, query, expected):
    assert engine.execute(query).column("value").to_pylist() == expected
