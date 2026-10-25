# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Cypher v9 scalar semantics, including values from Arrow-backed graph rows."""

import math

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True], ids=["join", "csr"])
def engine(request):
    config = GraphConfig.builder().node_table("Person", "id").build()
    people = pa.table(
        {
            "id": [1, 2, 3],
            "text": pa.array(["a猫😀z", None, ""], type=pa.large_string()),
            "number": [-1.5, None, 2.0],
            "items": pa.array([[1, None, 3], None, []], type=pa.list_(pa.int64())),
            "pair": pa.array([[1, 2], None, [3, 4]], type=pa.list_(pa.int64(), 2)),
        }
    )
    return CypherEngine(config, {"Person": people}, use_csr=request.param)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("left('a猫😀z', 3)", "a猫😀"),
        ("right('a猫😀z', 2)", "😀z"),
        ("substring('a猫😀z', 1, 2)", "猫😀"),
        ("substring('a猫😀z', 2)", "😀z"),
        ("substring('x', 99, 2)", ""),
        ("substring('x', 0, 0)", ""),
        ("left('x', 0)", ""),
        ("right('x', 0)", ""),
        ("left('x', 99)", "x"),
        ("right('x', 99)", "x"),
        ("size('a猫😀z')", 4),
        ("size('')", 0),
        ('size("")', 0),
        ("reverse('a猫😀z')", "z😀猫a"),
        ("replace('a猫a', 'a', '😀')", "😀猫😀"),
        ("replace('abc', 'x', 'y')", "abc"),
        ("split('a,,b,', ',')", ["a", "", "b", ""]),
        ("split('猫😀', '')", ["猫", "😀"]),
        ("split('', ',')", [""]),
        ("split('', '')", [""]),
        ("trim('  猫  ')", "猫"),
        ("lTrim('  猫  ')", "猫  "),
        ("rTrim('  猫  ')", "  猫"),
        ("abs(-5)", 5),
        ("abs(-1.5)", 1.5),
        ("round(-1.5)", -1.0),
        ("round(1.5)", 2.0),
        ("round(4503599627370497.0)", 4503599627370497.0),
        ("floor(-1.5)", -2.0),
        ("ceil(-1.5)", -1.0),
        ("sqrt(9)", 3.0),
        ("last([1, null, 3])", 3),
        ("last([1, null])", None),
        ("last([])", None),
        ("tail([1, null, 3])", [None, 3]),
        ("tail([1])", []),
        ("tail([])", []),
        ("reverse([1, null, 3])", [3, None, 1]),
        ("reverse([])", []),
        ("reverse([[1, 2], [3]])", [[3], [1, 2]]),
        ("size([[1, 2], [3]])", 2),
        ("range(0, 4)", [0, 1, 2, 3, 4]),
        ("range(0, 4, 2)", [0, 2, 4]),
        ("range(5, 1, -2)", [5, 3, 1]),
        ("range(1, 5, -2)", []),
        ("range(5, 1, 2)", []),
        ("range(1, 1, -2)", [1]),
        ("range(null, 4)", None),
        ("range(1, null)", None),
        ("range(1, 4, null)", None),
        ("left(null, null)", None),
        ("substring(null, -1)", None),
        ("replace('x', null, '')", None),
        ("split('x', null)", None),
    ]
    + [
        (f"{name}(null)", None)
        for name in (
            "abs",
            "round",
            "floor",
            "ceil",
            "sqrt",
            "trim",
            "lTrim",
            "rTrim",
            "reverse",
            "size",
            "last",
            "tail",
        )
    ],
)
def test_scalar_value(engine, expression, expected):
    result = engine.execute(f"RETURN {expression} AS value")
    assert result.to_pylist() == [{"value": expected}]


@pytest.mark.parametrize(
    "expression",
    [
        "left('x', -1)",
        "right('x', null)",
        "substring('x', -1)",
        "substring('x', 0, null)",
        "range(1, 3, 0)",
        "range(1.5, 3)",
        "abs('1')",
        "abs(true)",
        "abs(-9223372036854775807 - 1)",
        "size(1)",
        "reverse(1)",
        "tail('abc')",
        "last(2)",
        "left('x', 1.5)",
        "split('x', 1)",
        "round(1, 2)",
        "substring('x')",
        "range(1)",
    ],
)
def test_scalar_invalid_arguments(engine, expression):
    with pytest.raises((ValueError, RuntimeError)):
        engine.execute(f"RETURN {expression} AS value")


def test_scalar_arrow_columns_and_nulls(engine):
    result = engine.execute(
        "MATCH (p:Person) RETURN p.id AS id, "
        "substring(p.text, 1, 2) AS sub, size(p.text) AS size, "
        "round(p.number) AS rounded, reverse(p.items) AS reversed, "
        "tail(p.pair) AS tail, last(p.pair) AS last ORDER BY id"
    )
    assert result.to_pylist() == [
        {
            "id": 1,
            "sub": "猫😀",
            "size": 4,
            "rounded": -1.0,
            "reversed": [3, None, 1],
            "tail": [2],
            "last": 2,
        },
        {
            "id": 2,
            "sub": None,
            "size": None,
            "rounded": None,
            "reversed": None,
            "tail": None,
            "last": None,
        },
        {
            "id": 3,
            "sub": "",
            "size": 0,
            "rounded": 2.0,
            "reversed": [],
            "tail": [4],
            "last": 4,
        },
    ]
    assert result.schema.field("rounded").type == pa.float64()
    assert result.schema.field("size").type == pa.int64()


def test_scalar_whitespace_and_nan(engine):
    result = engine.execute(
        "RETURN trim($text) AS text, sqrt(-1) AS root, round(-0.5) AS zero",
        {"text": "\t\n 猫\u2003"},
    ).to_pylist()[0]
    assert result["text"] == "猫"
    assert math.isnan(result["root"])
    assert math.copysign(1, result["zero"]) == 1


def test_unimplemented_scalar_function_errors(engine):
    with pytest.raises(ValueError, match="not_a_function.*is not implemented"):
        engine.execute("RETURN not_a_function('x') AS value")


def test_scalar_varying_arguments(engine):
    result = engine.execute(
        "MATCH (p:Person) RETURN p.id AS id, left(p.text, p.id) AS prefix, "
        "range(0, p.id, 2) AS numbers ORDER BY id"
    )
    assert result.to_pylist() == [
        {"id": 1, "prefix": "a", "numbers": [0]},
        {"id": 2, "prefix": None, "numbers": [0, 2]},
        {"id": 3, "prefix": "", "numbers": [0, 2]},
    ]
