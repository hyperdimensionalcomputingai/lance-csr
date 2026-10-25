# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Cypher math types, nulls, and rounding through both execution backends."""

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
            "integer": pa.array([-128, None, 2], type=pa.int8()),
            "number": pa.array([-1.5, None, 2.0], type=pa.float32()),
        }
    )
    return CypherEngine(config, {"Person": people}, use_csr=request.param)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("abs(-5)", 5),
        ("abs(-1.5)", 1.5),
        ("round(-1.5)", -1.0),
        ("round(1.5)", 2.0),
        ("round(4503599627370495.5)", 4503599627370496.0),
        ("round(4503599627370497.0)", 4503599627370497.0),
        ("floor(-1.5)", -2.0),
        ("ceil(-1.5)", -1.0),
        ("sqrt(9)", 3.0),
    ]
    + [(f"{name}(null)", None) for name in ("abs", "round", "floor", "ceil", "sqrt")],
)
def test_math_value(engine, expression, expected):
    result = engine.execute(f"RETURN {expression} AS value")
    assert result.to_pylist() == [{"value": expected}]
    assert result.schema.field("value").type == (
        pa.int64() if isinstance(expected, int) else pa.float64()
    )


@pytest.mark.parametrize("name", ["abs", "round", "floor", "ceil", "sqrt"])
@pytest.mark.parametrize("argument", ["'1'", "true", "[1]", "{x: 1}", "", "1, 2"])
def test_math_invalid_arguments(engine, name, argument):
    with pytest.raises((ValueError, RuntimeError)):
        engine.execute(f"RETURN {name}({argument}) AS value")


def test_math_integer_overflow(engine):
    with pytest.raises((ValueError, RuntimeError), match="overflow"):
        engine.execute("RETURN abs(-9223372036854775807 - 1) AS value")


def test_math_arrow_columns_and_nulls(engine):
    result = engine.execute(
        "MATCH (p:Person) RETURN p.id AS id, abs(p.integer) AS magnitude, "
        "abs(p.number) AS floating, round(p.number) AS rounded, "
        "floor(p.number) AS floored, ceil(p.number) AS ceiling, "
        "sqrt(abs(p.integer)) AS root ORDER BY id"
    )
    assert result.to_pylist() == [
        {
            "id": 1,
            "magnitude": 128,
            "floating": 1.5,
            "rounded": -1.0,
            "floored": -2.0,
            "ceiling": -1.0,
            "root": math.sqrt(128),
        },
        {
            "id": 2,
            "magnitude": None,
            "floating": None,
            "rounded": None,
            "floored": None,
            "ceiling": None,
            "root": None,
        },
        {
            "id": 3,
            "magnitude": 2,
            "floating": 2.0,
            "rounded": 2.0,
            "floored": 2.0,
            "ceiling": 2.0,
            "root": math.sqrt(2),
        },
    ]
    assert result.schema.field("magnitude").type == pa.int64()
    for column in ("floating", "rounded", "floored", "ceiling", "root"):
        assert result.schema.field(column).type == pa.float64()


def test_math_nan_and_signed_zero(engine):
    result = engine.execute(
        "RETURN sqrt(-1) AS root, round(-0.5) AS zero, round($infinity) AS infinity",
        {"infinity": math.inf},
    ).to_pylist()[0]
    assert math.isnan(result["root"])
    assert math.copysign(1, result["zero"]) == 1
    assert result["infinity"] == math.inf


def test_math_out_of_range_unsigned_integer():
    config = GraphConfig.builder().node_table("Person", "id").build()
    engine = CypherEngine(
        config,
        {"Person": pa.table({"id": [1], "large": pa.array([2**63], type=pa.uint64())})},
    )
    with pytest.raises((ValueError, RuntimeError), match="cast|Cast"):
        engine.execute("MATCH (p:Person) RETURN abs(p.large) AS value")
