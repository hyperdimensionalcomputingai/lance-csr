# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Ordered reduce, lexical scope, and Cypher's three-valued list predicates."""

import itertools

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True], ids=["join", "csr"])
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
                    "id": [1, 2, 3],
                    "items": pa.array([[1, None, 3], None, []], pa.list_(pa.int64())),
                    "large": pa.array([[1, 2], None, []], pa.large_list(pa.int64())),
                    "pair": pa.array([[1, 2], None, [3, 4]], pa.list_(pa.int64(), 2)),
                }
            ),
            "KNOWS": pa.table({"src": [1, 1, 2], "dst": [2, 3, 3]}),
        },
        use_csr=request.param,
    )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("reduce(s=0, x IN [1,2,3] | s*10+x)", 123),
        ("reduce(s=9, x IN [] | s+x)", 9),
        ("reduce(s=9, x IN null | s+x)", None),
        ("reduce(s=0, x IN [1,null,3] | s+x)", None),
        ("reduce(s=0, x IN [1,null,3] | s+coalesce(x,0))", 4),
        ("reduce(s=null, x IN [1,2,3] | coalesce(s,0)+x)", 6),
        ("reduce(s=0, x IN [1.5,2.5] | s+x)", 4.0),
        ("reduce(s=1, x IN [2,3] | s/x)", 0),
        ("reduce(s='猫😀猫', x IN ['猫','😀'] | replace(s,x,'a'))", "aaa"),
        ("reduce(s={n:0}, x IN [1,2] | {n:s.n+x}).n", 3),
        ("reduce(s=0, x IN [{n:1},{n:2}] | s+x.n)", 3),
        ("reduce(s=0, xs IN [[1,2],[3]] | s+reduce(t=0, x IN xs | t+x))", 6),
        ("[xs IN [[1,2],[3]] | reduce(s=0,x IN xs | s+x)]", [3, 3]),
        ("reduce(s=0, x IN [1,2] | s+reduce(s=10, y IN [x] | s+y))", 23),
        ("reduce(s=0, x IN [1,2] | s+reduce(t=s, y IN [x] | t+y))", 4),
        (
            "reduce(s=0, x IN [1,2] | s+CASE WHEN any(y IN [x] WHERE y=2) "
            "THEN 10 ELSE x END)",
            11,
        ),
        ("any(x IN [null] WHERE x IS NULL)", True),
        ("all(x IN [null] WHERE x IS NULL)", True),
        ("single(x IN [null] WHERE x IS NULL)", True),
        ("none(x IN [null] WHERE x IS NULL)", False),
        ("any(x IN [[1,2],[3]] WHERE size(x)=2)", True),
        ("all(x IN [{n:1},{n:2}] WHERE x.n>0)", True),
        ("all(x IN [1,2] WHERE any(x IN [3,4] WHERE x>2))", True),
    ],
)
def test_fold_expression(engine, expression, expected):
    assert engine.execute(f"RETURN {expression} AS result").to_pylist() == [
        {"result": expected}
    ]


def test_predicate_truth_tables(engine):
    """Exhaust true/false/null combinations through length three."""
    lists = [None] + [
        list(values)
        for length in range(4)
        for values in itertools.product([True, False, None], repeat=length)
    ]
    expected = []
    for values in lists:
        if values is None:
            expected.append(dict.fromkeys(["a", "b", "c", "d"]))
            continue
        yes = values.count(True)
        no = values.count(False)
        unknown = None in values
        expected.append(
            {
                "a": False if no else None if unknown else True,
                "b": True if yes else None if unknown else False,
                "c": False if yes else None if unknown else True,
                "d": False if yes > 1 else None if unknown else yes == 1,
            }
        )
    for values, want in zip(lists, expected, strict=True):
        result = engine.execute(
            "RETURN all(x IN $xs WHERE x) AS a, any(x IN $xs WHERE x) AS b, "
            "none(x IN $xs WHERE x) AS c, single(x IN $xs WHERE x) AS d",
            {"xs": values},
        ).to_pylist()
        assert result == [want], values


def test_captured_properties_scope_and_arrow_lists(engine):
    result = engine.execute(
        "MATCH (p:Person) RETURN p.id AS id, "
        "reduce(p=p.id, x IN p.items | p+coalesce(x,0)) AS reduced, "
        "all(x IN p.items WHERE x<p.id) AS predicate, "
        "reduce(s=0, x IN p.pair | s*10+x) AS pair, "
        "reduce(s=0, x IN p.large | s+x) AS large ORDER BY id"
    ).to_pylist()
    assert result == [
        {"id": 1, "reduced": 5, "predicate": False, "pair": 12, "large": 3},
        {"id": 2, "reduced": None, "predicate": None, "pair": None, "large": None},
        {"id": 3, "reduced": 3, "predicate": True, "pair": 34, "large": 0},
    ]


def test_predicate_in_where_and_case_insensitive_locals(engine):
    assert engine.execute(
        "MATCH (p:Person) WHERE any(X IN p.items WHERE x=3) "
        "RETURN reduce(S=0, X IN p.items | s+coalesce(x,0)) AS result"
    ).to_pylist() == [{"result": 4}]


def test_aggregate_input_and_comprehension_nesting(engine):
    assert engine.execute(
        "MATCH (p:Person) WITH p ORDER BY p.id "
        "RETURN reduce(s=0, x IN collect(p.id) | s*10+x) AS result"
    ).to_pylist() == [{"result": 123}]
    assert engine.execute(
        "MATCH (p:Person {id:1}) "
        "RETURN reduce(s=0, x IN [(p)-[:KNOWS]->(q:Person)|q.id] | s+x) AS result"
    ).to_pylist() == [{"result": 5}]
    assert engine.execute(
        "MATCH (p:Person {id:1}) RETURN "
        "[x IN [2,3] | reduce(w=0.0, v IN [(p)-[:KNOWS]->(q:Person) "
        "WHERE q.id=x | 0.5] | w+v)] AS result"
    ).to_pylist() == [{"result": [0.5, 0.5]}]


def test_graph_predicates_reuse_correlated_comprehensions(engine):
    result = engine.execute(
        "MATCH (p:Person) RETURN p.id AS id, "
        "any(x IN [2,3] WHERE EXISTS {MATCH (p)-[:KNOWS]->(q:Person) "
        "WHERE q.id=x}) AS connected "
        "ORDER BY id"
    ).to_pylist()
    assert result == [
        {"id": 1, "connected": True},
        {"id": 2, "connected": True},
        {"id": 3, "connected": False},
    ]
    for value, expected in [(None, None), ([], True)]:
        assert engine.execute(
            "MATCH (p:Person {id:1}) RETURN "
            "all(x IN $xs WHERE EXISTS {MATCH (p)-[:KNOWS]->(q:Person) "
            "WHERE q.id=x}) AS result",
            {"xs": value},
        ).to_pylist() == [{"result": expected}]


def test_graph_predicate_null_and_duplicate_capture_bindings(engine):
    result = engine.execute(
        "MATCH (p:Person {id:1}) RETURN "
        "[x IN [2,2,null,99] | any(y IN [x] WHERE EXISTS {"
        "MATCH (p)-[:KNOWS]->(q:Person) WHERE q.id=y OR y IS NULL})] AS result"
    ).to_pylist()
    assert result == [{"result": [True, True, True, False]}]
    # No shared graph variable: correlation consists only of the scalar iterator.
    assert engine.execute(
        "RETURN all(x IN [1,2,3] WHERE EXISTS {MATCH (p:Person) "
        "WHERE p.id=x}) AS result"
    ).to_pylist() == [{"result": True}]


def test_pattern_reduce_correlates_captured_struct_and_duplicate_rows(engine):
    assert engine.execute(
        "WITH [{id:2},{id:2},{id:3}] AS values UNWIND values AS captured "
        "RETURN captured.id AS id, reduce(s=0, x IN "
        "[(a:Person)-[:KNOWS]->(b:Person) WHERE b.id=captured.id | 1] | s+x) AS n "
        "ORDER BY id"
    ).to_pylist() == [{"id": 2, "n": 1}, {"id": 2, "n": 1}, {"id": 3, "n": 2}]


@pytest.mark.parametrize(
    "query",
    [
        "RETURN reduce(s=0, s IN [1] | s)",
        "RETURN reduce(s=0, x IN 1 | s+x)",
        "RETURN any(x IN 1 WHERE true)",
        "RETURN all(x IN [1] WHERE x)",
        "RETURN reduce(s=0, x IN [1] | s+undefined)",
        "RETURN reduce(s=0, x IN [1] | s+x) AS result, x",
        "RETURN any(x IN [1] WHERE x=1) AS result, x",
        "RETURN reduce(s=0, x IN [1] | toString(x))",
        "RETURN reduce(s=0, x IN [1] | size([y IN [x] | y]))",
        "MATCH (p:Person) RETURN reduce(s=0,x IN [1] | "
        "CASE WHEN EXISTS {MATCH (p)-[:KNOWS]->(:Person)} THEN s+x ELSE s END)",
    ],
)
def test_invalid_fold_scope_and_types(engine, query):
    with pytest.raises((ValueError, RuntimeError, NotImplementedError)):
        engine.execute(query)


@pytest.mark.parametrize("use_csr", [False, True], ids=["join", "csr"])
def test_sliced_lists_and_unreachable_null_storage(use_csr):
    config = GraphConfig.builder().node_table("Person", "id").build()
    items = pa.ListArray.from_arrays(
        pa.array([0, 2, 4], type=pa.int32()),
        pa.array([0, 0, 1, 2]),
        mask=pa.array([True, False]),
    )
    people = pa.table({"id": [1, 2], "items": items})
    query = (
        "MATCH (p:Person) RETURN p.id AS id, "
        "reduce(s=12,x IN p.items | s/x) AS value, "
        "all(x IN p.items WHERE 12/x>0) AS predicate ORDER BY id"
    )
    expected = [
        {"id": 1, "value": None, "predicate": None},
        {"id": 2, "value": 6, "predicate": True},
    ]
    for table, want in [(people, expected), (people.slice(1), expected[1:])]:
        local = CypherEngine(config, {"Person": table}, use_csr=use_csr)
        assert local.execute(query).to_pylist() == want


def test_fold_in_sort_and_unwind(engine):
    assert engine.execute(
        "MATCH (p:Person) RETURN p.id AS id ORDER BY reduce(s=0,x IN [p.id] | s-x)"
    ).to_pylist() == [{"id": 3}, {"id": 2}, {"id": 1}]
    assert engine.execute(
        "UNWIND [reduce(s=0,x IN [1,2] | s+x)] AS value RETURN value"
    ).to_pylist() == [{"value": 3}]
