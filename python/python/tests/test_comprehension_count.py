# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Count-only comprehensions preserve multiplicity and predicate semantics."""

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
                    "id": [1, 2, 3, 4],
                    "values": pa.array(
                        [[1, None, 1, 2], [], None, [None]], type=pa.list_(pa.int64())
                    ),
                }
            ),
            # A duplicate edge affects the list's multiplicity; repeated matches
            # inside EXISTS must still contribute a single truth value per element.
            "KNOWS": pa.table({"src": [1, 1, 1, 2, 2], "dst": [2, 2, 3, 3, 3]}),
        },
        use_csr=request.param,
    )


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("", [4, 0, 0, 1]),
        ("WHERE x > 1", [1, 0, 0, 0]),
        ("WHERE x IS NULL", [1, 0, 0, 1]),
        ("WHERE x IS NULL OR x > 0", [4, 0, 0, 1]),
    ],
)
def test_count_matches_list_reference(engine, predicate, expected):
    query = (
        "MATCH (p:Person) UNWIND [1, 1] AS duplicate RETURN p.id AS id, "
        f"size([x IN p.values {predicate}]) AS n ORDER BY id"
    )
    reference = query.replace(f"{predicate}])", f"{predicate} | x])")
    rows = [
        {"id": index, "n": n} for index, n in enumerate(expected, 1) for _ in range(2)
    ]
    assert engine.execute(query).to_pylist() == rows
    assert engine.execute_batches(query).read_all().to_pylist() == rows
    assert engine.execute(reference).to_pylist() == rows
    assert "array_agg(" not in engine.explain(query).lower()
    assert "array_agg(" in engine.explain(reference).lower()


def test_count_with_node_lookup_and_exists_keeps_duplicate_elements(engine):
    query = (
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
        "WITH p, collect(f) AS friends "
        "RETURN p.id AS id, size([f IN friends WHERE (f)-[:KNOWS]->(:Person)]) AS n "
        "ORDER BY id"
    )
    expected = [
        {"id": 1, "n": 2},
        {"id": 2, "n": 0},
        {"id": 3, "n": 0},
        {"id": 4, "n": 0},
    ]
    assert engine.execute(query).to_pylist() == expected
    assert (
        engine.execute(query.replace("(:Person)])", "(:Person) | f])")).to_pylist()
        == expected
    )


def test_mapped_and_pattern_comprehensions_keep_normal_list_semantics(engine):
    query = (
        "MATCH (p:Person) RETURN p.id AS id, "
        "size([x IN p.values | null]) AS n ORDER BY id"
    )
    assert engine.execute(query).to_pylist() == [
        {"id": 1, "n": 4},
        {"id": 2, "n": 0},
        {"id": 3, "n": 0},
        {"id": 4, "n": 1},
    ]
    assert "array_agg(" in engine.explain(query).lower()
    pattern = (
        "MATCH (p:Person) RETURN p.id AS id, "
        "size([(p)-[:KNOWS]->(f) | f.id]) AS n ORDER BY id"
    )
    assert engine.execute(pattern).to_pylist() == [
        {"id": 1, "n": 3},
        {"id": 2, "n": 2},
        {"id": 3, "n": 0},
        {"id": 4, "n": 0},
    ]
    assert "array_agg(" in engine.explain(pattern).lower()
