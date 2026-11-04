# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""String operands use bound expressions with literal text semantics."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.mark.parametrize("csr", [False, True], ids=["join", "csr"])
def test_bound_string_operands(csr: bool) -> None:
    config = GraphConfig.builder().node_table("Person", "id").build()
    engine = CypherEngine(
        config,
        {
            "Person": pa.table(
                {"id": [1, 2, 3, 4], "name": ["Ada_%\\Zoë", "Ada", None, ""]}
            )
        },
        use_csr=csr,
    )
    for operator, needle, ids in [
        ("CONTAINS", "Ada", [1, 2]),
        ("CONTAINS", "%", [1]),
        ("CONTAINS", "_", [1]),
        ("CONTAINS", "\\", [1]),
        ("CONTAINS", "Zoë", [1]),
        ("CONTAINS", "", [1, 2, 4]),
        ("CONTAINS", None, []),
        ("CONTAINS", 42, []),
        ("STARTS WITH", "Ada", [1, 2]),
        ("ENDS WITH", "Zoë", [1]),
        ("STARTS WITH", "", [1, 2, 4]),
        ("ENDS WITH", None, []),
    ]:
        query = (
            f"MATCH (p:Person) WHERE p.name {operator} $text "
            "RETURN p.id AS id ORDER BY id"
        )
        assert engine.execute(query, {"text": needle}).to_pylist() == [
            {"id": value} for value in ids
        ]
    assert engine.execute(
        "MATCH (p:Person) WHERE toLower(p.name) STARTS WITH toLower($text) "
        "RETURN p.id AS id ORDER BY id SKIP $skip LIMIT $take",
        {"text": "ADA", "skip": 1, "take": 1},
    ).to_pylist() == [{"id": 2}]
    assert engine.execute(
        "MATCH (p:Person {id: $id}) RETURN p.id AS id", {"id": 2}
    ).to_pylist() == [{"id": 2}]
