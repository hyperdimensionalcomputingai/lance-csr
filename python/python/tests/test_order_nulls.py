# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""ORDER BY places nulls as openCypher does: last ascending, first descending."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)
TABLES = {
    "Person": pa.table({"id": [1, 2, 3, 4], "name": ["B", None, "A", "D"]}),
    "KNOWS": pa.table({"src": [1], "dst": [2]}),
}


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(
    ("order", "expected"),
    [
        ("ORDER BY name", ["A", "B", "D", None]),
        ("ORDER BY name ASC", ["A", "B", "D", None]),
        ("ORDER BY name DESC", [None, "D", "B", "A"]),
    ],
)
def test_order_by_null_placement(use_csr, order, expected):
    engine = CypherEngine(CONFIG, TABLES, use_csr)
    rows = engine.execute(f"MATCH (p:Person) RETURN p.name AS name {order}")
    assert [row["name"] for row in rows.to_pylist()] == expected
