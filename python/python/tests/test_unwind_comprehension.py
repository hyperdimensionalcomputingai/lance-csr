# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""UNWIND over a list comprehension, filtered or mapped."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("UNWIND [x IN [1, 2, 3] WHERE x > 1] AS y RETURN y", [2, 3]),
        ("UNWIND [x IN [1, 2, 3] | x * 10] AS y RETURN y", [10, 20, 30]),
        (
            "MATCH (n:N) WITH collect(n.id) AS ids "
            "UNWIND [i IN ids WHERE i > 1] AS y RETURN y",
            [2, 3],
        ),
    ],
)
def test_unwind_list_comprehension(query, expected):
    config = GraphConfig.builder().node_table("N", "id").build()
    engine = CypherEngine(config, {"N": pa.table({"id": [1, 2, 3]})})
    assert sorted(engine.execute(query).column("y").to_pylist()) == expected
