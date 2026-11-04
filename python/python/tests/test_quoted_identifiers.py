# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Quoted identifiers: backticks in Cypher, double quotes in SQL and GRAPH_TABLE."""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig, SqlEngine


@pytest.mark.parametrize("csr", [False, True], ids=["join", "csr"])
def test_backtick_quoted_identifiers(csr: bool) -> None:
    config = (
        GraphConfig.builder()
        .node_table("Device Type", "id")
        .rel_table("PART OF", "Device Type", "Device Type", "src_id", "dst_id")
        .build()
    )
    engine = CypherEngine(
        config,
        {
            "Device Type": pa.table(
                {
                    "id": [1, 2, 3],
                    "asset id": ["a-1", "a-2", "a-3"],
                    "name": ["pump", "valve", "tank"],
                }
            ),
            "PART OF": pa.table({"src_id": [1, 2], "dst_id": [3, 3]}),
        },
        use_csr=csr,
    )

    rows = engine.execute(
        "MATCH (x:`Device Type`) WHERE x.`asset id` IN $keys "
        "RETURN x.`asset id` AS `asset id`, x.name AS `a``b` ORDER BY `asset id`",
        {"keys": ["a-1", "a-3"]},
    ).to_pylist()
    assert rows == [
        {"asset id": "a-1", "a`b": "pump"},
        {"asset id": "a-3", "a`b": "tank"},
    ]

    rows = engine.execute(
        "MATCH (`match`:`Device Type`)-[:`PART OF`]->(`return`) "
        "RETURN `match`.name AS `where` ORDER BY `where`"
    ).to_pylist()
    assert rows == [{"where": "pump"}, {"where": "valve"}]

    # SQL quotes with double quotes; tables are registered under lowercase names.
    rows = (
        SqlEngine.from_engine(engine)
        .execute(
            """
            SELECT g."asset id", d.name AS "select" FROM GRAPH_TABLE (
                MATCH ("match":"Device Type")-[:"PART OF"]->(t) WHERE t.name = 'tank'
                COLUMNS ("match"."asset id" AS "asset id", t.id AS tid)
            ) g JOIN "device type" d ON d.id = g.tid
            ORDER BY g."asset id"
            """
        )
        .to_pylist()
    )
    assert rows == [
        {"asset id": "a-1", "select": "tank"},
        {"asset id": "a-2", "select": "tank"},
    ]
