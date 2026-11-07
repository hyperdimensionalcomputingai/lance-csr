# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""An OR of ANDs in WHERE also filters on what each branch says about one variable.

`(a.x = 1 AND b.y = 2) OR (a.x = 2 AND b.y = 1)` implies `a.x = 1 OR a.x = 2`, which
can narrow `a` before expanding to `b`. The original predicate still decides the rows.
"""

import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig


@pytest.fixture(params=[False, True], ids=["joins", "csr"])
def engine(request):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .rel_table("FOLLOWS", "Person", "Person", "src", "dst")
        .build()
    )
    return CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1, 2, 3, 4], "age": [30, 40, None, 30]}),
            "KNOWS": pa.table({"src": [1, 2, 3], "dst": [2, 3, 4]}),
            # A duplicate edge must still count twice.
            "FOLLOWS": pa.table({"src": [1, 2, 2, 3, 4], "dst": [2, 1, 1, 2, 1]}),
        },
        use_csr=request.param,
    )


def test_match_with_either_direction_predicate(engine):
    query = (
        "MATCH (a:Person)-[:FOLLOWS]->(b:Person) "
        "WHERE (a.age = 30 AND b.age = 40) OR (a.age = 40 AND b.age = 30) "
        "RETURN a.id AS a, b.id AS b ORDER BY a, b"
    )
    # Person 3's null age matches neither branch.
    assert engine.execute(query).to_pylist() == [
        {"a": 1, "b": 2},
        {"a": 2, "b": 1},
        {"a": 2, "b": 1},
    ]


def test_comprehension_correlated_through_an_or(engine):
    # Per path relationship, the FOLLOWS edges between its ends in either direction.
    query = (
        "MATCH path = (:Person {id: 1})-[:KNOWS*1..3]->(:Person {id: 4}) "
        "RETURN [r IN relationships(path) | size([(a:Person)-[:FOLLOWS]->(b:Person) "
        "WHERE (a.id = startNode(r).id AND b.id = endNode(r).id) "
        "OR (a.id = endNode(r).id AND b.id = startNode(r).id) | 1])] AS w"
    )
    assert engine.execute(query).to_pylist() == [{"w": [3, 1, 0]}]
    if "CsrExpandExec" in (plan := engine.explain(query)):
        # The implied `a.id = ... OR a.id = ...` joins below the expansion to b,
        # instead of expanding from every person and filtering afterwards.
        physical = plan.split("physical_plan")[1]
        assert physical.index("CsrExpandExec") < physical.index("NestedLoopJoinExec")
