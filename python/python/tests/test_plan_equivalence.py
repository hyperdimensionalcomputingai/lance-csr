# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Equivalent phrasings of a query get the same physical plan.

Where a traversal starts should depend on the query's meaning, not on how it is
written: anchor inline or in WHERE, the pattern written from either end, a path
named or not. Each group below lists phrasings of one query; all of them must give
the same plan and the same answer. Uses the graph from ``test_engine_csr.py``.
"""

import lance
import pytest
from lance_csr import CypherEngine
from test_engine_csr import CONFIG, KNOWS, PEOPLE

PATH = (
    "(c:Person{c})-[:KNOWS*1..2]->(d:Person{d})",
    "(d:Person{d})<-[:KNOWS*1..2]-(c:Person{c})",
)
TWO_HOPS = (
    "(c:Person{c})-[:KNOWS]->(b:Person)-[:KNOWS]->(d:Person{d})",
    "(d:Person{d})<-[:KNOWS]-(b:Person)<-[:KNOWS]-(c:Person{c})",
)


def phrasings(pattern: tuple[str, str], anchor: str, ret: str, names=("", "p = ")):
    """`pattern` written forward and backward, the anchor (`c` or `d`) inline and in
    WHERE, with each path name."""
    slots = {"c": "", "d": ""}
    return [
        f"MATCH {name}{body} {ret}"
        for name in names
        for text in pattern
        for body in (
            text.format(**{**slots, anchor: " {id: 1}"}),
            text.format(**slots) + f" WHERE {anchor}.id = 1",
        )
    ]


GROUPS = {
    "path from its start": phrasings(PATH, "c", "RETURN count(*) AS n"),
    "path from its end": phrasings(PATH, "d", "RETURN count(*) AS n"),
    "path returning ends": phrasings(PATH, "c", "RETURN d.id AS id ORDER BY id"),
    "two hops": phrasings(TWO_HOPS, "c", "RETURN d.id AS id ORDER BY id", names=("",)),
}


def physical(engine: CypherEngine, query: str) -> str:
    plan = engine.explain(query)
    plan = plan[plan.index("physical_plan") :]
    # The explain table pads every line to its widest one, which differs per query.
    return "\n".join(
        line.strip(" |") for line in plan.splitlines() if line.strip(" |+-")
    )


@pytest.fixture(params=["arrow", "lance"])
def engine(request, tmp_path):
    datasets = {"Person": PEOPLE, "KNOWS": KNOWS}
    if request.param == "lance":
        datasets = {
            "Person": lance.write_dataset(PEOPLE, tmp_path / "person.lance"),
            "KNOWS": lance.write_dataset(KNOWS, tmp_path / "knows.lance"),
        }
    return CypherEngine(CONFIG, datasets, use_csr=True)


@pytest.mark.parametrize("group", GROUPS)
def test_equivalent_phrasings_get_one_plan(engine, group):
    queries = GROUPS[group]
    plans = {}
    for query in queries:
        plans.setdefault(physical(engine, query), []).append(query)
    assert len(plans) == 1, "\n\n".join(
        "\n".join(qs) + "\n" + plan for plan, qs in plans.items()
    )
    answers = {str(engine.execute(query).to_pylist()) for query in queries}
    assert len(answers) == 1, answers
