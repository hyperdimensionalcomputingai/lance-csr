# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Experimental CSR single-hop expansion (``CypherEngine(..., use_csr=True)``).

Every query runs on a CSR engine and a join-path engine over the same data; the
sorted results must be identical.
"""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

CONFIG = (
    GraphConfig.builder()
    .with_node_label("Person", "id")
    .with_relationship("KNOWS", "src", "dst")
    .build()
)

PEOPLE = pa.table(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["a", "b", "c", "d", "e", "f"],
        "age": [20, 30, 40, 50, 60, 70],
    }
)
# Duplicate edge (1->2 twice), self loop (3->3), null endpoints, a dangling edge
# to a missing node (4->99), and an isolated node (6).
KNOWS = pa.table(
    {
        "src": pa.array([1, 1, 1, 2, 3, 4, None, 5, 5], pa.int64()),
        "dst": pa.array([2, 2, 3, 3, 3, 99, 1, None, 1], pa.int64()),
        "since": [2001, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008],
    }
)

QUERIES = [
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 25 RETURN a.id, b.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age >= 40 RETURN a.id, b.age",
    "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.id, b.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.id, c.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n",
    "MATCH (a:Person)-[:KNOWS]->(a) RETURN a.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 1 RETURN b.id, b.name, b.age",
    "MATCH (a:Person)<-[:KNOWS]-(b:Person) WHERE a.id = 3 RETURN b.name",
    "MATCH (a:Person)-[:KNOWS]->(b:Person {age: 40}) RETURN a.id",
    # Target filter moved into the CSR node (start is more selective than b).
    "MATCH (a:Person)-[:KNOWS]->(b:Person) "
    "WHERE a.id = 1 AND b.age >= 40 RETURN b.name",
    # Closing edges: cycles back to an already-bound variable (duplicates included).
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a) RETURN a.id, b.id",
    "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)<-[:KNOWS]-(a) RETURN count(*) AS n",
    # Closing onto a variable whose id came from an earlier CSR take (LDBC Q30 shape).
    "MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:KNOWS]->(c:Person)-[:KNOWS]->(b) "
    "RETURN count(*) AS n",
    # Variable-length paths: every hop is a CSR expansion (undirected ones included).
    "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.id, b.id",
    "MATCH (a:Person)<-[:KNOWS*2..2]-(b:Person) RETURN a.id, b.id",
    "MATCH (a:Person)-[:KNOWS*1..3]-(b:Person) WHERE a.id = 1 RETURN b.id, b.name",
    "MATCH (a:Person)-[:KNOWS*1..2]-(b:Person) RETURN count(*) AS n",
    # Shapes that keep the join path: bound relationship variable, property filter.
    "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.id, b.id",
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.id, r.since",
    "MATCH (a:Person)-[:KNOWS {since: 2001}]->(b:Person) RETURN a.id, b.id",
]


def rows(table):
    return sorted(table.to_pylist(), key=lambda r: [str(v) for v in r.values()])


@pytest.fixture(params=["arrow", "lance"])
def datasets(request, tmp_path):
    if request.param == "arrow":
        return {"Person": PEOPLE, "KNOWS": KNOWS}
    return {
        "Person": lance.write_dataset(PEOPLE, tmp_path / "person.lance"),
        "KNOWS": lance.write_dataset(
            KNOWS, tmp_path / "knows.lance", max_rows_per_file=4
        ),
    }


@pytest.mark.parametrize("query", QUERIES)
def test_csr_matches_join_path(datasets, query):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    join = CypherEngine(CONFIG, datasets)
    assert rows(csr.execute(query)) == rows(join.execute(query))


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize("rel", ["[:KNOWS]", "[:KNOWS*1..1]"])
def test_undirected_matches_edges_from_both_ends(datasets, use_csr, rel):
    # Each edge matches in both directions; the self loop 3->3 matches once.
    engine = CypherEngine(CONFIG, datasets, use_csr=use_csr)
    got = engine.execute(f"MATCH (a:Person)-{rel}-(b:Person) RETURN a.id, b.id")
    pairs = [(1, 2), (1, 2), (1, 3), (2, 3), (5, 1)]
    want = pairs + [(b, a) for a, b in pairs] + [(3, 3)]
    assert sorted(zip(*got.to_pydict().values())) == sorted(want)


def test_csr_operator_used_only_for_supported_shapes(datasets):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    join = CypherEngine(CONFIG, datasets)
    single = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.id"
    assert "CsrExpand" in csr.explain(single)
    assert "CsrExpand" not in join.explain(single)
    bound = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since"
    assert "CsrExpand" not in csr.explain(bound)


def test_variable_length_hops_use_csr(datasets):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    plan = csr.explain("MATCH (a:Person)-[:KNOWS*1..2]-(b:Person) RETURN b.id")
    lines = plan[plan.index("physical_plan") :].splitlines()
    expands = [line for line in lines if "CsrExpandExec" in line]
    # One expansion for the 1-hop branch and two for the 2-hop branch.
    assert len(expands) == 3 and all("undirected" in line for line in expands)
    assert "HashJoinExec" not in "\n".join(lines)


def test_source_filter_pushed_below_csr(datasets):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    plan = csr.explain(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 25 RETURN b.id"
    )
    lines = plan[plan.index("physical_plan") :].splitlines()
    csr_line = next(i for i, line in enumerate(lines) if "CsrExpand" in line)
    filters = [
        i
        for i, line in enumerate(lines)
        if ("FilterExec" in line or "full_filter" in line) and "age" in line
    ]
    # Children print below parents: the filter on `a` must sit under the expansion.
    assert filters and min(filters) > csr_line


def test_csr_stats_report_edges_and_source_version(tmp_path):
    knows = lance.write_dataset(KNOWS, tmp_path / "knows.lance")
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": knows}, use_csr=True)
    stats = engine.csr_stats()
    # Edges with a null endpoint are not indexed (they can never join).
    assert stats["KNOWS"]["edges"] == 7
    assert stats["KNOWS"]["version"] == 1
    assert stats["KNOWS"]["build_ms"] >= 0
    assert CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": knows}).csr_stats() == {}


def test_csr_tracks_edge_changes_but_not_node_changes(tmp_path):
    person = lance.write_dataset(PEOPLE, tmp_path / "person.lance")
    knows = lance.write_dataset(KNOWS, tmp_path / "knows.lance")
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows}, use_csr=True)
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 6 RETURN b.id"
    before = engine.csr_stats()

    # Node-table change only: the CSR depends on the edge table alone.
    lance.write_dataset(
        pa.table({"id": [7], "name": ["g"], "age": [80]}),
        tmp_path / "person.lance",
        mode="append",
    )
    engine.refresh()
    assert engine.csr_stats() == before

    new_edge = pa.table(
        {
            "src": pa.array([6], pa.int64()),
            "dst": pa.array([1], pa.int64()),
            "since": [2009],
        }
    )
    lance.write_dataset(new_edge, tmp_path / "knows.lance", mode="append")
    assert engine.execute(query).num_rows == 0  # pinned snapshot
    engine.refresh()
    after = engine.csr_stats()
    assert after["KNOWS"]["version"] == 2 and after["KNOWS"]["edges"] == 8
    assert engine.execute(query).column(0).to_pylist() == [1]


def test_non_integer_ids_fall_back_to_join():
    people = pa.table({"id": ["x", "y"], "name": ["a", "b"]})
    knows = pa.table({"src": ["x"], "dst": ["y"]})
    csr = CypherEngine(CONFIG, {"Person": people, "KNOWS": knows}, use_csr=True)
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name"
    assert "CsrExpand" not in csr.explain(query)
    assert csr.execute(query).column(0).to_pylist() == ["b"]
    assert csr.csr_stats() == {}


def test_execute_batches_with_csr(datasets):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id"
    streamed = pa.Table.from_batches(list(csr.execute_batches(query)))
    assert rows(streamed) == rows(csr.execute(query))


def physical_plan(engine, query):
    plan = engine.explain(query)
    return plan[plan.index("physical_plan") :]


def test_target_lookup_replaces_target_join(datasets):
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 1 RETURN b.name"
    csr_plan = physical_plan(CypherEngine(CONFIG, datasets, use_csr=True), query)
    assert "CsrExpandExec" in csr_plan and "take=b" in csr_plan
    assert "HashJoinExec" not in csr_plan
    assert "HashJoinExec" in physical_plan(CypherEngine(CONFIG, datasets), query)


def test_cross_label_target_lookup(tmp_path):
    config = (
        GraphConfig.builder()
        .with_node_label("Person", "id")
        .with_node_label("Company", "cid")
        .with_relationship("WORKS_AT", "pid", "cid")
        .build()
    )
    company = pa.table({"cid": pa.array([10, 20], pa.int32()), "cname": ["x", "y"]})
    works = pa.table({"pid": [1, 2, 2, 3], "cid": [10, 10, 20, 30]})
    datasets = {
        "Person": PEOPLE,
        "Company": lance.write_dataset(company, tmp_path / "company.lance"),
        "WORKS_AT": works,
    }
    query = "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.cid, c.cname"
    csr = CypherEngine(config, datasets, use_csr=True)
    assert "HashJoinExec" not in physical_plan(csr, query)
    assert rows(csr.execute(query)) == rows(
        CypherEngine(config, datasets).execute(query)
    )


def test_lance_node_table_with_deleted_rows(tmp_path):
    person = lance.write_dataset(PEOPLE, tmp_path / "person.lance", max_rows_per_file=2)
    person.delete("id = 2 OR id = 5")
    person = lance.dataset(tmp_path / "person.lance")
    datasets = {"Person": person, "KNOWS": KNOWS}
    for query in QUERIES:
        csr = CypherEngine(CONFIG, datasets, use_csr=True)
        assert rows(csr.execute(query)) == rows(
            CypherEngine(CONFIG, datasets).execute(query)
        ), query


def test_duplicate_node_ids_fall_back_to_join():
    people = pa.table({"id": [1, 2, 2], "name": ["a", "b", "b2"], "age": [1, 2, 3]})
    datasets = {"Person": people, "KNOWS": KNOWS}
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 1 RETURN b.name"
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    assert "HashJoinExec" in physical_plan(csr, query)
    assert rows(csr.execute(query)) == rows(
        CypherEngine(CONFIG, datasets).execute(query)
    )
    assert "Person" not in csr.node_index_stats()


def test_node_index_rebuilt_only_when_node_version_changes(tmp_path):
    person = lance.write_dataset(PEOPLE, tmp_path / "person.lance")
    knows = lance.write_dataset(KNOWS, tmp_path / "knows.lance")
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows}, use_csr=True)
    before = engine.node_index_stats()
    assert before["Person"]["rows"] == 6 and before["Person"]["version"] == 1

    # Edge-only change: the node index is reused.
    edge = pa.table(
        {
            "src": pa.array([6], pa.int64()),
            "dst": pa.array([7], pa.int64()),
            "since": [1],
        }
    )
    lance.write_dataset(edge, tmp_path / "knows.lance", mode="append")
    engine.refresh()
    assert engine.node_index_stats() == before
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 6 RETURN b.name"
    assert engine.execute(query).num_rows == 0  # node 7 does not exist yet

    lance.write_dataset(
        pa.table({"id": [7], "name": ["g"], "age": [80]}),
        tmp_path / "person.lance",
        mode="append",
    )
    engine.refresh()
    after = engine.node_index_stats()
    assert after["Person"]["version"] == 2 and after["Person"]["rows"] == 7
    assert engine.execute(query).column(0).to_pylist() == ["g"]


@pytest.mark.parametrize(
    ("query", "taken"),
    [
        ("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n", "take=b[]"),
        ("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.id", "take=b[b__id*]"),
        ("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name", "take=b[b__name]"),
        (
            # Equal filters on both ends keep the textual start, so `b` is taken; its
            # filter is evaluated on the Person table, so `b.age` is not taken per row.
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.age > 0 AND b.age > 30 RETURN b.id",
            "take=b[b__id*], filter=",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.id, b.age",
            "take=b[b__id*, b__age]",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name",
            "take=b[b__id*]",
        ),
    ],
)
def test_target_take_fetches_only_used_columns(datasets, query, taken):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    assert taken in physical_plan(csr, query)
    assert rows(csr.execute(query)) == rows(
        CypherEngine(CONFIG, datasets).execute(query)
    )


def test_target_filter_runs_before_the_take(datasets):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    plan = physical_plan(
        csr,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) "
        "WHERE a.id = 1 AND b.age >= 40 RETURN b.name",
    )
    expand = next(line for line in plan.splitlines() if "CsrExpandExec" in line)
    # The filter column is evaluated on the target table, not taken per row.
    assert "filter=" in expand and "take=b[b__name]" in expand
    assert "FilterExec" not in plan.split("CsrExpandExec")[0]


def test_closing_edge_checks_bound_target(datasets):
    csr = CypherEngine(CONFIG, datasets, use_csr=True)
    plan = physical_plan(
        csr, "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a) RETURN a.id"
    )
    assert "match=a__id" in plan
