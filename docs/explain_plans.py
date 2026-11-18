"""Print the explain() plans pasted in docs/internals/ and docs/csr.md, from the local build.

The pages paste physical plans as `text` blocks, often trimmed to the lines that matter, so they can't be
compared automatically. Run this after changing an operator, an optimizer rule or the planner, and compare each
section's plan with the page it names; update the page where they differ.

Each section rebuilds the graph its page describes: a four-person `Person`/`KNOWS` graph, twelve nodes with
complete `R` and `S` relationships, and the 100,000-person graph of the graph-benchmark repository
(https://github.com/hyperdimensionalcomputingai/graph-benchmark), all in memory with `use_csr=True`. The
benchmark sections run only when `--graph-benchmark` points at a checkout whose `lance_csr/graph_lance/` holds
the datasets (run its `lance_csr/build_graph.py` first).

    cd python && uv run --no-sync python ../docs/explain_plans.py --graph-benchmark ../../graph-benchmark

When a page gains a plan, add its query here under the page's name.
"""

import argparse
from pathlib import Path

import lance
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig


def physical(engine: CypherEngine, query: str, params: dict | None = None) -> str:
    "The physical plan, without the explain table's borders."
    lines, inside = [], False
    for row in engine.explain(query, params).splitlines():
        cells = row.split("|")
        if len(cells) < 4:
            continue
        kind, plan = cells[1].strip(), cells[2].rstrip()
        if kind:
            inside = kind == "physical_plan"
        if inside:
            lines.append(plan[1:])
    return "\n".join(lines).rstrip()


def show(
    page: str, engine: CypherEngine, query: str, params: dict | None = None
) -> None:
    print(f"\n######## {page}\n{query.strip()}\n\n{physical(engine, query, params)}")


def people(use_csr: bool) -> CypherEngine:
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    person = pa.table(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dan"],
            "age": [30, 40, 25, 50],
        }
    )
    knows = pa.table(
        {"src": [1, 2, 3, 1], "dst": [2, 3, 4, 3], "since": [2019, 2021, 2022, 2023]}
    )
    return CypherEngine(config, {"Person": person, "KNOWS": knows}, use_csr=use_csr)


def complete_twelve() -> CypherEngine:
    config = (
        GraphConfig.builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .rel_table("S", "N", "N", "src", "dst")
        .build()
    )
    ids = list(range(12))
    edges = pa.table(
        {"src": [a for a in ids for _ in ids], "dst": [b for _ in ids for b in ids]}
    )
    return CypherEngine(
        config, {"N": pa.table({"id": ids}), "R": edges, "S": edges}, use_csr=True
    )


def benchmark_graph(root: Path) -> CypherEngine:
    labels = ("Person", "City", "State", "Country", "Interest")
    rels = (
        ("FOLLOWS", "Person", "Person"),
        ("LIVES_IN", "Person", "City"),
        ("HAS_INTEREST", "Person", "Interest"),
        ("CITY_IN", "City", "State"),
        ("STATE_IN", "State", "Country"),
    )
    builder = GraphConfig.builder()
    for label in labels:
        builder = builder.node_table(label, "id")
    for rel, src, dst in rels:
        builder = builder.rel_table(rel, src, dst, "src", "dst")
    names = labels + tuple(rel for rel, _, _ in rels)
    data = {
        name: lance.dataset(str(root / f"{name}.lance")).to_table() for name in names
    }
    return CypherEngine(builder.build(), data, use_csr=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--graph-benchmark",
        type=Path,
        help="a graph-benchmark checkout, for the 100K-person plans",
    )
    args = parser.parse_args()

    csr, joins = people(True), people(False)
    two_hop = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person {name: 'Dan'}) RETURN a.name"
    show("operators.md: CsrExpand, use_csr=True", csr, two_hop)
    show("operators.md: CsrExpand, use_csr=False", joins, two_hop)
    show(
        "operators.md: several capabilities; optimizer.md: why the order matters",
        csr,
        "MATCH (a:Person {id: 1})-[k:KNOWS]->(b:Person) WHERE k.since > 2020 AND b.age > 35 RETURN b.name",
    )
    show(
        "operators.md: variable-length path",
        csr,
        "MATCH (a:Person {id: 1})-[:KNOWS*1..3]->(b:Person) RETURN b.name",
    )
    show(
        "operators.md: NodeLookup",
        csr,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b, count(*) AS n RETURN b.name, n",
    )
    exists = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE NOT (b)-[:KNOWS]->(a) RETURN a.name, b.name"
    show("operators.md: PathExists, use_csr=True", csr, exists)
    show("operators.md: PathExists, use_csr=False", joins, exists)
    show(
        "operators.md: ShortestPath",
        csr,
        "MATCH (a:Person {id: 1}), (b:Person {id: 4}) MATCH p = shortestPath((a)-[:KNOWS*]-(b)) RETURN length(p)",
    )

    twelve = complete_twelve()
    show(
        "optimizer.md: grouped two-hop count by the middle node",
        twelve,
        "MATCH (a:N)-[:R]->(b:N)-[:S]->(c:N) RETURN b.id, count(*) AS n",
    )
    show(
        "optimizer.md: grouped two-hop count by an outer node",
        twelve,
        "MATCH (a:N)-[:R]->(b:N)-[:S]->(c:N) RETURN a.id, count(*) AS n",
    )

    if args.graph_benchmark is None:
        print("\n(skipped the 100K-person plans: pass --graph-benchmark)")
        return
    graph = benchmark_graph(args.graph_benchmark / "lance_csr" / "graph_lance")
    show(
        "optimizer.md: two-hop counts from degrees",
        graph,
        """MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
WHERE b.age < $age_1 AND c.age > $age_2 RETURN count(*) AS numpaths""",
        {"age_1": 50, "age_2": 25},
    )
    show(
        "optimizer.md: counting the first hop per node",
        graph,
        """MATCH (follower:Person)-[:FOLLOWS]->(person:Person)-[:LIVES_IN]->(city:City)
RETURN person.name AS name, count(follower.id) AS numfollowers, city.city AS city, city.state AS state,
       city.country AS country ORDER BY numfollowers DESC LIMIT 1""",
    )
    show(
        "csr.md: spotting a hop over the whole relationship",
        graph,
        "MATCH (follower:Person)-[:FOLLOWS]->(person:Person) RETURN person.name, max(follower.age)",
    )


if __name__ == "__main__":
    main()
