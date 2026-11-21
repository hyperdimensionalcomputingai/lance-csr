"""
Export a graph as CSR arrays, run PageRank and connected components with icebug, and write
the results back to the Lance node table.

icebug is a separate package; on macOS it also needs `brew install libomp`.
Run from the repository root (a lance-csr release with `export_graph`, or a local build):
  uv run --with lance-csr --with icebug --with networkx python examples/graph_algorithms.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import icebug as ib
import lance
import pyarrow as pa

from lance_csr import CypherEngine, GraphConfig


def main() -> None:
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("FOLLOWS", "Person", "Person", "src", "dst")
        .build()
    )
    people = pa.table({"id": [10, 20, 30, 40, 50], "name": ["Alice", "Bob", "Carol", "Dan", "Eve"]})
    follows = pa.table({"src": [10, 20, 30, 10, 40], "dst": [20, 30, 10, 30, 50]})
    root = Path(tempfile.mkdtemp())
    lance.write_dataset(people, root / "person.lance")
    lance.write_dataset(follows, root / "follows.lance")
    engine = CypherEngine(
        config,
        {"Person": lance.dataset(root / "person.lance"), "FOLLOWS": lance.dataset(root / "follows.lance")},
    )

    # PageRank follows edge direction and reads in-edges: a directed export has both.
    g = engine.export_graph()
    graph = ib.Graph.fromCSR(g.num_nodes, g.directed, g.out_indices, g.out_indptr, g.in_indices, g.in_indptr)
    pagerank = ib.centrality.PageRank(graph, distributeSinks=ib.centrality.SinkHandling.DistributeSinks)
    engine.write_node_column(g, "pagerank", pagerank.run().scores())

    # Components ignore direction: an undirected export lists each edge both ways.
    u = engine.export_graph(directed=False)
    undirected = ib.Graph.fromCSR(u.num_nodes, False, u.out_indices, u.out_indptr)
    components = ib.components.ConnectedComponents(undirected).run()
    engine.write_node_column(u, "component", components.getPartition().getVector())

    # Both are now columns of person.lance, and queries see them.
    result = engine.execute(
        "MATCH (p:Person) RETURN p.name, p.component, p.pagerank ORDER BY p.pagerank DESC"
    )
    for row in result.to_pylist():
        print(row)

if __name__ == "__main__":
    main()
