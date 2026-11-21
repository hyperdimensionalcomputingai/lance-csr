# Graph algorithms

Cypher queries match patterns: they start from some nodes and follow relationships to find paths. Graph
algorithms such as PageRank, connected components or community detection work differently. They read the
whole graph and compute one value for every node, a score or a group. Libraries such as
[icebug](https://github.com/Ladybug-Memory/icebug), igraph and scipy implement them efficiently, and they all
take the graph in the same form: compressed sparse rows (CSR) over dense node ids.

[`CypherEngine.export_graph`][lance_csr.CypherEngine.export_graph] builds that form from your node and
relationship tables. It reads only the key and endpoint columns, from the same dataset versions your queries
see, and returns Arrow arrays that these libraries take without copying.

## What the export contains

A CSR numbers the nodes `0` to `n - 1`, its *dense ids*, and stores the edges in two arrays:

- `indices` lists every node's neighbours, one node after another, each list sorted;
- `indptr` has `n + 1` offsets: the neighbours of node `i` are `indices[indptr[i]:indptr[i + 1]]`.

Your node keys are rarely `0` to `n - 1`, so the export also returns `node_keys`, the key of each dense id. Here
is a small graph with keys 10 to 40, where 40 has no edges:

```python
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("FOLLOWS", "Person", "Person", "src", "dst")
    .build()
)
people = pa.table({"id": [30, 10, 20, 40], "name": ["Carol", "Alice", "Bob", "Dan"]})
follows = pa.table({"src": [10, 10, 20, 30], "dst": [20, 30, 30, 10]})
engine = CypherEngine(config, {"Person": people, "FOLLOWS": follows})

g = engine.export_graph()
print(g.node_keys.to_pylist())  # [30, 10, 20, 40]
print(g.out_indptr.to_pylist())  # [0, 1, 3, 4, 4]
print(g.out_indices.to_pylist())  # [1, 0, 2, 0]
```

Dense ids follow the order of the node table, so 30 is id 0 and 10 is id 1. Node 1 (key 10) follows ids 0 and 2
(keys 30 and 20), and node 3 (key 40) has an empty range. The arrays are `uint64`; `node_keys` is `int64`.

The returned [`GraphExport`][lance_csr.GraphExport] also holds:

| Field | Meaning |
| --- | --- |
| `num_nodes`, `directed` | The number of nodes and whether the arrays are directed |
| `in_indptr`, `in_indices` | The same arrays for incoming edges; `None` for an undirected export |
| `label_ranges` | Each node label's dense ids, as `(start, end)` with `end` excluded |
| `dropped_edges` | Edges whose endpoint key isn't in its node table |

The export follows the same rules as a query's join between relationship and node tables. An edge whose
endpoint key isn't in its node table is left out (and counted in `dropped_edges`), an edge with a null endpoint is
skipped, and the same edge listed twice appears once. Keys must be unique within a label, and both keys and
endpoints must be integer columns up to `int64`.

## Directed and undirected graphs

Some algorithms treat relationships as one-way and others don't. PageRank follows the direction of each edge,
and reads the edges *into* each node, so a directed export returns the in-edges too:

```python
print(g.in_indptr.to_pylist())  # [0, 2, 3, 4, 4]
print(g.in_indices.to_pylist())  # [1, 2, 0, 1]
```

Connected components and most community detection algorithms ignore direction. `directed=False` returns one
set of arrays that lists every edge in both directions, and a self-loop once:

```python
u = engine.export_graph(["FOLLOWS"], directed=False)
print(u.out_indptr.to_pylist())  # [0, 2, 4, 6, 6]
print(u.in_indptr)  # None
```

`rel_types` picks the relationship types to include; left out, every type whose dataset has integer endpoints
is included. The nodes are those of the labels at either end. Each label gets its own range of dense ids, in label
name order, so equal keys in two tables stay distinct nodes:

```python
config2 = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .node_table("City", "id")
    .rel_table("LIVES_IN", "Person", "City", "src", "dst")
    .build()
)
cities = pa.table({"id": [1, 2]})
lives_in = pa.table({"src": [10, 20], "dst": [1, 2]})
engine2 = CypherEngine(config2, {"Person": people, "City": cities, "LIVES_IN": lives_in})
print(engine2.export_graph().label_ranges)  # {'City': (0, 2), 'Person': (2, 6)}
```

## Running an algorithm with icebug

[icebug](https://github.com/Ladybug-Memory/icebug) is a fork of [NetworKit](https://networkit.github.io/) whose
graphs are built directly on Arrow CSR arrays. `Graph.fromCSR` takes the export's arrays without copying them, so
the only work left is the algorithm itself:

<!-- skip-check -->
```python
import icebug as ib

graph = ib.Graph.fromCSR(  # (1)!
    g.num_nodes, g.directed, g.out_indices, g.out_indptr, g.in_indices, g.in_indptr
)
pagerank = ib.centrality.PageRank(
    graph, distributeSinks=ib.centrality.SinkHandling.DistributeSinks  # (2)!
)
scores = pa.table({"id": g.node_keys, "pagerank": pagerank.run().scores()})
```

1. Keep `graph` in a variable for as long as an algorithm uses it. The algorithm objects don't keep the graph
   alive, and a graph freed early gives wrong results without an error.
2. Spreads the score of nodes without out-edges over all nodes, as networkx and most definitions of PageRank do.
   icebug's default leaves it out.

!!! note "Installing icebug"

    `icebug` is a separate package (`uv add icebug`); lance-csr doesn't depend on it. Version 13.6 requires
    exactly `pyarrow==26.0.1`, imports only when `networkx` is installed, and on macOS needs OpenMP from
    Homebrew (`brew install libomp`).

Other libraries take the same arrays. For example, `scipy.sparse.csr_array((data, g.out_indices, g.out_indptr))`
builds an adjacency matrix, after converting the arrays with `to_numpy()`, which doesn't copy them either.

## Writing results back to Lance

An algorithm's results are a value per dense id. To query them alongside the rest of a node's properties, they
have to become a column of the node table, matched to each node by its key.
[`write_node_column`][lance_csr.CypherEngine.write_node_column] does this in one call. The export already says
which dense ids belong to which label (`label_ranges`) and what each node's key is (`node_keys`), and the config
says which dataset and key column each label has, so the engine can route every value to its row.

Here the out-degree of each node, computed from `out_indptr`, stands in for an algorithm's result:

```python
import lance

lance.write_dataset(people, "people.lance")
lance.write_dataset(follows, "follows.lance")
engine = CypherEngine(
    config, {"Person": lance.dataset("people.lance"), "FOLLOWS": lance.dataset("follows.lance")}
)
g = engine.export_graph()
indptr = g.out_indptr.to_numpy()
engine.write_node_column(g, "out_degree", indptr[1:] - indptr[:-1])  # (1)!

top = engine.execute(
    "MATCH (p:Person) RETURN p.name, p.out_degree ORDER BY p.out_degree DESC, p.name LIMIT 2"
)
print(top.to_pylist())
# [{'p.name': 'Alice', 'p.out_degree': 2}, {'p.name': 'Bob', 'p.out_degree': 1}]
```

1. `values` can be a pyarrow array, a NumPy array or a list, such as the scores icebug returns. With the icebug
   example above, this is `engine.write_node_column(g, "pagerank", pagerank.run().scores())`.

For each label in the export, the engine pairs that label's values with its keys and uses Lance's `merge` to
join them to the node table on the key column. Lance writes only the new column's data files, as a new version
of the dataset. A node row that the export left out because its key is null gets a null.

The engine then moves to the new versions of those node tables, so the next query sees the column without a
[`checkout_latest`](lance.md#checkout-latest). Other datasets stay at the versions the engine was pinned to.
Relationship tables don't change, so the CSR index isn't rebuilt.

Some cases need care:

- **Running an algorithm again.** Writing to a column that already exists raises `ValueError`, so a result isn't
  replaced by accident. Pass `overwrite=True` to drop the old column and write the new one.
- **Versions build up.** Each write adds a version to every node table it touches, two with `overwrite=True`,
  and a replaced column's files stay on disk until old versions are cleaned up.
  [`optimize_datasets`](lance.md#compaction-and-old-versions) cleans them up and keeps the engine in step.
- **Several labels.** Each label is a separate Lance write. A failure partway through leaves the labels before
  it written.
- **Arrow inputs.** Only Lance datasets can be written to. For a node table passed as a PyArrow table, pair the
  values with the keys yourself: `pa.table({"id": g.node_keys, "out_degree": degree})`.
- **Keeping the node table unchanged.** To keep results apart from the node table, for example to compare
  several runs, write that same table to a separate dataset with `lance.write_dataset`.

## Limits

- The export holds the whole graph in memory: eight bytes per edge in each direction for `indices`, plus the
  offsets and keys. A directed export of 10 million edges takes about 160 MB, and building it briefly needs
  about as much again.
- Keys and endpoints must be integers up to `int64`; string and `uint64` keys aren't supported.
- Edge properties, such as weights, aren't exported.
