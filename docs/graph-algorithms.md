# Graph algorithms

Cypher queries match patterns: they start from some nodes and follow relationships to find paths. Graph
algorithms such as PageRank, connected components or community detection work differently. They read the
whole graph and compute one value for every node, a score or a group. Libraries such as
[icebug](https://github.com/Ladybug-Memory/icebug), igraph and scipy implement them efficiently, and they all
take the graph in the same form: compressed sparse rows (CSR) over dense node ids.

[`CypherEngine.export_graph`][lance_csr.CypherEngine.export_graph] builds that form from your node and
relationship tables. It reads only the key and endpoint columns, plus a weight column when you ask for one,
from the same dataset versions your queries see, and returns Arrow arrays that these libraries take without
copying. Every parameter is optional, and all but `rel_types` are keyword-only:

| Parameter | Default | What it controls |
| --- | --- | --- |
| `rel_types` | `None` | The relationship types to include, or every type with integer endpoints; the nodes are those of the labels at either end. See [Directed and undirected graphs](#directed-and-undirected-graphs). |
| `directed` | `True` | `True` returns out-edge and in-edge arrays; `False` returns one set listing every edge both ways. See [Directed and undirected graphs](#directed-and-undirected-graphs). |
| `weight` | `None` | A column name, or a dict of relationship type to column, exported as `float64` edge weights; `None` exports none. See [Edge weights](#edge-weights). |
| `combine_edges` | `"sum"` | How an edge listed more than once combines its weights: `"sum"`, `"min"` or `"max"`. See [Edge weights](#edge-weights). |
| `default_weight` | `None` | The weight of every edge of a type with no weight column; `None` makes such a type an error. See [Several relationship types](#several-relationship-types). |

To export only the part of the graph a Cypher query selects, use `export_subgraph`, described in
[Subgraphs](#subgraphs).

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
| `out_weights`, `in_weights` | Edge weights, when asked for (see [Edge weights](#edge-weights)); otherwise `None` |
| `dropped_edges` | Edges whose endpoint key isn't in its node table, or whose weight is null |

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

## Edge weights

Some algorithms read a number on each edge as well as the edge itself, called the edge's *weight*: a distance
for shortest paths, a strength for weighted PageRank or community detection. A graph whose edges carry such
numbers is a *weighted graph*. In a relationship table the weight is a column, such as the messages sent along
each `FOLLOWS` edge. Name it with `weight` and the export returns it as `float64` arrays that line up with
`out_indices` and `in_indices`:

```python
messages = pa.table(
    {"src": [10, 10, 20, 30, 10], "dst": [20, 30, 30, 10, 20], "messages": [3, 1, 5, 2, 4]}
)
weighted_engine = CypherEngine(config, {"Person": people, "FOLLOWS": messages})
w = weighted_engine.export_graph(weight="messages")
print(w.out_indices.to_pylist())  # [1, 0, 2, 0]
print(w.out_weights.to_pylist())  # [2.0, 1.0, 7.0, 5.0]
```

The weight of `out_indices[i]` is `out_weights[i]`, and the same holds for the in-edges. The weight column
must be numeric. Only the endpoint and weight columns are read, so other properties of the relationship table
play no part.

Weights belong to edges only; the export has no node weights. A number that describes a node, such as a person's
age, stays in the node table, and `node_keys` gives the order to line it up with the dense ids if an algorithm
needs it.

### Several relationship types

When the export includes several relationship types, each needs a weight. They may store it under different
names, or one of them may have none. A `weight` dict names the column of each type, and `default_weight` gives
the weight of every edge of a type without a column:

```python
config3 = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("FOLLOWS", "Person", "Person", "src", "dst")
    .rel_table("MENTIONS", "Person", "Person", "src", "dst")
    .build()
)
mentions = pa.table({"src": [20], "dst": [10], "times": [9]})
mixed = CypherEngine(config3, {"Person": people, "FOLLOWS": messages, "MENTIONS": mentions})

both = mixed.export_graph(weight={"FOLLOWS": "messages", "MENTIONS": "times"})
print(both.out_weights.to_pylist())  # [2.0, 1.0, 7.0, 5.0, 9.0]
some = mixed.export_graph(weight={"FOLLOWS": "messages"}, default_weight=1.0)
print(some.out_weights.to_pylist())  # [2.0, 1.0, 7.0, 5.0, 1.0]
```

A string `weight` with `default_weight` works the same way: types that have the column use it, and the others
get the default. Without `default_weight`, a type with no weight column is an error, since a guessed weight
can change an algorithm's answer without warning. A few mistakes are errors even with a default: a column the
dict names that doesn't exist, a dict entry for a type that isn't exported, and a string column that no type
has. To leave a type out of the export instead, list the others in `rel_types`.

When the types' weights mean different things, such as messages and mentions, make sure adding them up, or
comparing them, makes sense for the algorithm.

A null weight means the weight is unknown, and no number stands in for it safely: 1.0 would make a costly
edge look cheap to a shortest-path search. So the export leaves the edge out and counts it in `dropped_edges`,
as it does an edge whose endpoint is missing:

```python
gaps = pa.table({"src": [10, 20], "dst": [20, 30], "messages": [3, None]})
partial = CypherEngine(config, {"Person": people, "FOLLOWS": gaps}).export_graph(weight="messages")
print(partial.out_weights.to_pylist(), partial.dropped_edges)  # [3.0] 1
```

The export lists each edge once, so an edge that appears in several rows needs one weight, and
`combine_edges` says how to get it. The default, `"sum"`, adds the weights, which suits counts and amounts:
10 → 20 appears twice, with 3 and 4 messages, and gets 7. When the weight is a distance or a cost, the
cheapest edge is usually the one that matters, and `"min"` keeps it; `"max"` keeps the largest:

```python
cheapest = weighted_engine.export_graph(weight="messages", combine_edges="min")
print(cheapest.out_weights.to_pylist())  # [2.0, 1.0, 3.0, 5.0]
```

An undirected export applies the same rule to an edge stored in both directions, so `a → b` and `b → a`
become one edge with one combined weight. A self-loop's weight counts once.

Weights cost memory only when you ask for them. Without `weight`, the export reads no extra column and
`out_weights` and `in_weights` are `None`.

## Subgraphs

An analysis often needs only part of the graph: the busiest accounts and their contacts, or one region's roads.
Ranking the whole graph and filtering the scores afterwards gives a different answer, since PageRank on a
subgraph only counts the edges inside it. [`export_subgraph`][lance_csr.CypherEngine.export_subgraph] runs a
Cypher query that selects the edges, and builds the same arrays as `export_graph` from its result:

```python
sub = engine.export_subgraph(
    "MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE a.id = $who RETURN a.id, b.id",
    {"who": 10},
)
print(sub.node_keys.to_pylist())  # [10, 20, 30]
print(sub.out_indptr.to_pylist(), sub.out_indices.to_pylist())  # [0, 2, 2, 2] [1, 2]
print(sub.label_ranges)  # {'Person': (0, 3)}
```

The query returns one row per edge, with the source and target node keys as its first two columns. The nodes are
the keys present in the result, sorted, so Dan (40) and anyone else outside the selected edges aren't in the
export. The edges follow the same rules as `export_graph`: an edge listed twice appears once, `directed=False`
lists each edge both ways, and a self-loop counts once.

When both key columns are a labeled node's key, as `a.id` here with `a` a `Person` and `id` its key, the export
groups the ids per label as `export_graph` does. `label_ranges` is filled, and
[`write_node_column`](#writing-results-back-to-lance) can write an algorithm's results back to the node table, with
nulls for the nodes outside the subgraph. When a column is computed, or its node has no single label, all keys
share one id space and `label_ranges` is empty; join `node_keys` to your own table instead.

The options are those of `export_graph`, except that the query does the selecting, so there is no `rel_types`,
and `weight` names a column of the query result:

| Parameter | Default | What it controls |
| --- | --- | --- |
| `query`, `params` | | The Cypher query and its parameters, as for [`execute`][lance_csr.CypherEngine.execute]. |
| `directed` | `True` | `True` returns out-edge and in-edge arrays; `False` returns one set listing every edge both ways. |
| `weight` | `None` | A numeric column of the result, such as `r.cost AS cost`, exported as `float64` edge weights. |
| `combine_edges` | `"sum"` | How an edge listed more than once combines its weights: `"sum"`, `"min"` or `"max"`. |
| `default_weight` | `None` | Without `weight`, the weight of every row. |

`default_weight=1.0` without `weight` gives each edge the number of rows that list it. That turns a result with
repeated rows into a graph where repeated edges count more, as PageRank on a multigraph does:

```python
counted = weighted_engine.export_subgraph(
    "MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.id, b.id", default_weight=1.0
)
print(counted.out_weights.to_pylist())  # [2.0, 1.0, 1.0, 1.0]
```

The query's result is held in memory while the arrays are built, as with
[`execute`][lance_csr.CypherEngine.execute].

## Running an algorithm with icebug

[icebug](https://github.com/Ladybug-Memory/icebug) is a fork of [NetworKit](https://networkit.github.io/) whose
graphs are built directly on Arrow CSR arrays. `Graph.fromCSR` takes the export's arrays without copying them, so
the only work left is the algorithm itself. Passing the arrays by name, as here, saves remembering the order
`fromCSR` expects them in:

<!-- skip-check -->
```python
import icebug as ib

graph = ib.Graph.fromCSR(  # (1)!
    n=g.num_nodes,  # number of nodes
    directed=g.directed,  # whether edges have a direction
    out_indices=g.out_indices,  # each node's out-neighbours, in node order
    out_indptr=g.out_indptr,  # where each node's list starts in out_indices
    in_indices=g.in_indices,  # the same for in-edges; None if undirected
    in_indptr=g.in_indptr,
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

### Edge weights in icebug

The PageRank above counts every follow the same. With [edge weights](#edge-weights), a follow can count for
more when more messages travel along it. The `messages` export from that section gives icebug a weighted graph
when you also pass its weight arrays to `fromCSR`, and the same PageRank call then uses them:

<!-- skip-check -->
```python
w = weighted_engine.export_graph(weight="messages")  # (1)!
graph = ib.Graph.fromCSR(
    n=w.num_nodes,
    directed=w.directed,
    out_indices=w.out_indices,
    out_indptr=w.out_indptr,
    in_indices=w.in_indices,
    in_indptr=w.in_indptr,
    out_weights=w.out_weights,  # the weight of each entry in out_indices
    in_weights=w.in_weights,  # the weight of each entry in in_indices
)
pagerank = ib.centrality.PageRank(
    graph, distributeSinks=ib.centrality.SinkHandling.DistributeSinks
)
scores = pagerank.run().scores()
print([round(s, 3) for s in scores])  # [0.331, 0.329, 0.292, 0.048]
```

1. `weighted_engine` is the engine from [Edge weights](#edge-weights), whose `FOLLOWS` table has a `messages`
   column.

Bob (key 20, dense id 2) rises from 0.205 without weights to 0.292. Alice sends him 7 messages and Carol only 1,
so most of the score Alice passes on now goes to Bob.

PageRank reads the weights, but not every icebug algorithm does. Some give the same answer on a weighted graph
as on an unweighted one, without a warning, so check an algorithm here before you rely on its weights. The
table lists what each one does in icebug 13.6. We found it by running each algorithm on the same graph with and
without weights:

| Module | Uses the weights | Ignores the weights, without a warning |
| --- | --- | --- |
| `distance` | `Dijkstra`, `BidirectionalDijkstra`, `AStar`, `APSP`, `Diameter` | `BFS`, `Eccentricity` |
| `centrality` | `PageRank`, `Betweenness`, `ApproxBetweenness`, `EstimateBetweenness`, `Closeness`, `ApproxCloseness`, `HarmonicCloseness`, `TopHarmonicCloseness`, `EigenvectorCentrality`, `KatzCentrality`, `ForestCentrality`, `SpanningEdgeCentrality` (on a copy) | `KadabraBetweenness`, `TopCloseness`, `DegreeCentrality`, `CoreDecomposition`, `LocalClusteringCoefficient` |
| `community` | `PLM` (Louvain), `ParallelLeiden`, `PLP` | `LPDegreeOrdered` |
| `graph` | `RandomMaximumSpanningForest` | `SpanningForest` |
| `matching` | `PathGrowingMatcher`, `SuitorMatcher` | |
| `flow` | `EdmondsKarp` (on a copy) | |

`KadabraBetweenness` is the one most likely to catch you out: it looks like a faster `Betweenness`, but it
computes unweighted betweenness. `centrality.ApproxElectricalCloseness`, not in the table, is the one algorithm
that refuses a weighted graph and raises an error.

Two of the algorithms that use the weights, `SpanningEdgeCentrality` and `EdmondsKarp`, also need an id on every
edge, which `indexEdges()` adds. A graph built by `fromCSR` can't be changed, so `indexEdges()` raises an error on
it, and these two run on an editable copy instead. Make the copy with `weighted=True`, because without it the
copy leaves the weights out. Here the messages act as capacities for a maximum flow. Alice sends Carol 1 message
directly and Bob 7, but Bob sends Carol only 5, so at most 6 can flow from Alice to Carol:

<!-- skip-check -->
```python
editable = ib.Graph(graph, weighted=True, directed=graph.isDirected())
editable.indexEdges()
alice, carol = 1, 0  # dense ids of keys 10 and 30
print(ib.flow.EdmondsKarp(editable, alice, carol).run().getMaxFlow())  # 6.0
```

The copy holds the whole graph a second time in memory.

!!! note "Limits that come from icebug"

    These limits are in icebug rather than in the export, so lance-csr can't work around them. icebug 13.6
    doesn't support edge ids on a graph built by `fromCSR`, which is why `SpanningEdgeCentrality` and
    `EdmondsKarp` need the copy. The algorithms in the right-hand column, and `ApproxElectricalCloseness`, don't
    support edge weights.

The algorithms that use the weights don't all read them the same way. Shortest paths, betweenness and closeness
treat a weight as a length, so a lower weight brings two nodes closer. PageRank, eigenvector and Katz
centrality, community detection, maximum spanning forests and matchings treat it as a strength, so a higher
weight ties two nodes more closely. One column rarely suits both: `messages` is a strength, so a shortest path
over it would prefer the follows with the fewest messages. For shortest paths, compute the inverse of each
weight array, such as `inverse_out = pyarrow.compute.divide(1.0, w.out_weights)`, and pass the inverses to
`fromCSR` in place of the weights. Dijkstra and the algorithms built on it also assume that no weight is
negative, and the export doesn't check this.

### Other libraries

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
  about as much again. Weights add eight bytes per edge in each direction, and building a weighted export
  briefly needs up to three times the memory of an unweighted one, because each edge is sorted together with its
  weight. An unweighted export needs no memory for weights.
- Keys and endpoints must be integers up to `int64`; string and `uint64` keys aren't supported.
- Only one edge property is exported, as a weight; other edge properties aren't.
