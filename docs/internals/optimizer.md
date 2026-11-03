# Optimizer rules and rewrites

lance-csr changes plans at two levels: graph rewrites before the plan reaches DataFusion, and DataFusion optimizer
rules that understand the [custom operators](operators.md). It also gives DataFusion the join statistics it needs for
the hops that stay relational.

The rules, rewrites and statistics on this page were added in lance-csr. Unrolling a single-type variable-length path
into one branch per length came from [lance-graph](https://github.com/lance-format/lance-graph); lance-csr added the
iterative traversal that replaces it when the CSR covers the path.

## Logical optimizer rules

DataFusion's own optimizer treats extension nodes as opaque, so two `OptimizerRule`s fill the gaps.

### `PushCsrTargetFilters`

DataFusion can push a filter below a join, but not into an extension node. This rule splits a `Filter` above a
`CsrExpand` into its conjuncts, looking through column-only projections that projection pushdown may have left between
them, and moves:

- conjuncts that read only the fused target's columns into the node's target filter;
- conjuncts that read only the named relationship's own columns into its relationship filter.

Conjuncts that mix variables, or are volatile, stay where they were. The operator then reads just the filter's columns
for each batch of candidates, evaluates the predicate, and fetches the remaining columns only for rows that pass.
Evaluating the filter once over the whole target table was tried first; it cost more than it saved whenever the
expansion was already small, which re-rooting usually ensures.

### `PruneCsrTargetColumns`

DataFusion's projection pushdown prunes an extension node's inputs but not its outputs, so without this rule a fused
lookup would fetch every target column for every row. The rule collects the columns used anywhere in the plan and
narrows each `CsrExpand`'s target and relationship columns, and each `NodeLookup`, to that set. A `NodeLookup` whose
columns are all unused is removed. The columns a `NodeLookup` fetches shadow any of the same name below it, so the rule
prunes the plan under each lookup again, against only what that plan outputs: a hop under
`NodeLookup: take=b[b__name]` doesn't fetch `b__name` for every edge just because the lookup's `b__name` is used above.

"Used" is conservative: expression references, the plan's output columns, each CSR node's key and closing column, and
all inputs of nodes whose column use is implicit, such as `DISTINCT` and `UNION`.

### Why the order matters

`PushCsrTargetFilters` runs first. After a filter moves into the node, the columns only the filter reads are no longer
referenced above it, so `PruneCsrTargetColumns` can drop them from the output. The logical plan (before the rules) and
the physical plan (after them) of one hop show both:

```text
MATCH (a:Person {id: 1})-[k:KNOWS]->(b:Person) WHERE k.since > 2020 AND b.age > 35 RETURN b.name

logical:  CsrExpand: key=a__id -> [k__src, k__dst], edges=4, take=b[b__id*, b__name, b__age], edge=k[k__since]
physical: CsrExpandExec: key=a__id, edges=4, take=b[b__name], edge=k[], filter=age@0 > 35, edge_filter=since@0 > 2020
```

## Graph rewrites before DataFusion sees the plan

### Re-rooting: start at the most selective node

The logical planner scans a path's first node and expands left to right, so
`MATCH (f:Forum)-[:hasMember]->(p:Person) WHERE p.id = 42` would expand every forum's memberships before filtering.
The join path partly recovers because DataFusion pushes the filter below the join; a CSR expansion can't. So
`pattern_order.rs` rewrites each `MATCH` into an equivalent traversal that starts at the best-scoring node (one with a
key or property filter), walking segments backwards where needed: an outgoing edge walked from its target is incoming.

```mermaid
flowchart LR
    subgraph before ["As written"]
        direction LR
        F1["(f:Forum)<br/>every forum"] -->|hasMember| P1["(p:Person)<br/>WHERE p.id = 42"]
    end
    subgraph after ["Re-rooted"]
        direction LR
        P2["(p:Person {id: 42})<br/>one row"] -->|"hasMember, incoming"| F2["(f:Forum)"]
    end
    before ~~~ after
```

The same edges, endpoints, labels and variables are matched, and the full `WHERE` clause still applies above the
match. Only the order of work changes.

### Alternation and multi-label nodes become branches

`-[:A|B]->` and `(m:Post:Comment)` are rewritten into branches with exactly one type per relationship and at most one
label per node, combined with `UNION ALL`. Only combinations consistent with the configured endpoint labels are kept,
and each branch is an ordinary pattern, so each keeps the CSR and re-rooting.

A variable-length segment with several types, such as `-[:hasType|isSubclassOf*1..20]->`, runs as one typed
[iterative traversal](../csr.md#variable-length-traversal) when every type and label is indexed. Otherwise it is
unrolled into the sequences of types whose endpoint labels chain, and each sequence is branched the same way.

Combinations that the configuration can't pin down are an error rather than a guess: joining an edge type to the
wrong node table silently matches wrong rows whenever two labels share ids.

### List comprehensions keep each row once

`[p IN posts WHERE ...]` is evaluated relationally: the rows are numbered, each row's list becomes one row per element,
the filter (and any pattern predicate, through [`PathExists`](operators.md#pathexists-pattern-predicates-walked-in-place)
or its fallback) runs on those element rows, and the kept elements are counted or collected back per row number.
Two details keep this from repeating each row's data once per element:

- The list is unnested beside its element positions, so an element row holds its own item rather than a copy of the
  whole list.
- A list read from a column, such as `posts` from an earlier `collect(post)`, is not a grouping key on the way back.
  It is rebuilt once per row from its elements, in order (null for a null list, empty for an empty one), so when the
  comprehension doesn't read the list itself, projection pushdown drops it before the unnest.

A list of nodes binds each element to its node through a [`NodeLookup`](operators.md#nodelookup-group-by-id-then-look-up)
when the node table is in memory, rather than a hash join against the whole table.

## Join statistics for graph workloads

Not every hop runs on the CSR, and the joins that remain need good cardinality estimates. DataFusion estimates an
equi-join's output as `L × R / max(NDV_l, NDV_r)`, using the number of distinct values (NDV) of each join key.
Neither `MemTable` nor `LanceTableProvider` reports distinct counts, so DataFusion substitutes the row count. For a
graph join chain, that turns a multi-million-row intermediate into an estimate of a few thousand rows, under the
128k-row `CollectLeft` threshold, and DataFusion then builds that intermediate as one single-threaded hash table.

`KeyStatisticsProvider` wraps each table and reports distinct counts for its node-id and edge-endpoint columns, taken
from the CSR when there is one and otherwise from one scan of the endpoint columns. Every other statistic comes from
the inner table. The counts are cached per dataset version, so a `checkout_latest()` with no changes costs nothing.
