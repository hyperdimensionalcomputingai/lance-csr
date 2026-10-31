# Custom operators

Every graph operator in lance-csr is a DataFusion extension: a logical node that implements
`UserDefinedLogicalNodeCore`, lowered by `CsrExtensionPlanner` to its own `ExecutionPlan`. This page covers each one,
with the plans `engine.explain(query)` prints for it.

The plans below come from a four-person graph: a `Person` table (`id`, `name`, `age`) and a `KNOWS` table (`src`,
`dst`, `since`) with four edges, in memory, with `use_csr=True`. Plans of real graphs differ in their numbers and
partitioning, not in their operators.

## `CsrExpand`: a hop as an array lookup

Without it, `(a:Person)-[:KNOWS]->(b:Person)` is a hash join of `a` against the whole `KNOWS` table and a second hash
join against the whole `Person` table to fetch `b`. With it, each input row looks up `a`'s neighbours in the CSR and
fetches `b`'s rows by position.

Here is the same two-hop query planned both ways:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person {name: 'Dan'}) RETURN a.name
```

=== "use_csr=True"

    ```text
    ProjectionExec: expr=[a__name@0 as a.name]
      FilterExec: b__id@1 != a__id@2 OR c__id@0 != b__id@1, projection=[a__name@3]
        ProjectionExec: expr=[c__id@0 as c__id, b__id@3 as b__id, a__id@6 as a__id, a__name@7 as a__name]
          CsrExpandExec: key=b__id, edges=4, take=a[a__id*, a__name]
            CsrExpandExec: key=c__id, edges=4, take=b[b__id*]
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                ProjectionExec: expr=[id@0 as c__id]
                  FilterExec: name@1 = Dan, projection=[id@0]
                    KeyStatisticsExec: distinct=[id=4]
                      DataSourceExec: partitions=1, partition_sizes=[1]
    ```

=== "use_csr=False"

    ```text
    ProjectionExec: expr=[a__name@0 as a.name]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(knows_2__src@2, a__id@0)], ...
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(b__id@1, knows_2__dst@1)], ...
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(knows_1__src@1, b__id@0)], ...
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c__id@0, knows_1__dst@1)], ...
              ProjectionExec: expr=[id@0 as c__id]
                FilterExec: name@1 = Dan, projection=[id@0]
                  KeyStatisticsExec: distinct=[id=4]
                    DataSourceExec: partitions=1, partition_sizes=[1]
              ProjectionExec: expr=[src@0 as knows_1__src, dst@1 as knows_1__dst]
                KeyStatisticsExec: distinct=[dst=3, src=3]
                  DataSourceExec: partitions=1, partition_sizes=[1]
            ProjectionExec: expr=[id@0 as b__id]
              KeyStatisticsExec: distinct=[id=4]
                DataSourceExec: partitions=1, partition_sizes=[1]
          ProjectionExec: expr=[src@0 as knows_2__src, dst@1 as knows_2__dst]
            KeyStatisticsExec: distinct=[dst=3, src=3]
              DataSourceExec: partitions=1, partition_sizes=[1]
        ProjectionExec: expr=[id@0 as a__id, name@1 as a__name]
          KeyStatisticsExec: distinct=[id=4]
            DataSourceExec: partitions=1, partition_sizes=[1]
    ```

Four hash joins over five scans become two `CsrExpandExec`s over one filtered scan. The `FilterExec` above them
keeps the two hops from reusing one edge. Both plans start from `c` (Dan), not `a`: that is
[re-rooting](optimizer.md#re-rooting-start-at-the-most-selective-node).

### What one expansion can do

| Capability | In `explain()` | What it replaces |
| --- | --- | --- |
| Neighbour lookup in one direction, or both for `-[:R]-` | `key=a__id, edges=N`, `undirected` | Join against the relationship table |
| [Fused target lookup](../csr.md#the-fused-target-lookup): neighbour id to row position, then `take` of only the columns the query reads | `take=b[b__name]` | Join against the target node table |
| [Relationship properties](../csr.md#relationship-variables) fetched by edge row, since each CSR entry records the row its edge came from | `edge=k[k__since]` | Keeping the relationship table in the join |
| Target filter: reads the filter's columns first, and fetches the rest only for neighbours that pass | `filter=age > 35` | `FilterExec` after a full fetch |
| Relationship filter: drops failing edges before anything else is fetched | `edge_filter=since > 2020` | `FilterExec` after the join |
| Closing edge: the target is already bound (a cycle), so only neighbours equal to it are kept | `match=b__id` | Expanding everything, then filtering |
| [Iterative variable-length traversal](../csr.md#variable-length-traversal): one cursor per hop over typed (label, key) frontiers, with bounded output batches | `iterative hops=1..3` | One join branch per path length, combined with `UNION ALL` |

A hop that uses several of these at once:

```text
MATCH (a:Person {id: 1})-[k:KNOWS]->(b:Person) WHERE k.since > 2020 AND b.age > 35 RETURN b.name

ProjectionExec: expr=[b__name@3 as b.name]
  CsrExpandExec: key=a__id, edges=4, take=b[b__name], edge=k[], filter=age@0 > 35, edge_filter=since@0 > 2020
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      ProjectionExec: expr=[id@0 as a__id]
        FilterExec: id@0 = 1
          KeyStatisticsExec: distinct=[id=4]
            DataSourceExec: partitions=1, partition_sizes=[1]
```

Both `WHERE` conditions moved into the expansion, and the columns only they read (`age`, `since`) are gone from its
output. [Optimizer rules](optimizer.md#logical-optimizer-rules) explains how.

A variable-length path is one operator instead of a branch per length:

```text
MATCH (a:Person {id: 1})-[:KNOWS*1..3]->(b:Person) RETURN b.name

ProjectionExec: expr=[b__name@3 as b.name]
  CsrExpandExec: key=a__id, edges=4, take=b[b__name], iterative hops=1..3
    ...
```

### Performance details

- **Adaptive reads of lazy Lance tables.** A batch of scattered rows from a large Lance table touches most of a
  column's pages. Once a relationship filter has seen more than 1/256 of the relationship's edges, the operator reads
  the filter's columns once, in order, builds a bitset per fragment, and checks edges against it from then on.
  Target-column fetches switch to one sequential read the same way. In-memory Arrow tables always gather directly.
- **Bounded memory per partition.** Each partition holds one input batch and at most one output chunk of neighbour
  positions, sized by DataFusion's batch size, before it fetches properties. It yields partway through a high-degree
  node's neighbour list.
- **Fast hashing on the hot path.** Integer-keyed maps on the per-edge lookup path use `ahash`, as Arrow and
  DataFusion do; with the standard library's SipHash, hashing dominated expansion time in profiles.
- **A predicate pushdown boundary.** The node lists its own output columns in `prevent_predicate_push_down_columns`,
  so DataFusion pushes filters on input columns below it and keeps filters on the columns it creates above it.

A hop is eligible when its endpoint columns are integers, it has exactly one relationship type (alternation is split
into branches first) and it has no inline relationship property map. [Which hops use it](../csr.md#which-hops-use-it)
has the full list. Any other hop stays a join.

## `NodeLookup`: group by id, then look up

`WITH b, count(*) AS n` groups by the node `b`, which relationally means grouping by every one of `b`'s columns. Rows
before a grouping can outnumber nodes many times over, because they come out of an expansion, so carrying `b`'s
properties through the aggregate for each of them is expensive. When `b`'s label has a node index (unique ids), the
planner groups by `b`'s id alone and adds a `NodeLookup` above the aggregate that fetches `b`'s other properties once
per group:

```text
MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b, count(*) AS n RETURN b.name, n

ProjectionExec: expr=[b__name@2 as b.name, __agg_0@1 as n]
  NodeLookupExec: key=b__id, take=b[b__name]
    AggregateExec: mode=FinalPartitioned, gby=[b__id@0 as b__id], aggr=[count(1) as __agg_0]
      RepartitionExec: partitioning=Hash([b__id@0], 10), input_partitions=10
        AggregateExec: mode=Partial, gby=[b__id@0 as b__id], aggr=[count(1) as __agg_0]
          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
            ProjectionExec: expr=[b__id@3 as b__id]
              CsrExpandExec: key=a__id, edges=4, take=b[b__id*, b__name]
                ...
```

Unlike a join back to the node table, its cost follows the number of groups, not the size of the table. It applies to
in-memory node tables only; see [grouping by a node](../csr.md#grouping-by-a-node) for why lazily read Lance tables
keep the old plan.

## `ShortestPath`: breadth-first search over the adjacency

`shortestPath` and `allShortestPaths` run as a breadth-first search over the CSR adjacency. Without a CSR, the node
gets a second input, a scan of the relationship's two endpoint columns, and builds an adjacency from it once per
query, shared by all partitions.

- A source with one target searches from both ends using the outgoing and incoming adjacency. It expands the smaller
  frontier and finishes each layer before joining paths, so it keeps every shortest alternative.
- Several targets for the same source share one forward search.
- Predecessors retain every shortest path, including parallel edges.
- The operator appends `<path>__length` and `<path>__nodes` to each input row, once per path. Rows with no path are
  dropped for MATCH, and kept with nulls through a left join for OPTIONAL MATCH.

```text
MATCH (a:Person {id: 1}), (b:Person {id: 4}) MATCH p = shortestPath((a)-[:KNOWS*]-(b)) RETURN length(p)

ProjectionExec: expr=[p__length@6 as length(p)]
  ShortestPathExec: a__id -> b__id, Undirected, hops 1..4294967295, one
    CrossJoinExec
      ...
```

## `PathMatch`: named paths with real edge identity

CSR neighbours don't carry edge identity or properties, but `MATCH p = ...` with `relationships(p)` needs both, and
parallel edges must stay distinct even through `collect()` and `UNWIND`. `PathMatchExec` builds one immutable,
query-local edge inventory in which an edge's identity is its actual scanned occurrence. It enumerates trails over the
inventory, resuming between output batches, and reuses the bidirectional breadth-first search on a bipartite
node/edge graph for shortest paths. Path values keep properties in a marked child, so user property names can't
collide with identity metadata. See [path values](../cypher.md#path-values) for what users see.

## `Apply`: correlated CALL subqueries

A read-only `CALL { ... }` body runs once per incoming row: its `LIMIT` and aggregates apply to each row, not to the
whole input. DataFusion has no operator for that, so the body is kept as an unoptimized child template. For each
outer row, `ApplyExec` binds the explicit imports to concrete values, plans the child, and streams its batches. It
holds one outer batch and one child stream per partition, and dropping the result stream drops both.

## `KeyStatistics`: statistics on the scan

`KeyStatisticsExec` is a transparent wrapper over a table scan that reports distinct counts for join keys (see
[join statistics](optimizer.md#join-statistics-for-graph-workloads)). Filters, projections, limits and sort pushdown
pass through to the inner scan unchanged, as with DataFusion's own `CooperativeExec`. It shows in plans as
`KeyStatisticsExec: distinct=[id=4]`.
