# DataFusion internals

This section is for contributors and for readers who want to know how lance-csr extends
[Apache DataFusion](https://datafusion.apache.org/) to run graph queries. You don't need any of it to use the engine:
[How it works](../execution.md) gives the short version.

lance-csr's goal is fast multi-hop traversal over data that already lives in [Lance](https://lancedb.github.io/lance/),
with no import into a graph database and no second copy of the data. DataFusion is a strong relational engine, but a
graph hop written relationally is two hash joins, and a five-hop query is ten of them. lance-csr keeps DataFusion for
everything it already does well (filters, projections, aggregates, sorts, joins, vectorized multi-threaded execution)
and adds graph-aware pieces where relational plans fall short. The [compressed sparse row (CSR) index](../csr.md) is
the data structure those pieces read.

| Page | What it covers |
| --- | --- |
| [Custom operators](operators.md) | `CsrExpand`, `NodeLookup`, `ShortestPath`, `PathMatch`, `Apply` and `KeyStatistics`, with their plans |
| [Optimizer rules and rewrites](optimizer.md) | The two logical optimizer rules, graph rewrites before DataFusion sees the plan, and join statistics |
| [Indexes and refresh](indexes.md) | How the CSR adjacency and node indexes are laid out, and how a refresh diffs Lance versions |
| [Execution plumbing](execution-plumbing.md) | Partitioning, the plan template cache, deadlines and Cypher functions |

## What lance-csr added to lance-graph

lance-csr is a fork of [lance-graph](https://github.com/lance-format/lance-graph), and the two parts of the engine have
different origins.

From [lance-graph](https://github.com/lance-format/lance-graph)
:   The Cypher parser and semantic analysis, the graph logical plan's core operators (`ScanByLabel`, `Expand`,
    `VariableLengthExpand`, `Filter`, `Unwind`, `Project`, `Distinct`, `Sort`, `Offset`, `Limit`), and the planner
    that turns them into DataFusion scans and joins, with variable-length paths unrolled into one branch per length.
    Vector search functions, releasing the GIL from Python, and a compact CSR neighbour store that lance-graph's query
    planner didn't use. lance-csr has since extended and fixed much of this code.

Added in lance-csr
:   Everything this section describes as a DataFusion extension: the extension nodes and their operators, the
    extension planner, both optimizer rules, the deadline rule and the join statistics. Also the graph rewrites
    (re-rooting, alternation and multi-label branching), the `OptionalMatch`, `ShortestPath`, `PathMatch` and `Apply`
    operators, the CSR adjacency and node indexes with incremental refresh, engine snapshots, the plan template cache
    and the Cypher-specific functions.

[lance-graph](https://github.com/lance-format/lance-graph) plans every hop as joins, and lance-csr keeps that planning
as the fallback for any hop the CSR can't take. Each page in this section says what came from lance-graph where it
matters.

## Design principles

- **The data stays in Lance.** Tables are read through Lance's own `LanceTableProvider`. The graph indexes store
  Lance row addresses and row ids, so a hop fetches exactly the rows it needs with `take`, and an index never holds a
  copy of node or relationship properties.
- **Every graph operator is a DataFusion extension node.** It sits inside an ordinary DataFusion plan, so everything
  around it is still planned, optimized and executed by DataFusion.
- **Results never depend on the fast path.** Any hop the CSR can't express is planned as a hash join in the same plan.
  The test suite runs each supported shape with the CSR on and off and compares the answers.
- **Bounded, streaming operators.** The extension operators yield bounded output batches and resume where they left
  off, even inside a single high-degree node's neighbour list, and they check for cancellation as they go.
- **Immutable snapshots.** An engine holds a snapshot of pinned dataset versions plus the indexes built for them.
  `refresh()` builds a replacement next to it, reusing whatever didn't change, and swaps it in atomically, so running
  queries never see a half-updated index.

## Where the extensions plug in

```mermaid
flowchart TD
    T["Cypher text"] --> P["Parse<br/><code>parser.rs</code>"]
    P --> S["Semantic analysis<br/><code>semantic.rs</code><br/>scopes, labels, $parameters"]
    S --> R["Graph rewrites<br/><code>pattern_order.rs</code><br/><code>pattern_alternatives.rs</code>"]
    R --> G["Graph logical plan<br/><code>logical_plan.rs</code><br/>ScanByLabel, Expand, VariableLengthExpand,<br/>ShortestPath, PathMatch, OptionalMatch, Apply"]
    G --> D["DataFusion logical plan<br/><code>datafusion_planner/</code><br/>scans + joins + extension nodes"]
    D --> O["DataFusion optimizer<br/>+ PushCsrTargetFilters<br/>+ PruneCsrTargetColumns"]
    O --> X["Physical planning<br/>CsrQueryPlanner → CsrExtensionPlanner"]
    X --> E["Execution<br/>CsrExpandExec, NodeLookupExec, ShortestPathExec,<br/>PathMatchExec, ApplyExec, KeyStatisticsExec"]
    E --> B["Arrow record batches"]
```

Parsing, semantic analysis and the core of both logical plans came from
[lance-graph](https://github.com/lance-format/lance-graph). The graph rewrites, the extension nodes, the optimizer
rules and the extension planner were added in lance-csr. `engine.explain(query)` prints the graph logical plan, the
DataFusion logical plan and the physical plan, and every custom operator shows up in it by name.

## Every extension point

| DataFusion trait | lance-csr implementation | Role |
| --- | --- | --- |
| `UserDefinedLogicalNodeCore` | `CsrExpandNode`, `NodeLookupNode`, `ShortestPathNode`, `PathMatchNode`, `ApplyNode` | Graph operators as logical plan nodes |
| `ExecutionPlan` | `CsrExpandExec`, `NodeLookupExec`, `ShortestPathExec`, `PathMatchExec`, `ApplyExec`, `KeyStatisticsExec` | Their physical operators |
| `ExtensionPlanner` | `CsrExtensionPlanner` | Lowers each graph node to its `ExecutionPlan` |
| `QueryPlanner` | `CsrQueryPlanner` | Installs the extension planner on the session |
| `OptimizerRule` | `PushCsrTargetFilters`, `PruneCsrTargetColumns` | Filter pushdown into, and column pruning of, extension nodes |
| `PhysicalOptimizerRule` | `DeadlineJoins` (`query_deadline_joins`) | Cooperative deadlines in nested-loop and cross joins |
| `TableProvider` | `KeyStatisticsProvider` | Reports distinct counts of join keys so DataFusion sizes graph joins correctly |
| `ScalarUDFImpl` | `cypher_list`, `cypher_field`, Cypher math and scalar functions; vector distance and similarity functions (from [lance-graph](https://github.com/lance-format/lance-graph)) | Cypher semantics that DataFusion's built-ins don't match |

Apart from the vector functions, every implementation in this table was added in lance-csr;
[lance-graph](https://github.com/lance-format/lance-graph) used no DataFusion extension points. `CypherEngine` builds
one DataFusion session per snapshot (in `engine.rs`). It installs `CsrQueryPlanner` and adds `PushCsrTargetFilters`
before `PruneCsrTargetColumns`, so that pruning can drop the columns a moved filter reads. Rust callers that bring
their own `SessionContext` must install `CsrQueryPlanner` themselves; contexts created by `CypherQuery` and
`CypherEngine` already have it.

## Source map

All paths are under
[`crates/lance-csr/src/`](https://github.com/hyperdimensionalcomputingai/lance-csr/tree/main/crates/lance-csr/src).

| File | What it holds | Origin |
| --- | --- | --- |
| `csr_expand.rs` | `Adjacency`, `NodeRows`, `EdgeRows`, `CsrExpandNode` and `CsrExpandExec`, `CsrExtensionPlanner`, `CsrQueryPlanner`, both optimizer rules, fragment diffing for refresh | lance-csr |
| `csr_expand/node_lookup.rs` | `NodeLookupNode` and `NodeLookupExec` | lance-csr |
| `csr_index.rs` | The compact CSR neighbour store | [lance-graph](https://github.com/lance-format/lance-graph); edge rows and the delta added in lance-csr, unused traversal helpers removed |
| `csr_traversal.rs` | Resumable, typed variable-length walks | lance-csr |
| `shortest_path.rs` | `ShortestPathNode` and `ShortestPathExec`, bidirectional breadth-first search | lance-csr |
| `path_match.rs`, `path_inventory.rs`, `path_value.rs` | Named paths, the edge inventory, Arrow path values | lance-csr |
| `apply.rs` | `ApplyNode` and `ApplyExec` for CALL subqueries | lance-csr |
| `key_statistics.rs` | `KeyStatisticsProvider` and `KeyStatisticsExec` | lance-csr |
| `pattern_order.rs`, `pattern_alternatives.rs` | Re-rooting, alternation and multi-label branching | lance-csr |
| `logical_plan.rs` | Graph logical operators | [lance-graph](https://github.com/lance-format/lance-graph), extended in lance-csr |
| `datafusion_planner/` | Graph plan to DataFusion plan; `builder/expand_ops.rs` chooses CSR or join for each hop | [lance-graph](https://github.com/lance-format/lance-graph), extended in lance-csr |
| `engine.rs` | `CypherEngine`: snapshots, session setup, refresh, plan template cache | lance-csr |
| `query/deadline.rs` | `DeadlineJoins` and `with_execution_deadline` | lance-csr |
