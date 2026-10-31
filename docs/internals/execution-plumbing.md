# Execution plumbing

This section is about the smaller pieces that make the [custom operators](operators.md) run well inside DataFusion.
All of them were added in lance-csr, apart from the vector functions and the GIL release, which came from
[lance-graph](https://github.com/lance-format/lance-graph), the upstream project we forked from.

## Partitioning in-memory tables

A `MemTable` holding one large batch is one partition with one batch, and `RoundRobinBatch` repartitioning moves
whole batches. An operator that doesn't hash-repartition, such as `CsrExpandExec`, would then run on one thread over
all of the input. So the engine splits each in-memory table into zero-copy slices of 8,192 rows, spread over one
partition per core. Lance datasets are scanned by Lance's own parallel reader.

## The plan template cache

Each [snapshot](../execution.md#snapshots-and-concurrency) keeps up to 128 unoptimized DataFusion plan templates,
least recently used first out, keyed by the query text and the types of its parameters. Running a query again with
new scalar `$parameters` skips parsing and graph planning, and only binds the new values. Some operators need literal
values while planning; when a template can't be built, the query is planned in full as before. A refresh starts the
new snapshot with an empty cache. CALL queries skip the cache and are planned in full.

## Cooperative deadlines

DataFusion has no query deadline, and a nested-loop join over a Cartesian product can run for a long time without
producing a row. `with_execution_deadline` (in `query/deadline.rs`) installs the `DeadlineJoins` physical optimizer
rule for queries planned inside it. The rule adds a volatile deadline check to the filter of each nested-loop join,
so it fails with `Query timeout exceeded` while evaluating candidates even when nothing matches, and makes cross
joins yield to the runtime between batches so a timer can stop them. The Python `timeout` argument uses one deadline
for planning, execution and every streamed batch; see [timeouts](../api-reference.md#timeouts).

## Cypher functions

DataFusion's built-in functions don't always match Cypher's semantics, so the planner registers its own:

- `cypher_list` builds list literals: Cypher lists may mix types and Arrow lists hold one, so elements of different
  kinds become strings instead of failing to cast;
- `cypher_field` reads a struct field by name, ignoring case;
- Cypher math and scalar functions where DataFusion's equivalents differ;
- ordered folds and three-valued list predicates, built on DataFusion lambdas;
- vector distance and similarity functions for [vector search](../vector-search.md) inside Cypher, which came from
  [lance-graph](https://github.com/lance-format/lance-graph).

## A workaround for COUNT DISTINCT

DataFusion 54 treats filtered distinct-count estimates as exact, so its `aggregate_statistics` physical rule can
answer a `count(DISTINCT ...)` from statistics that don't preserve empty-input semantics. When a plan contains a
distinct count, the engine removes that one rule for that query, leaving the caller's session unchanged. The
workaround goes once DataFusion's statistics handle empty inputs.

## Running from Python

The GIL is released while a query runs, as it was in [lance-graph](https://github.com/lance-format/lance-graph), so
several Python threads can query one engine at the same time, and `Ctrl-C` interrupts a running `execute()`.