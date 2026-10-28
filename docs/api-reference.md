# Python API reference

Everything below is importable from `lance_csr`. The classes are thin Python wrappers around the Rust engine,
and this page is generated from their docstrings and type hints, so it matches the installed package. The
guides link here for exact signatures and link back for examples.

| Class | Purpose |
| --- | --- |
| [`GraphConfig`][lance_csr.GraphConfig], [`GraphConfigBuilder`][lance_csr.GraphConfigBuilder] | Map tables to node labels and relationship types |
| [`CypherEngine`][lance_csr.CypherEngine] | Run many Cypher queries over registered datasets |
| [`CypherQuery`][lance_csr.CypherQuery] | Run one Cypher query |
| [`SqlEngine`][lance_csr.SqlEngine], [`SqlQuery`][lance_csr.SqlQuery] | Run SQL through DataFusion |
| [`VectorSearch`][lance_csr.VectorSearch], [`DistanceMetric`][lance_csr.DistanceMetric] | Vector similarity search and reranking |
| [`DirNamespace`][lance_csr.DirNamespace] | Resolve table names to Lance datasets in a directory |
| [`ExecutionStrategy`][lance_csr.ExecutionStrategy], [`SqlDialect`][lance_csr.SqlDialect] | Enums |

`lance_csr.__version__` is the package version.

This page covers the Python API. Rust users should read [Using from Rust](rust.md) and the
[Rust API reference on docs.rs](https://docs.rs/lance-csr).

## Type checking

Every public class is type-annotated, and the package ships a `py.typed` marker. Editors and type checkers
can follow builder methods, parameters, query results and statistics. The annotations describe the API;
they don't change runtime validation or exceptions.

PyArrow distributes its type information separately. For strict checks of Arrow inputs and results, add
`pyarrow-stubs` to your development dependencies:

```bash
uv add --dev pyright pyarrow-stubs
uv run pyright your_script.py
```

```python
from typing import assert_type

import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

config = GraphConfig.builder().node_table("Person", "id").build()
engine = CypherEngine(config, {"Person": pa.table({"id": [1, 2]})})
result = engine.execute("MATCH (p:Person) RETURN p.id ORDER BY p.id")
assert_type(result, pa.Table)
print(result.to_pylist())  # [{'p.id': 1}, {'p.id': 2}]
```

## Datasets

Methods that take `datasets` take a `dict[str, object]` mapping each node label or relationship type (or SQL
table name) to one of:

- `lance.LanceDataset`: read lazily and version-pinned by `CypherEngine`; read into memory by the other
  classes;
- `pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`, or any object exporting the Arrow C
  stream interface (`__arrow_c_stream__`).

Keys match labels case-insensitively.

## Exceptions

| Exception | When |
| --- | --- |
| `ValueError` | Invalid query, incompatible known operand types (with expression and type details), unknown label, missing dataset or column, unsupported parameter type, empty `datasets` |
| `NotImplementedError` | A Cypher feature that parses but isn't supported |
| `RuntimeError` | Execution or I/O failures |
| `TypeError` | A dataset value of an unsupported type, such as a LanceDB `Table`, or an argument of the wrong class, such as a string where a `GraphConfig` is expected |

## Timeouts

`CypherEngine.execute`, `CypherEngine.execute_batches` and `CypherQuery.execute` accept keyword-only
`timeout` in seconds (`None` means no deadline). Zero expires immediately; negative, nonfinite or
unrepresentable durations raise `ValueError`. One deadline includes planning, every batch and pauses
between reads. Before returning a reader, expiry raises `TimeoutError`. Reading an expired native Arrow
reader raises `pyarrow.ArrowInvalid` containing `Query timeout exceeded`. Earlier batches remain valid.
Expiry or reader close releases the stream even while the caller is idle; the engine remains usable.

Cancellation is cooperative. Traversal loops yield at bounded work intervals, and timed cross joins
yield between output batches. Timed nested-loop joins check the deadline while evaluating candidates,
including batches that produce no matches. Synchronous parsing,
Python data providers and foreign blocking calls cannot be forcibly preempted. Deadlines are checked
between planning stages and before accepting completed results; this is not a hard real-time limit.

## Path values

Cypher named paths can be returned as Arrow structs containing `length`, `nodes`, `node_records` and
`relationships`. They retain their values through `collect()` and `UNWIND`; `nodes(path)` returns integer
keys, and `relationships(path)` returns typed relationship records. `startNode(r)` and `endNode(r)` return
the stored endpoint node records. Their occurrence identities are query-local. See
[path values and their memory cost](cypher.md#path-values).

::: lance_csr.GraphConfig

::: lance_csr.GraphConfigBuilder

::: lance_csr.CypherEngine

::: lance_csr.CypherQuery

::: lance_csr.SqlEngine

::: lance_csr.SqlQuery

::: lance_csr.VectorSearch

::: lance_csr.DistanceMetric
    options:
      members: false

::: lance_csr.DirNamespace

::: lance_csr.ExecutionStrategy
    options:
      members: false

::: lance_csr.SqlDialect
    options:
      members: false
