"""Running Cypher queries: one at a time, or many through a cached engine."""

from __future__ import annotations

import warnings
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Literal, Self, final

from ._internal import graph as _graph
from ._types import inner
from .config import GraphConfig
from .dialect import SqlDialect
from .export import GraphExport
from .namespace import DirNamespace
from .vector import VectorSearch

if TYPE_CHECKING:
    from datetime import timedelta

    from pyarrow import RecordBatchReader, Table

    from ._types import (
        CsrStats,
        Dataset,
        DatasetOptimizeStats,
        NodeIndexStats,
        Parameter,
        Scalar,
    )


class ExecutionStrategy(IntEnum):
    """How [`CypherQuery.execute`][lance_csr.CypherQuery.execute] runs a query.

    Attributes
    ----------
    DataFusion
        Plan and run the query with DataFusion: the default and only implemented one.
    LanceNative
        Reserved; raises `NotImplementedError`.
    """

    DataFusion = 0
    LanceNative = 1


def _option(value: object, cls: type, name: str) -> Any:
    return None if value is None else inner(value, cls, name)


@final
class CypherEngine:
    """Run many Cypher queries over the same datasets.

    The engine registers `datasets` once and reuses the catalog, the DataFusion session
    and (with `use_csr=True`) the CSR index across queries. It is safe to share between
    threads.

    Lance datasets are read lazily, pinned to the version passed in: later writes, by
    this or another process, aren't visible until
    [`checkout_latest`][lance_csr.CypherEngine.checkout_latest]. Each dataset is pinned
    separately, so versions across tables are not a transactional snapshot. PyArrow
    inputs are held in memory and never change; create a new engine to replace them.

    [`execute`][lance_csr.CypherEngine.execute] and
    [`execute_batches`][lance_csr.CypherEngine.execute_batches] reuse up to 128
    unoptimized query templates per snapshot, keyed by the exact query text and the
    names and types of scalar parameters. Values are bound before optimization on every
    call. Container parameters, rich path values, `CALL` subqueries, parameterized
    `SKIP`/`LIMIT` and unsupported template shapes use ordinary planning.
    [`explain`][lance_csr.CypherEngine.explain] plans directly unless `analyze=True`,
    which plans as `execute` does, through the cache. A successful
    `checkout_latest` clears the cache. See [Repeated
    queries](running-queries.md#repeated-queries).

    Parameters
    ----------
    config : GraphConfig
        The graph mapping.
    datasets : dict[str, Dataset]
        The tables, keyed by node label or relationship type (see
        [Datasets](#datasets)). Lance datasets are pinned to the version passed in and
        not read here.
    use_csr : bool
        Build the experimental [CSR index](csr.md) for relationship types with integer
        endpoints, and node indexes for labels with unique integer keys. With either
        setting, each hop of a variable-length segment uses a different stored
        relationship. Parallel relationships between the same nodes count separately.
    target_partitions : int or None
        Keyword-only. A target from 1 to 1024 for DataFusion query partitioning; `None`
        keeps DataFusion's default. `checkout_latest` preserves it. It is not a thread
        or memory limit. See [Query parallelism](running-queries.md#query-parallelism).
    persist_csr : bool or "load"
        Keyword-only, experimental, needs `use_csr=True` unless `False`. With `True`,
        store each relationship dataset's CSR inside the dataset's own directory, at
        `_lance_csr/<version>.arrow`, and load it instead of building when an engine
        starts. Changes since the stored version apply as a delta under the same 25%
        threshold as `checkout_latest`; past it, the CSR is rebuilt and stored again.
        Lance's `cleanup_old_versions` leaves the folder alone. PyArrow inputs aren't
        stored. A dataset that can't be written (read-only storage or credentials) is
        built in memory, and the engine emits a `RuntimeWarning` and reports the error
        as `store_error` in [`csr_stats`][lance_csr.CypherEngine.csr_stats]. With
        `"load"`, load stored files the same way but never write to the datasets: a
        CSR that can't be loaded is built in memory, as with `False`. See [Storing the
        CSR](csr.md#storing-the-csr).

    Raises
    ------
    ValueError
        If `datasets` is empty, a configured key or endpoint column is missing, a Lance
        dataset was opened through a namespace client and can't be reopened by URI,
        `target_partitions` is outside 1 to 1024, `persist_csr` isn't `True`, `False`
        or `"load"`, or it is set without `use_csr`.
    TypeError
        If `config` isn't a `GraphConfig`, a dataset has an unsupported type, or
        `target_partitions` is a boolean or another non-integer other than `None`.
    RuntimeError
        If a dataset can't be opened.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lance_csr import CypherEngine, GraphConfig
    >>> config = (
    ...     GraphConfig.builder()
    ...     .node_table("Person", "id")
    ...     .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
    ...     .build()
    ... )
    >>> engine = CypherEngine(config, {
    ...     "Person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}),
    ...     "KNOWS": pa.table({"src_id": [1], "dst_id": [2]}),
    ... })
    >>> engine.execute(
    ...     "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
    ... ).to_pylist()
    [{'a.name': 'Alice', 'b.name': 'Bob'}]
    """

    __slots__ = ("_inner",)
    _inner: _graph.CypherEngine

    def __init__[DatasetT: Dataset](
        self,
        config: GraphConfig,
        datasets: dict[str, DatasetT],
        use_csr: bool = False,
        *,
        target_partitions: int | None = None,
        persist_csr: bool | Literal["load"] = False,
    ) -> None:
        self._inner = _graph.CypherEngine(
            inner(config, GraphConfig, "config"),
            datasets,
            use_csr,
            target_partitions=target_partitions,
            persist_csr=persist_csr,
        )
        self._warn_store_errors({})

    def _warn_store_errors(self, before: dict[str, CsrStats]) -> None:
        # The engine only logs a failed CSR write; Python callers see it here once.
        for rel_type, stats in self._inner.csr_stats().items():
            error = stats["store_error"]
            if error and error != before.get(rel_type, {}).get("store_error"):
                warnings.warn(
                    f"Could not store the CSR of {rel_type}: {error}",
                    RuntimeWarning,
                    stacklevel=3,
                )

    def __repr__(self) -> str:
        return repr(self._inner)

    def execute[ParameterT: Parameter](
        self,
        query: str,
        params: dict[str, ParameterT] | None = None,
        *,
        timeout: float | None = None,
    ) -> Table:
        """Run a Cypher query and return the whole result.

        The result is held in memory; use
        [`execute_batches`][lance_csr.CypherEngine.execute_batches] for large outputs.
        Direct counts from `size()` of an unmapped list comprehension have a
        non-nullable Arrow field; see [Cypher functions](cypher.md#functions).

        Parameters
        ----------
        query : str
            The Cypher query.
        params : dict[str, Parameter] or None
            Values for `$name` parameters: `int`, `float`, `str`, `bool`, `None`,
            lists, string-keyed nested maps, `datetime.datetime` and `datetime.date`
            (compared as timestamps and dates). Integers must fit in a signed 64-bit
            value.
        timeout : float or None
            Keyword-only. Seconds for the whole query; `None` means no deadline. See
            [Timeouts](#timeouts).

        Returns
        -------
        pyarrow.Table
            The query result.

        Raises
        ------
        ValueError
            If the query is invalid, refers to an unknown label, compares or combines
            incompatible types, or a parameter is unsupported, nested beyond 128 levels
            or cyclic.
        NotImplementedError
            If the query uses a Cypher feature that parses but isn't supported.
        TimeoutError
            If the query runs past `timeout`.
        RuntimeError
            If execution fails.
        """
        return self._inner.execute(query, params, timeout=timeout)

    def execute_batches[ParameterT: Parameter](
        self,
        query: str,
        params: dict[str, ParameterT] | None = None,
        *,
        timeout: float | None = None,
    ) -> RecordBatchReader:
        """Run a Cypher query and stream the result.

        Batches are computed as the reader is consumed. Scans, filters and projections
        stream with bounded memory; joins, sorts and aggregations may still buffer. The
        reader keeps its snapshot alive, so a concurrent
        [`checkout_latest`][lance_csr.CypherEngine.checkout_latest] doesn't affect it.
        Planning errors raise immediately; execution errors raise while reading.

        Parameters
        ----------
        query : str
            The Cypher query.
        params : dict[str, Parameter] or None
            Values for `$name` parameters, as for
            [`execute`][lance_csr.CypherEngine.execute].
        timeout : float or None
            Keyword-only. Seconds for the whole query, including pauses between reads;
            `None` means no deadline. See [Timeouts](#timeouts).

        Returns
        -------
        pyarrow.RecordBatchReader
            The result, one batch at a time.

        Raises
        ------
        ValueError
            If the query is invalid.
        TimeoutError
            If the deadline passes before the reader is returned. Reading after it
            passes raises `pyarrow.ArrowInvalid` containing `Query timeout exceeded`.
        RuntimeError
            If planning fails.
        """
        return self._inner.execute_batches(query, params, timeout=timeout)

    def explain[ParameterT: Parameter](
        self,
        query: str,
        params: dict[str, ParameterT] | None = None,
        *,
        analyze: bool = False,
    ) -> str:
        """The plans for a query, without running it unless `analyze` is set.

        Parameters
        ----------
        query : str
            The Cypher query.
        params : dict[str, Parameter] or None
            Values for `$name` parameters, as for
            [`execute`][lance_csr.CypherEngine.execute].
        analyze : bool
            Run the query, like SQL's `EXPLAIN ANALYZE`, and show each physical
            operator's metrics (output rows, elapsed compute time and more) in place
            of the plain physical plan. The query is planned as
            [`execute`][lance_csr.CypherEngine.execute] plans it and runs to
            completion; its rows are discarded.

        Returns
        -------
        str
            The graph logical plan, the DataFusion logical plan and the physical plan,
            including pushed-down scans. With `analyze`, the physical plan's row is
            labelled `physical_plan_with_metrics`.

        Raises
        ------
        ValueError
            If the query is invalid.
        RuntimeError
            If `analyze` is set and the query fails while running.
        """
        return self._inner.explain(query, params, analyze=analyze)

    def checkout_latest(self) -> None:
        """Move every Lance dataset to its latest version.

        The engine's counterpart of Lance's `LanceDataset.checkout_latest()`, for every
        dataset at once. Updates the CSR, node indexes and statistics to match. Datasets
        whose version and commit time are unchanged keep their open handle. The
        replacement is validated in full before it's swapped in: on error nothing
        changes and the previous snapshot stays usable. Queries already running finish
        on the snapshot they started with. PyArrow inputs are unchanged. Concurrent
        calls run one at a time, and `target_partitions` is preserved. See [Lance
        datasets](lance.md#checkout-latest).
        """
        before = self._inner.csr_stats()
        self._inner.checkout_latest()
        self._warn_store_errors(before)

    def optimize_csr(self) -> None:
        """Rebuild each CSR that carries a delta (experimental; needs `use_csr=True`).

        The CSR's counterpart of Lance's `optimize_indices()`.
        [`checkout_latest`][lance_csr.CypherEngine.checkout_latest] applies Lance writes
        as a delta over the base CSR, and rebuilds on its own only once pending changes
        pass 25% of the base. This rebuilds every CSR that carries a delta now, at the
        version the engine is pinned to, so lookups stop carrying it. It scans every
        edge again, so call it after a bulk load or at a quiet time, not after every
        `checkout_latest`. It doesn't move to newer versions: call `checkout_latest`
        first for that. CSRs without a delta, and PyArrow inputs, are left as they are,
        and query results never change. Afterwards
        [`csr_stats`][lance_csr.CypherEngine.csr_stats] reports `update` `"rebuild"`,
        `unindexed_edges` 0 and `base_version` equal to `version`. As for
        `checkout_latest`, the replacement is validated in full before it's swapped in,
        and the two run one at a time. See [Rebuilding the
        CSR](csr.md#rebuilding-the-csr).
        """
        before = self._inner.csr_stats()
        self._inner.optimize_csr()
        self._warn_store_errors(before)

    def delete_stored_csr(self) -> int:
        """Delete the CSR files stored with `persist_csr=True` (experimental).

        Removes the `_lance_csr` folder of every Lance relationship dataset this
        engine reads, whether the engine itself was created with `persist_csr=True`
        or `False`. The engine keeps the CSR it already holds in memory, so queries
        aren't affected. To go back to in-memory builds only, create later engines
        without `persist_csr`: an engine with it stores a new file at its next full
        build. Runs one at a time with `checkout_latest` and `optimize_csr`. See
        [Storing the CSR](csr.md#storing-the-csr).

        Returns
        -------
        int
            The number of files deleted; 0 when nothing was stored.

        Raises
        ------
        ValueError
            If the engine was created with `persist_csr="load"`, which never writes
            to its datasets.
        """
        return self._inner.delete_stored_csr()

    def node_index_stats(self) -> dict[str, NodeIndexStats]:
        """Statistics for each node index (experimental; needs `use_csr=True`).

        A label is missing when its keys aren't unique integers; relationships that end
        at it then use a join.

        Returns
        -------
        dict[str, NodeIndexStats]
            Per node label: `rows`, `version` and `build_ms`, as in
            [`csr_stats`][lance_csr.CypherEngine.csr_stats].
        """
        return self._inner.node_index_stats()

    def csr_stats(self) -> dict[str, CsrStats]:
        """Statistics for each CSR adjacency index (experimental; needs `use_csr=True`).

        An identical result after
        [`checkout_latest`][lance_csr.CypherEngine.checkout_latest] means the snapshot
        itself didn't change. See [Inspecting the CSR](csr.md#inspecting-the-csr).

        Returns
        -------
        dict[str, CsrStats]
            Per relationship type with a CSR, empty without `use_csr=True`:

            - `edges`: live indexed edges (null endpoints are skipped).
            - `delta_edges`, `tombstones`: edges appended and removed since the last
              full build, held outside the base CSR.
            - `unindexed_edges`: live edges outside the base CSR, appended since its
              last full build and not since removed.
            - `update`: how the last build, `checkout_latest` or
              [`optimize_csr`][lance_csr.CypherEngine.optimize_csr] produced it:
              `"build"` (first), `"rebuild"` (full), `"loaded"` (from a file stored with
              `persist_csr=True`, with any changes since applied as a delta), `"delta"`
              (changes applied) or `"reused"` (no edge change).
            - `version`: the relationship dataset version it reflects (`None` for
              PyArrow).
            - `base_version`: the relationship dataset version of the last full
              build (`None` for PyArrow). It trails `version` while deltas apply.
            - `build_ms`: wall time of that build or delta, both directions.
            - `store_error`: why the base CSR couldn't be stored with
              `persist_csr=True`, or `None`. It stays set while the engine keeps that
              base, through deltas.
        """
        return self._inner.csr_stats()

    def optimize_datasets(
        self, *, cleanup_older_than: timedelta | None = None
    ) -> dict[str, DatasetOptimizeStats]:
        """Compact the engine's Lance datasets, move onto them, delete old versions.

        Every write to a Lance dataset makes a new version, and appends leave small
        fragments that slow scans down. This runs the maintenance Lance and LanceDB
        recommend, and keeps the engine in step with it. For each Lance dataset the
        engine reads, it:

        1. opens the newest version, including writes made through other handles,
           such as a LanceDB table;
        2. compacts it, rewriting small fragments into larger ones;
        3. moves the engine to the new versions, as
           [`checkout_latest`][lance_csr.CypherEngine.checkout_latest] does;
        4. deletes versions older than `cleanup_older_than`, except the latest and
           tagged ones.

        The engine moves before the cleanup, so it never reads a deleted version. See
        [Compaction and old versions](lance.md#compaction-and-old-versions).

        Parameters
        ----------
        cleanup_older_than : datetime.timedelta or None
            Keyword-only. Delete versions older than this; `None` means 7 days, as in
            LanceDB's `optimize()`. Other readers pinned to an older version, including
            queries already running on this engine, fail once their version is
            deleted, so use a short window only when nothing else reads the datasets.

        Returns
        -------
        dict[str, DatasetOptimizeStats]
            Per Lance dataset: `fragments_removed` and `fragments_added` by compaction,
            `old_versions` and `bytes_removed` by cleanup. PyArrow inputs aren't listed.

        Raises
        ------
        TypeError
            If `cleanup_older_than` isn't a `timedelta`.
        ValueError
            If `cleanup_older_than` is negative.
        RuntimeError
            If a dataset can't be opened, compacted or cleaned up. Datasets compacted
            before the failure stay compacted.

        Notes
        -----
        Lance indexes, such as vector indexes, aren't updated with new rows. LanceDB
        users who keep them still run `table.optimize()` for that, followed by
        `checkout_latest()`.
        """
        before = self._inner.csr_stats()
        result = self._inner.optimize_datasets(cleanup_older_than=cleanup_older_than)
        self._warn_store_errors(before)
        return result

    def export_graph(
        self,
        rel_types: list[str] | None = None,
        *,
        directed: bool = True,
        weight: str | dict[str, str] | None = None,
        combine_edges: Literal["sum", "min", "max"] = "sum",
        default_weight: float | None = None,
    ) -> GraphExport:
        """The graph as dense CSR arrays, for graph-algorithm libraries.

        Reads the key column of each node table and the endpoint columns of each
        relationship table from the engine's pinned snapshot, as queries see it, with
        or without `use_csr`. Lance datasets are scanned for just those columns. See
        [Graph algorithms](graph-algorithms.md).

        Parameters
        ----------
        rel_types : list[str] or None
            Relationship types to include, case-insensitive. `None` includes every
            type whose dataset has integer endpoint columns. The nodes are those of
            the labels at either end of the included types.
        directed : bool
            Keyword-only. `True` returns out-edge and in-edge arrays; `False` returns
            one set of arrays listing every edge in both directions.
        weight : str, dict[str, str] or None
            Keyword-only. The numeric column holding each edge's weight, exported as
            `float64` in `out_weights` and `in_weights`. A string names one column for
            every included type; a dict maps relationship types to their own columns,
            so types can store weights under different names. Names are
            case-insensitive. A type the dict leaves out, or that lacks the shared
            column, needs `default_weight`. An edge whose weight is null is left out
            and counted in `dropped_edges`. `None` (the default) exports no weights,
            and the export then needs no memory for them.
        combine_edges : {"sum", "min", "max"}
            Keyword-only. How an edge listed more than once combines its weights:
            `"sum"` (the default) adds them, as for counts or amounts; `"min"` keeps the
            smallest, as for distances; `"max"` the largest. In an undirected export,
            `a -> b` and `b -> a` are one edge and combine the same way. Ignored without
            `weight`.
        default_weight : float or None
            Keyword-only. The weight of every edge of an included type that has no
            weight column: one the `weight` dict leaves out, or one without the shared
            `weight` column (at least one type must have it). A column named in the
            dict must exist even with a default. Needs `weight`.

        Returns
        -------
        GraphExport
            Dense node ids, their keys and the CSR arrays.

        Raises
        ------
        ValueError
            If a relationship type or label isn't in the config, has no dataset, or a
            key or endpoint column is missing or isn't an integer type up to `int64`
            (`uint64` isn't supported), a weight column is missing or isn't numeric, an
            included type has no weight column and there's no `default_weight`, the
            `weight` dict names a type that isn't included, `default_weight` is given
            without `weight`, `combine_edges` isn't one of the three rules, or there's
            nothing to export.
        RuntimeError
            If a node key appears twice within a label, or a scan fails.

        Examples
        --------
        >>> import pyarrow as pa
        >>> from lance_csr import CypherEngine, GraphConfig
        >>> config = (
        ...     GraphConfig.builder()
        ...     .node_table("Person", "id")
        ...     .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
        ...     .build()
        ... )
        >>> engine = CypherEngine(config, {
        ...     "Person": pa.table({"id": [10, 20, 30]}),
        ...     "KNOWS": pa.table({"src_id": [10, 20], "dst_id": [20, 30]}),
        ... })
        >>> g = engine.export_graph()
        >>> g.out_indptr.to_pylist(), g.out_indices.to_pylist()
        ([0, 1, 2, 2], [1, 2])
        """
        return GraphExport._from_inner(
            self._inner.export_graph(
                rel_types,
                directed=directed,
                weight=weight,
                combine_edges=combine_edges,
                default_weight=default_weight,
            )
        )

    def export_subgraph[ParameterT: Parameter](
        self,
        query: str,
        params: dict[str, ParameterT] | None = None,
        *,
        directed: bool = True,
        weight: str | None = None,
        combine_edges: Literal["sum", "min", "max"] = "sum",
        default_weight: float | None = None,
    ) -> GraphExport:
        """The part of the graph a Cypher query selects, as dense CSR arrays.

        Runs `query` and builds the same arrays as
        [`export_graph`][lance_csr.CypherEngine.export_graph], by the same edge rules,
        from its result: one row per edge, with the source and target node keys as the
        first two columns. The nodes are the keys present in the result. When both
        columns are a labeled node's key, as `RETURN a.id, b.id` with `(a:Person)` and
        `id` its key, the ids are grouped per label as in `export_graph`, so
        `label_ranges` is filled and
        [`write_node_column`][lance_csr.CypherEngine.write_node_column] can write
        results back; otherwise all keys share one id space and `label_ranges` is
        empty. See [Graph algorithms](graph-algorithms.md#subgraphs).

        Parameters
        ----------
        query : str
            A Cypher query returning the source and target keys as its first two
            columns, which must be integer types up to `int64`.
        params : dict or None
            Query parameters, as for [`execute`][lance_csr.CypherEngine.execute].
        directed : bool
            Keyword-only. `True` returns out-edge and in-edge arrays; `False` returns
            one set of arrays listing every edge in both directions.
        weight : str or None
            Keyword-only. A numeric column of the result holding each edge's weight,
            case-insensitive, exported as `float64` in `out_weights` and `in_weights`.
            An edge whose weight is null is left out and counted in `dropped_edges`.
        combine_edges : {"sum", "min", "max"}
            Keyword-only. How an edge listed more than once combines its weights, as in
            `export_graph`. Ignored without weights.
        default_weight : float or None
            Keyword-only. Without `weight`, the weight of every row: `1.0` makes each
            edge's weight the number of rows that list it.

        Returns
        -------
        GraphExport
            Dense node ids, their keys and the CSR arrays.

        Raises
        ------
        ValueError
            If the query is invalid, its first two columns aren't integer keys, the
            weight column is missing or isn't numeric, or `combine_edges` isn't one of
            the three rules.
        RuntimeError
            If the query fails while running.

        Examples
        --------
        >>> import pyarrow as pa
        >>> from lance_csr import CypherEngine, GraphConfig
        >>> config = (
        ...     GraphConfig.builder()
        ...     .node_table("Person", "id")
        ...     .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
        ...     .build()
        ... )
        >>> engine = CypherEngine(config, {
        ...     "Person": pa.table({"id": [10, 20, 30, 40]}),
        ...     "KNOWS": pa.table({"src_id": [10, 20, 30], "dst_id": [20, 30, 40]}),
        ... })
        >>> g = engine.export_subgraph(
        ...     "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id >= 20 "
        ...     "RETURN a.id, b.id"
        ... )
        >>> g.node_keys.to_pylist(), g.out_indices.to_pylist()
        ([20, 30, 40], [1, 2])
        """
        return GraphExport._from_inner(
            self._inner.export_subgraph(
                query,
                params,
                directed=directed,
                weight=weight,
                combine_edges=combine_edges,
                default_weight=default_weight,
            )
        )

    def write_node_column(
        self,
        export: GraphExport,
        column: str,
        values: Any,
        *,
        overwrite: bool = False,
    ) -> None:
        """Add one value per node of an export, such as an algorithm's scores, as a
        column of the node tables.

        `values[i]` belongs to dense id `i` of `export`. For each label in
        `export.label_ranges`, the label's slice of `values` is paired with its keys
        and merged into its Lance dataset on the key column, which writes only the new
        column's data files as a new dataset version. Node rows the export left out
        (null keys) get nulls. The engine then moves to the new versions of those
        datasets, so queries see the column at once; other datasets stay pinned where
        they were. See [Graph algorithms](graph-algorithms.md).

        Parameters
        ----------
        export : GraphExport
            The export the values were computed on, from
            [`export_graph`][lance_csr.CypherEngine.export_graph].
        column : str
            The column to add to each node table.
        values : pyarrow.Array, pyarrow.ChunkedArray, numpy array or list
            `export.num_nodes` values, in dense-id order. Anything `pyarrow.array`
            takes is converted with it.
        overwrite : bool
            Keyword-only. Replace `column` where it already exists, by dropping it
            first. A dropped column's files stay on disk until Lance's
            `cleanup_old_versions` removes old versions.

        Raises
        ------
        ValueError
            If `values` doesn't have one entry per node, a label's data is a PyArrow
            table rather than a Lance dataset, `column` is a key column, or it exists
            and `overwrite` is `False`.
        RuntimeError
            If a write fails. Labels are written one after another, so a failure can
            leave earlier labels written.
        """
        import pyarrow as pa

        if isinstance(values, pa.ChunkedArray):
            values = values.combine_chunks()
        elif not isinstance(values, pa.Array):
            values = pa.array(values)
        self._inner.write_node_column(
            export._inner, column, values, overwrite=overwrite
        )

    def dataset_versions(self) -> dict[str, int]:
        """The pinned version of each Lance dataset, keyed by dataset name.

        PyArrow inputs aren't listed.
        """
        return self._inner.dataset_versions()

    def config(self) -> GraphConfig:
        """The engine's graph config."""
        return GraphConfig._from_inner(self._inner.config())

    def execute_with_vector_rerank(
        self, query: str, vector_search: VectorSearch
    ) -> Table:
        """Run a Cypher query, then rank its rows by vector similarity.

        Like `CypherQuery.execute_with_vector_rerank`, with the registered datasets.
        See [Vector search](vector-search.md).

        Parameters
        ----------
        query : str
            The Cypher query. It must return the column `vector_search` ranks by.
        vector_search : VectorSearch
            The ranking.

        Returns
        -------
        pyarrow.Table
            At most `top_k` rows, closest first.

        Raises
        ------
        ValueError
            If the query is invalid or the vector search is misconfigured.
        RuntimeError
            If execution fails.
        """
        return self._inner.execute_with_vector_rerank(
            query, inner(vector_search, VectorSearch, "vector_search")
        )


@final
class CypherQuery:
    """One parsed Cypher query, run against the datasets passed to each call.

    Builder methods return a new query. Datasets are read into memory for each call; use
    [`CypherEngine`][lance_csr.CypherEngine] to register them once and run many queries.

    Parameters
    ----------
    query_text : str
        The Cypher query.

    Raises
    ------
    ValueError
        If `query_text` doesn't parse.
    """

    __slots__ = ("_inner",)
    _inner: _graph.CypherQuery

    def __init__(self, query_text: str) -> None:
        self._inner = _graph.CypherQuery(query_text)

    @classmethod
    def _from_inner(cls, inner: _graph.CypherQuery) -> Self:
        query = object.__new__(cls)
        query._inner = inner
        return query

    def __repr__(self) -> str:
        return repr(self._inner)

    def with_config(self, config: GraphConfig) -> Self:
        """Set the graph config. Required before running the query.

        Parameters
        ----------
        config : GraphConfig
            The graph mapping.

        Returns
        -------
        CypherQuery
            A new query with the config set.
        """
        return self._from_inner(
            self._inner.with_config(inner(config, GraphConfig, "config"))
        )

    def with_parameter(self, key: str, value: Parameter) -> Self:
        """Bind `$key` to `value`.

        Parameters
        ----------
        key : str
            The parameter name, without `$`.
        value : Parameter
            The value, of a type that
            [`CypherEngine.execute`][lance_csr.CypherEngine.execute] accepts in
            `params`.

        Returns
        -------
        CypherQuery
            A new query with the parameter bound.
        """
        return self._from_inner(self._inner.with_parameter(key, value))

    def query_text(self) -> str:
        """The query text."""
        return self._inner.query_text()

    def parameters(self) -> dict[str, Scalar]:
        """The bound parameters."""
        return self._inner.parameters()

    def execute[DatasetT: Dataset](
        self,
        datasets: dict[str, DatasetT],
        strategy: ExecutionStrategy | None = None,
        *,
        timeout: float | None = None,
    ) -> Table:
        """Run the query.

        Parameters
        ----------
        datasets : dict[str, Dataset]
            The tables, keyed by node label or relationship type (see
            [Datasets](#datasets)). They are read into memory for this call.
        strategy : ExecutionStrategy or None
            How to run the query; `None` means `ExecutionStrategy.DataFusion`.
        timeout : float or None
            Keyword-only. Seconds for the whole query; `None` means no deadline. See
            [Timeouts](#timeouts).

        Returns
        -------
        pyarrow.Table
            The query result.

        Raises
        ------
        ValueError
            If the query is invalid or a dataset is missing.
        NotImplementedError
            If the query uses an unsupported Cypher feature, or `strategy` is
            `ExecutionStrategy.LanceNative`.
        TimeoutError
            If the query runs past `timeout`.
        RuntimeError
            If execution fails.
        """
        return self._inner.execute(
            datasets,
            _option(strategy, ExecutionStrategy, "strategy"),
            timeout=timeout,
        )

    def execute_with_namespace(
        self, namespace: DirNamespace, strategy: ExecutionStrategy | None = None
    ) -> Table:
        """Run the query, reading each label from `<base_uri>/<label>.lance`.

        Parameters
        ----------
        namespace : DirNamespace
            Where the datasets are.
        strategy : ExecutionStrategy or None
            How to run the query; `None` means `ExecutionStrategy.DataFusion`.

        Returns
        -------
        pyarrow.Table
            The query result.

        Raises
        ------
        ValueError
            If the query is invalid.
        RuntimeError
            If a dataset can't be read or execution fails.
        """
        return self._inner.execute_with_namespace(
            inner(namespace, DirNamespace, "namespace"),
            _option(strategy, ExecutionStrategy, "strategy"),
        )

    def explain[DatasetT: Dataset](self, datasets: dict[str, DatasetT]) -> str:
        """The plans for the query, without running it.

        Parameters
        ----------
        datasets : dict[str, Dataset]
            The tables, keyed by node label or relationship type.

        Returns
        -------
        str
            The graph logical plan, the DataFusion logical plan and the physical plan.

        Raises
        ------
        ValueError
            If the query is invalid or a dataset is missing.
        RuntimeError
            If planning fails.
        """
        return self._inner.explain(datasets)

    def to_sql[DatasetT: Dataset](
        self, datasets: dict[str, DatasetT], dialect: SqlDialect | None = None
    ) -> str:
        """Export the query as SQL joins.

        Only supported expressions are exported. See
        [From Cypher to SQL](sql.md#from-cypher-to-sql).

        Parameters
        ----------
        datasets : dict[str, Dataset]
            The tables, keyed by node label or relationship type; their schemas shape
            the SQL.
        dialect : SqlDialect or None
            The SQL dialect; `None` means `SqlDialect.Default`.

        Returns
        -------
        str
            The SQL query.

        Raises
        ------
        NotImplementedError
            If a runtime-only Cypher function has no faithful SQL translation.
        RuntimeError
            If SQL generation fails.
        """
        return self._inner.to_sql(datasets, _option(dialect, SqlDialect, "dialect"))

    def execute_with_vector_rerank[DatasetT: Dataset](
        self, datasets: dict[str, DatasetT], vector_search: VectorSearch
    ) -> Table:
        """Run the query, then rank its rows by vector similarity.

        This is the common GraphRAG pattern: find candidates by graph traversal, then
        rerank them by similarity. With
        [`VectorSearch.use_lance_index(True)`][lance_csr.VectorSearch.use_lance_index]
        and Lance datasets, the Lance index is searched first instead. See
        [Vector search](vector-search.md).

        Parameters
        ----------
        datasets : dict[str, Dataset]
            The tables, keyed by node label or relationship type.
        vector_search : VectorSearch
            The ranking. Its column is a returned column, such as `"d.embedding"`.

        Returns
        -------
        pyarrow.Table
            At most `top_k` rows, closest first.

        Raises
        ------
        ValueError
            If the query is invalid or the vector search is misconfigured.
        RuntimeError
            If execution fails.
        """
        return self._inner.execute_with_vector_rerank(
            datasets, inner(vector_search, VectorSearch, "vector_search")
        )

    def variables(self) -> list[str]:
        """The variables the query uses, including `UNION` branches, unordered."""
        return self._inner.variables()

    def node_labels(self) -> list[str]:
        """The node labels the query uses, including `UNION` branches, unordered."""
        return self._inner.node_labels()

    def relationship_types(self) -> list[str]:
        """The relationship types the query uses, with `UNION` branches, unordered."""
        return self._inner.relationship_types()
