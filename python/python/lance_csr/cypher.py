"""Running Cypher queries: one at a time, or many through a cached engine."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, Any, Self, final

from ._internal import graph as _graph
from ._types import inner
from .config import GraphConfig
from .namespace import DirNamespace
from .sql import SqlDialect
from .vector import VectorSearch

if TYPE_CHECKING:
    from pyarrow import RecordBatchReader, Table

    from ._types import CsrStats, Dataset, NodeIndexStats, Parameter, Scalar


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
    [`refresh`][lance_csr.CypherEngine.refresh]. Each dataset is pinned separately, so
    versions across tables are not a transactional snapshot. PyArrow inputs are held in
    memory and never change; create a new engine to replace them.

    [`execute`][lance_csr.CypherEngine.execute] and
    [`execute_batches`][lance_csr.CypherEngine.execute_batches] reuse up to 128
    unoptimized query templates per snapshot, keyed by the exact query text and the
    names and types of scalar parameters. Values are bound before optimization on every
    call. Container parameters, rich path values, `CALL` subqueries, parameterized
    `SKIP`/`LIMIT` and unsupported template shapes use ordinary planning.
    [`explain`][lance_csr.CypherEngine.explain] always plans directly, and a successful
    `refresh` clears the cache. See [Repeated queries](lance.md#repeated-queries).

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
        endpoints, and node indexes for labels with unique integer keys.
    target_partitions : int or None
        Keyword-only. A target from 1 to 1024 for DataFusion query partitioning; `None`
        keeps DataFusion's default. `refresh` preserves it. It is not a thread or memory
        limit. See [Query parallelism](lance.md#query-parallelism).

    Raises
    ------
    ValueError
        If `datasets` is empty, a configured key or endpoint column is missing, a Lance
        dataset was opened through a namespace client and can't be reopened by URI, or
        `target_partitions` is outside 1 to 1024.
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
    ) -> None:
        self._inner = _graph.CypherEngine(
            inner(config, GraphConfig, "config"),
            datasets,
            use_csr,
            target_partitions=target_partitions,
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
        [`refresh`][lance_csr.CypherEngine.refresh] doesn't affect it. Planning errors
        raise immediately; execution errors raise while reading.

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
        self, query: str, params: dict[str, ParameterT] | None = None
    ) -> str:
        """The plans for a query, without running it.

        Parameters
        ----------
        query : str
            The Cypher query.
        params : dict[str, Parameter] or None
            Values for `$name` parameters, as for
            [`execute`][lance_csr.CypherEngine.execute].

        Returns
        -------
        str
            The graph logical plan, the DataFusion logical plan and the physical plan,
            including pushed-down scans.

        Raises
        ------
        ValueError
            If the query is invalid.
        """
        return self._inner.explain(query, params)

    def refresh(self) -> None:
        """Move every Lance dataset to its latest version.

        Updates the CSR, node indexes and statistics to match. Datasets whose version
        and commit time are unchanged keep their open handle. The replacement is
        validated in full before it's swapped in: on error nothing changes and the
        previous snapshot stays usable. Queries already running finish on the snapshot
        they started with. PyArrow inputs are unchanged. Concurrent calls run one at a
        time, and `target_partitions` is preserved. See
        [Lance datasets](lance.md#refresh).
        """
        self._inner.refresh()

    def optimize_csr(self) -> None:
        """Fold each CSR's delta into a new base (experimental; needs `use_csr=True`).

        The CSR's counterpart of Lance's `optimize_indices()`.
        [`refresh`][lance_csr.CypherEngine.refresh] applies Lance writes as a delta
        over the base CSR, and rebuilds on its own only once pending changes pass 25%
        of the base. This rebuilds every CSR that carries a delta now, at the version
        the engine is pinned to, so lookups stop carrying it. It scans every edge
        again, so call it after a bulk load or at a quiet time, not after every
        refresh. It doesn't move to
        newer versions: call `refresh` first for that. CSRs without a delta, and
        PyArrow inputs, are left as they are, and query results never change.
        Afterwards [`csr_stats`][lance_csr.CypherEngine.csr_stats] reports `update`
        `"rebuild"`, `unindexed_edges` 0 and `base_version` equal to `version`. As for
        `refresh`, the replacement is validated in full before it's swapped in, and the
        two run one at a time. See
        [Folding the delta in](csr.md#folding-the-delta-in).
        """
        self._inner.optimize_csr()

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

        An identical result after [`refresh`][lance_csr.CypherEngine.refresh] means the
        snapshot itself didn't change. See
        [Inspecting the CSR](csr.md#inspecting-the-csr).

        Returns
        -------
        dict[str, CsrStats]
            Per relationship type with a CSR, empty without `use_csr=True`:

            - `edges`: live indexed edges (null endpoints are skipped).
            - `delta_edges`, `tombstones`: edges appended and removed since the last
              full build, held outside the base CSR.
            - `unindexed_edges`: live edges outside the base CSR, appended since its
              last full build and not since removed.
            - `update`: how the last refresh or
              [`optimize_csr`][lance_csr.CypherEngine.optimize_csr] produced it:
              `"build"` (first), `"rebuild"` (full), `"delta"` (changes applied) or
              `"reused"` (no edge change).
            - `version`: the relationship dataset version it reflects (`None` for
              PyArrow).
            - `base_version`: the relationship dataset version of the last full
              build (`None` for PyArrow). It trails `version` while deltas apply.
            - `build_ms`: wall time of that build or delta, both directions.
        """
        return self._inner.csr_stats()

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
