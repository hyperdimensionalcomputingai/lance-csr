"""SQL over Arrow tables and Lance datasets, through DataFusion, with graph patterns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, final

from ._internal import graph as _graph
from ._types import inner
from .config import GraphConfig
from .cypher import CypherEngine
from .dialect import SqlDialect

__all__ = ["SqlDialect", "SqlEngine", "SqlQuery"]

if TYPE_CHECKING:
    from pyarrow import Table

    from ._types import Dataset, Parameter


@final
class SqlQuery:
    """One SQL query, run against the datasets passed to each call.

    No [`GraphConfig`][lance_csr.GraphConfig] is needed: you write the joins yourself.
    Table names in the query match the dataset keys case-insensitively. See
    [SQL queries](sql.md).

    Parameters
    ----------
    sql : str
        The SQL query.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lance_csr import SqlQuery
    >>> person = pa.table({"name": ["Alice", "Bob"], "age": [28, 34]})
    >>> query = SqlQuery("SELECT name FROM person WHERE age > 30")
    >>> query.execute({"person": person}).to_pylist()
    [{'name': 'Bob'}]
    """

    __slots__ = ("_inner",)
    _inner: _graph.SqlQuery

    def __init__(self, sql: str) -> None:
        self._inner = _graph.SqlQuery(sql)

    def __repr__(self) -> str:
        return repr(self._inner)

    def sql(self) -> str:
        """The query text."""
        return self._inner.sql()

    def execute[DatasetT: Dataset](self, datasets: dict[str, DatasetT]) -> Table:
        """Register `datasets` and run the query.

        Parameters
        ----------
        datasets : dict[str, Dataset]
            The tables, keyed by the names the query uses. Lance datasets are read into
            memory for this call.

        Returns
        -------
        pyarrow.Table
            The query result.

        Raises
        ------
        RuntimeError
            If the query fails.
        """
        return self._inner.execute(datasets)

    def explain[DatasetT: Dataset](self, datasets: dict[str, DatasetT]) -> str:
        """DataFusion's logical and physical plans for the query.

        Parameters
        ----------
        datasets : dict[str, Dataset]
            The tables, keyed by the names the query uses.

        Returns
        -------
        str
            The plans.

        Raises
        ------
        RuntimeError
            If planning fails.
        """
        return self._inner.explain(datasets)


@final
class SqlEngine:
    """Run many SQL queries on the same datasets, with graph patterns given a graph.

    Each dataset is a table under its lower-cased name. Lance datasets are scanned in
    place, pinned to the version passed in, with column and filter pushdown; PyArrow
    inputs are held in memory. With a `config`, a `GRAPH_TABLE` clause embeds a Cypher
    graph pattern, and with `use_csr=True` its hops run on the [CSR](csr.md). See
    [Graph patterns in SQL](sql.md#graph-patterns-in-sql).

    Plans are cached per snapshot, as [`CypherEngine`][lance_csr.CypherEngine] caches
    Cypher templates: up to 128, keyed by the query text and the names and types of
    scalar parameters, with values bound on every call. Container parameters and
    `GRAPH_TABLE` patterns Cypher can't plan as templates are planned on every call.

    Parameters
    ----------
    datasets : dict[str, Dataset]
        The tables, keyed by the names queries use; with a `config`, also by node label
        or relationship type, as for `CypherEngine`.
    config : GraphConfig or None
        The graph `GRAPH_TABLE` patterns match. Without one, `GRAPH_TABLE` raises
        `ValueError`.
    use_csr : bool
        Build the experimental CSR index, as for `CypherEngine`. Needs a `config`.

    Raises
    ------
    ValueError
        If `datasets` is empty, `use_csr` is set without a `config`, or a configured
        column is missing.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lance_csr import SqlEngine
    >>> engine = SqlEngine({
    ...     "person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}),
    ...     "knows": pa.table({"src": [1], "dst": [2]}),
    ... })
    >>> engine.execute(
    ...     "SELECT p.name FROM person p JOIN knows k ON p.id = k.src "
    ...     "WHERE k.dst = $dst",
    ...     {"dst": 2},
    ... ).to_pylist()
    [{'name': 'Alice'}]
    """

    __slots__ = ("_inner",)
    _inner: _graph.SqlEngine

    def __init__[DatasetT: Dataset](
        self,
        datasets: dict[str, DatasetT],
        config: GraphConfig | None = None,
        use_csr: bool = False,
    ) -> None:
        self._inner = _graph.SqlEngine(
            datasets,
            None if config is None else inner(config, GraphConfig, "config"),
            use_csr,
        )

    @classmethod
    def from_engine(cls, engine: CypherEngine) -> Self:
        """SQL over a `CypherEngine`'s snapshot, sharing its datasets and CSR.

        Nothing is rebuilt. The two share one snapshot, so
        [`checkout_latest`][lance_csr.SqlEngine.checkout_latest] on either moves both.

        Parameters
        ----------
        engine : CypherEngine
            The engine whose datasets, graph and CSR to use.

        Returns
        -------
        SqlEngine
            An engine over the same snapshot.
        """
        sql_engine = cls.__new__(cls)
        sql_engine._inner = _graph.SqlEngine.from_engine(
            inner(engine, CypherEngine, "engine")
        )
        return sql_engine

    def __repr__(self) -> str:
        return repr(self._inner)

    def execute[ParameterT: Parameter](
        self,
        sql: str,
        params: dict[str, ParameterT] | None = None,
        *,
        timeout: float | None = None,
    ) -> Table:
        """Run a SQL query against the registered datasets.

        Parameters
        ----------
        sql : str
            The SQL query. Only queries run: DDL (`CREATE`), DML (`INSERT`, `COPY`) and
            statements such as `SET` are rejected.
        params : dict[str, Parameter] or None
            Values for `$name` parameters, in the SQL and inside `GRAPH_TABLE` alike.
            Names are case-insensitive.
        timeout : float or None
            Keyword-only. Seconds for the whole query, planning included; `None` means
            no deadline. See [Query deadlines](cypher.md#query-deadlines).

        Returns
        -------
        pyarrow.Table
            The query result.

        Raises
        ------
        ValueError
            If the query is invalid, would write, uses `GRAPH_TABLE` without a graph,
            or a parameter has no value.
        TimeoutError
            If the query runs past `timeout`.
        RuntimeError
            If execution fails.
        """
        return self._inner.execute(sql, params, timeout=timeout)

    def explain[ParameterT: Parameter](
        self, sql: str, params: dict[str, ParameterT] | None = None
    ) -> str:
        """The optimized logical and physical plans for a query, without running it.

        The plans are built afresh, with `params` bound, and show pushed-down Lance
        scans and CSR operators.

        Parameters
        ----------
        sql : str
            The SQL query.
        params : dict[str, Parameter] or None
            Values for `$name` parameters, as for
            [`execute`][lance_csr.SqlEngine.execute].

        Returns
        -------
        str
            The plans.

        Raises
        ------
        ValueError
            If the query is invalid or would write.
        """
        return self._inner.explain(sql, params)

    def checkout_latest(self) -> None:
        """Move every Lance dataset to its latest version.

        As [`CypherEngine.checkout_latest`][lance_csr.CypherEngine.checkout_latest]; it
        also clears the plan cache.
        """
        self._inner.checkout_latest()
