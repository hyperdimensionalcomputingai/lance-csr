"""Plain SQL over Arrow tables and Lance datasets, through DataFusion."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, final

from ._internal import graph as _graph

if TYPE_CHECKING:
    from pyarrow import Table

    from ._types import Dataset


class SqlDialect(IntEnum):
    """The SQL dialect [`CypherQuery.to_sql`][lance_csr.CypherQuery.to_sql] writes.

    Attributes
    ----------
    Default
        Generic DataFusion SQL.
    Spark
        Spark SQL.
    PostgreSql
        PostgreSQL.
    MySql
        MySQL.
    Sqlite
        SQLite.
    """

    Default = 0
    Spark = 1
    PostgreSql = 2
    MySql = 3
    Sqlite = 4


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
    """Run many SQL queries against the same datasets.

    The datasets are registered once, under lower-cased names, in a DataFusion session
    that every query reuses.

    Parameters
    ----------
    datasets : dict[str, Dataset]
        The tables, keyed by the names queries use.

    Raises
    ------
    ValueError
        If `datasets` is empty.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lance_csr import SqlEngine
    >>> engine = SqlEngine({
    ...     "person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}),
    ...     "knows": pa.table({"src": [1], "dst": [2]}),
    ... })
    >>> engine.execute(
    ...     "SELECT p.name FROM person p JOIN knows k ON p.id = k.src"
    ... ).to_pylist()
    [{'name': 'Alice'}]
    """

    __slots__ = ("_inner",)
    _inner: _graph.SqlEngine

    def __init__[DatasetT: Dataset](self, datasets: dict[str, DatasetT]) -> None:
        self._inner = _graph.SqlEngine(datasets)

    def __repr__(self) -> str:
        return repr(self._inner)

    def execute(self, sql: str) -> Table:
        """Run a SQL query against the registered datasets.

        Parameters
        ----------
        sql : str
            The SQL query.

        Returns
        -------
        pyarrow.Table
            The query result.

        Raises
        ------
        RuntimeError
            If the query fails.
        """
        return self._inner.execute(sql)
