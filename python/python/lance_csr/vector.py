"""Vector similarity search over Arrow tables, and reranking of Cypher results."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, Self, final

from ._internal import graph as _graph
from ._types import inner

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyarrow import RecordBatch, RecordBatchReader, Table

    from ._types import ArrowStream


class DistanceMetric(IntEnum):
    """The distance `VectorSearch` ranks rows by; smaller is closer.

    Attributes
    ----------
    L2
        Euclidean distance.
    Cosine
        `1 - cosine similarity`.
    Dot
        Negative dot product.
    """

    L2 = 0
    Cosine = 1
    Dot = 2


@final
class VectorSearch:
    """An immutable builder for vector similarity search.

    Every method except [`search`][lance_csr.VectorSearch.search] returns a new builder.
    Pass the builder to `search` to rank an Arrow table, or to
    `execute_with_vector_rerank` on [`CypherEngine`][lance_csr.CypherEngine] or
    [`CypherQuery`][lance_csr.CypherQuery] to rerank a Cypher result. This is separate
    from the `vector_distance()` and `vector_similarity()` Cypher functions. See
    [Vector search](vector-search.md).

    Parameters
    ----------
    column : str
        The vector column. When reranking a Cypher result, use the returned column name,
        `alias.property` (for example `"d.embedding"`).

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lance_csr import DistanceMetric, VectorSearch
    >>> table = pa.table({"id": [1, 2], "embedding": [[1.0, 0.0], [0.0, 1.0]]})
    >>> search = (
    ...     VectorSearch("embedding")
    ...     .query_vector([1.0, 0.1])
    ...     .metric(DistanceMetric.Cosine)
    ...     .top_k(1)
    ... )
    >>> search.search(table)["id"].to_pylist()
    [1]
    """

    __slots__ = ("_inner",)
    _inner: _graph.VectorSearch

    def __init__(self, column: str) -> None:
        self._inner = _graph.VectorSearch(column)

    @classmethod
    def _from_inner(cls, inner: _graph.VectorSearch) -> Self:
        search = object.__new__(cls)
        search._inner = inner
        return search

    def __repr__(self) -> str:
        return repr(self._inner)

    def query_vector(self, vector: Sequence[float]) -> Self:
        """Set the query vector. Required before searching.

        Parameters
        ----------
        vector : Sequence[float]
            The vector to compare each row with.

        Returns
        -------
        VectorSearch
            A new builder with the query vector set.
        """
        return self._from_inner(self._inner.query_vector(vector))

    def metric(self, metric: DistanceMetric) -> Self:
        """Set the distance metric. Default `DistanceMetric.L2`.

        Parameters
        ----------
        metric : DistanceMetric
            The distance to rank by.

        Returns
        -------
        VectorSearch
            A new builder with the metric set.
        """
        return self._from_inner(
            self._inner.metric(inner(metric, DistanceMetric, "metric"))
        )

    def top_k(self, k: int) -> Self:
        """Set how many rows to return. Default `10`.

        Parameters
        ----------
        k : int
            The number of closest rows to keep.

        Returns
        -------
        VectorSearch
            A new builder with `top_k` set.
        """
        return self._from_inner(self._inner.top_k(k))

    def include_distance(self, include: bool) -> Self:
        """Set whether results get a distance column. Default `True`.

        Parameters
        ----------
        include : bool
            Add the distance column when `True`.

        Returns
        -------
        VectorSearch
            A new builder with the setting applied.
        """
        return self._from_inner(self._inner.include_distance(include))

    def distance_column_name(self, name: str) -> Self:
        """Set the name of the distance column. Default `"_distance"`.

        Parameters
        ----------
        name : str
            The column name.

        Returns
        -------
        VectorSearch
            A new builder with the column name set.
        """
        return self._from_inner(self._inner.distance_column_name(name))

    def use_lance_index(self, enabled: bool) -> Self:
        """Set whether reranking queries a Lance ANN index first. Default `False`.

        When the datasets are Lance datasets, reranking searches the Lance
        approximate-nearest-neighbour index first and then runs the Cypher query on the
        top-k rows. This can be much faster on large datasets, but it can change the
        results when the query also filters, because the filter then applies to the
        top-k rows only.

        Parameters
        ----------
        enabled : bool
            Use the Lance index when possible.

        Returns
        -------
        VectorSearch
            A new builder with the setting applied.
        """
        return self._from_inner(self._inner.use_lance_index(enabled))

    def search(
        self, table: Table | RecordBatch | RecordBatchReader | ArrowStream
    ) -> Table:
        """Brute-force search `table`.

        Parameters
        ----------
        table : pyarrow.Table, RecordBatch, RecordBatchReader or ArrowStream
            The rows to search, with the vector column.

        Returns
        -------
        pyarrow.Table
            At most `top_k` rows, sorted by distance, closest first.

        Raises
        ------
        ValueError
            If no query vector is set or the column doesn't exist.
        RuntimeError
            If the search fails.
        """
        return self._inner.search(table)
