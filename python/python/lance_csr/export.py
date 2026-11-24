"""A graph as dense CSR arrays, for graph-algorithm libraries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, final

if TYPE_CHECKING:
    from pyarrow import DoubleArray, Int64Array, UInt64Array

    from ._internal import graph as _graph


@final
class GraphExport:
    """A graph as compressed sparse rows over dense node ids, from
    [`CypherEngine.export_graph`][lance_csr.CypherEngine.export_graph].

    Graph-algorithm libraries such as icebug, igraph or scipy take a graph in this
    form: nodes numbered `0` to `num_nodes - 1`, and for each node `i` its neighbours
    at `indices[indptr[i]:indptr[i + 1]]`. `node_keys` maps each dense id back to its
    node's key, so results computed per dense id can be joined back to the node table.

    The arrays are pyarrow arrays over buffers built in Rust, passed without copying.
    See [Graph algorithms](graph-algorithms.md).
    """

    __slots__ = ("_inner",)
    _inner: _graph.GraphExport

    def __init__(self) -> None:
        raise TypeError(
            "GraphExport has no constructor; use CypherEngine.export_graph()"
        )

    @classmethod
    def _from_inner(cls, inner: _graph.GraphExport) -> Self:
        export = object.__new__(cls)
        export._inner = inner
        return export

    def __repr__(self) -> str:
        return repr(self._inner)

    @property
    def num_nodes(self) -> int:
        """Number of nodes. Dense ids run from 0 to `num_nodes - 1`."""
        return self._inner.num_nodes

    @property
    def directed(self) -> bool:
        """Whether the arrays describe a directed graph."""
        return self._inner.directed

    @property
    def node_keys(self) -> Int64Array:
        """The key of each dense id.

        Each label's nodes are in the order its table is scanned; rows with a null key
        are left out.
        """
        return self._inner.node_keys

    @property
    def label_ranges(self) -> dict[str, tuple[int, int]]:
        """Each node label's dense ids, as `(start, end)` with `end` excluded.

        Labels get consecutive ranges in name order, so equal keys in two labels stay
        distinct nodes.
        """
        return self._inner.label_ranges

    @property
    def out_indptr(self) -> UInt64Array:
        """Offsets into `out_indices`, `num_nodes + 1` entries."""
        return self._inner.out_indptr

    @property
    def out_indices(self) -> UInt64Array:
        """Each node's out-neighbours, sorted, without duplicates.

        An undirected graph lists every edge in both directions and a self-loop once.
        """
        return self._inner.out_indices

    @property
    def in_indptr(self) -> UInt64Array | None:
        """Offsets into `in_indices`; `None` for an undirected graph."""
        return self._inner.in_indptr

    @property
    def in_indices(self) -> UInt64Array | None:
        """Each node's in-neighbours, sorted, without duplicates.

        `None` for an undirected graph. Algorithms that read in-edges, such as PageRank,
        need them for a directed graph.
        """
        return self._inner.in_indices

    @property
    def out_weights(self) -> DoubleArray | None:
        """The weight of each entry in `out_indices`, as `float64`.

        `None` unless `export_graph` was given a `weight` column. An edge listed more
        than once, or in an undirected export both `a -> b` and `b -> a`, gets one
        weight, combined by `export_graph`'s `combine_edges` rule (a sum by default).
        """
        return self._inner.out_weights

    @property
    def in_weights(self) -> DoubleArray | None:
        """The weight of each entry in `in_indices`, as `float64`.

        `None` for an undirected export, or unless `export_graph` was given a `weight`
        column.
        """
        return self._inner.in_weights

    @property
    def dropped_edges(self) -> int:
        """Edges left out because an endpoint key isn't in its node table.

        A query's join between the relationship and node tables leaves them out too.
        Edges with a null endpoint are skipped and not counted.
        """
        return self._inner.dropped_edges
