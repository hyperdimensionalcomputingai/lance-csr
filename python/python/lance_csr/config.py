"""Graph configuration: which tables are nodes and which are relationships."""

from __future__ import annotations

from typing import Self, final

from ._internal import graph as _graph


@final
class GraphConfig:
    """An immutable mapping from tables to a property graph.

    Create one with [`GraphConfig.builder()`][lance_csr.GraphConfig.builder]; there is
    no public constructor. See [Graph schema](graph-schema.md).
    """

    __slots__ = ("_inner",)
    _inner: _graph.GraphConfig

    def __init__(self) -> None:
        raise TypeError("GraphConfig has no constructor; use GraphConfig.builder()")

    @classmethod
    def _from_inner(cls, inner: _graph.GraphConfig) -> Self:
        config = object.__new__(cls)
        config._inner = inner
        return config

    def __repr__(self) -> str:
        return repr(self._inner)

    @staticmethod
    def builder() -> GraphConfigBuilder:
        """Start a new, empty builder.

        Returns
        -------
        GraphConfigBuilder
            The same as `GraphConfigBuilder()`.
        """
        return GraphConfigBuilder()

    def node_labels(self) -> list[str]:
        """The configured node labels, lower-cased, in no particular order."""
        return self._inner.node_labels()

    def relationship_types(self) -> list[str]:
        """The configured relationship types, lower-cased, in no particular order."""
        return self._inner.relationship_types()


@final
class GraphConfigBuilder:
    """Builds a [`GraphConfig`][lance_csr.GraphConfig] from node and relationship
    tables.

    `GraphConfigBuilder()` is the same as `GraphConfig.builder()`. Every method returns
    a new builder and leaves this one unchanged.
    """

    __slots__ = ("_inner",)
    _inner: _graph.GraphConfigBuilder

    def __init__(self) -> None:
        self._inner = _graph.GraphConfigBuilder()

    @classmethod
    def _from_inner(cls, inner: _graph.GraphConfigBuilder) -> Self:
        builder = object.__new__(cls)
        builder._inner = inner
        return builder

    def node_table(self, label: str, primary_key: str) -> Self:
        """Add a node table.

        The nodes labelled `label` are the rows of the dataset named `label`, each
        identified by its `primary_key` column.

        Parameters
        ----------
        label : str
            The node label, and the name of its dataset (for example `"Person"`).
        primary_key : str
            The column that identifies each node.

        Returns
        -------
        GraphConfigBuilder
            A new builder with the node table added.
        """
        return self._from_inner(self._inner.node_table(label, primary_key))

    def rel_table(
        self, rel_type: str, from_table: str, to_table: str, src: str, dst: str
    ) -> Self:
        """Add a relationship table.

        The edges of type `rel_type` are the rows of the dataset named `rel_type`, from
        `from_table` nodes to `to_table` nodes. Both node tables must be added by the
        time [`build`][lance_csr.GraphConfigBuilder.build] runs.

        Parameters
        ----------
        rel_type : str
            The relationship type, and the name of its dataset (for example `"KNOWS"`).
        from_table : str
            The node table at the source end.
        to_table : str
            The node table at the target end.
        src : str
            The column holding the source node's primary key.
        dst : str
            The column holding the target node's primary key.

        Returns
        -------
        GraphConfigBuilder
            A new builder with the relationship table added.
        """
        return self._from_inner(
            self._inner.rel_table(rel_type, from_table, to_table, src, dst)
        )

    def build(self) -> GraphConfig:
        """Validate the tables and return the config.

        Returns
        -------
        GraphConfig
            The configured graph.

        Raises
        ------
        ValueError
            If the configuration is invalid, for example when a relationship's endpoint
            label has no node table.
        """
        return GraphConfig._from_inner(self._inner.build())
