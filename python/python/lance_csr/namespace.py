"""Resolving table names to Lance datasets in a directory."""

from __future__ import annotations

from typing import final

from ._internal import graph as _graph


@final
class DirNamespace:
    """Resolves each table name to the Lance dataset `<base_uri>/<name>.lance`.

    Use it with `CypherQuery.execute_with_namespace`.

    Parameters
    ----------
    base_uri : str
        The directory that holds the datasets.
    """

    __slots__ = ("_inner",)
    _inner: _graph.DirNamespace

    def __init__(self, base_uri: str) -> None:
        self._inner = _graph.DirNamespace(base_uri)

    @property
    def base_uri(self) -> str:
        """The directory that holds the datasets."""
        return self._inner.base_uri
