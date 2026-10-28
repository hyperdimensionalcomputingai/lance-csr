"""Type aliases shared by the public classes, and the helper that unwraps them."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypedDict

from ._internal import graph as _graph

if TYPE_CHECKING:
    from lance import LanceDataset
    from pyarrow import RecordBatch, RecordBatchReader, Table


class ArrowStream(Protocol):
    """Any object that exports the Arrow C stream interface."""

    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...


type Dataset = Table | RecordBatch | RecordBatchReader | LanceDataset | ArrowStream
"""A table to query: a Lance dataset, Arrow table, batch or reader, or C stream."""

type Parameter = (
    int
    | float
    | str
    | bool
    | date
    | datetime
    | None
    | list[Parameter]
    | dict[str, Parameter]
)
"""A value for a `$name` query parameter."""

type Scalar = int | float | str | bool | None | list[Scalar] | dict[str, Scalar]
"""A bound parameter value, as `CypherQuery.parameters` returns it."""


class NodeIndexStats(TypedDict):
    """Statistics for one node index, from `CypherEngine.node_index_stats`."""

    rows: int
    version: int | None
    build_ms: float


class CsrStats(TypedDict):
    """Statistics for one CSR adjacency index, from `CypherEngine.csr_stats`."""

    edges: int
    delta_edges: int
    tombstones: int
    update: Literal["build", "rebuild", "delta", "reused"]
    version: int | None
    build_ms: float


def inner(value: object, cls: type, name: str) -> Any:
    """Return the compiled object behind `value`, or raise `TypeError` naming `name`."""
    if not isinstance(value, cls):
        raise TypeError(f"{name} must be {cls.__name__}, not {type(value).__name__}")
    if isinstance(value, Enum):
        # The compiled enums have the same member names.
        return getattr(getattr(_graph, cls.__name__), value.name)
    return value._inner  # pyright: ignore[reportAttributeAccessIssue]
