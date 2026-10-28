"""Python bindings for the ``lance-csr`` crate.

Provides two query interfaces:

- **Cypher**: ``CypherQuery`` and ``CypherEngine`` for graph-pattern queries
  (requires a ``GraphConfig`` with node/relationship mappings).
- **SQL**: ``SqlQuery`` and ``SqlEngine`` for standard SQL queries executed
  directly against datasets via DataFusion (no ``GraphConfig`` needed).
"""

from __future__ import annotations

try:
    from . import _internal as _bindings
except ImportError as e:
    raise ImportError(
        "Unable to import the compiled lance-csr extension. "
        "Run `maturin develop` (or install the package) before importing."
    ) from e

from .config import GraphConfig, GraphConfigBuilder
from .cypher import CypherEngine, CypherQuery, ExecutionStrategy
from .namespace import DirNamespace
from .sql import SqlDialect, SqlEngine, SqlQuery
from .vector import DistanceMetric, VectorSearch

__all__ = [
    "GraphConfig",
    "GraphConfigBuilder",
    "CypherQuery",
    "CypherEngine",
    "SqlQuery",
    "SqlEngine",
    "ExecutionStrategy",
    "SqlDialect",
    "VectorSearch",
    "DistanceMetric",
    "DirNamespace",
]

__version__: str = _bindings.__version__
