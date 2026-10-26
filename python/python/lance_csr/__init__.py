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

GraphConfig = _bindings.graph.GraphConfig
GraphConfigBuilder = _bindings.graph.GraphConfigBuilder
CypherQuery = _bindings.graph.CypherQuery
CypherEngine = _bindings.graph.CypherEngine
SqlQuery = _bindings.graph.SqlQuery
SqlEngine = _bindings.graph.SqlEngine
ExecutionStrategy = _bindings.graph.ExecutionStrategy
SqlDialect = _bindings.graph.SqlDialect
VectorSearch = _bindings.graph.VectorSearch
DistanceMetric = _bindings.graph.DistanceMetric

DirNamespace = _bindings.graph.DirNamespace

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

__version__ = _bindings.__version__
