from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Literal, Protocol, Self, TypedDict, TypeVar, final

from lance import LanceDataset
from pyarrow import (
    Array,
    Int64Array,
    RecordBatch,
    RecordBatchReader,
    Table,
    UInt64Array,
)

class _ArrowStream(Protocol):
    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...

_Dataset = Table | RecordBatch | RecordBatchReader | LanceDataset | _ArrowStream
_DatasetT = TypeVar("_DatasetT", bound=_Dataset)
_Parameter = (
    int
    | float
    | str
    | bool
    | date
    | datetime
    | None
    | list["_Parameter"]
    | dict[str, "_Parameter"]
)
_ParameterT = TypeVar("_ParameterT", bound=_Parameter)
_Scalar = int | float | str | bool | None | list["_Scalar"] | dict[str, "_Scalar"]

class _NodeIndexStats(TypedDict):
    rows: int
    version: int | None
    build_ms: float

class _CsrStats(TypedDict):
    edges: int
    delta_edges: int
    tombstones: int
    unindexed_edges: int
    update: Literal["build", "rebuild", "loaded", "delta", "reused"]
    version: int | None
    base_version: int | None
    build_ms: float
    store_error: str | None

class _DatasetOptimizeStats(TypedDict):
    fragments_removed: int
    fragments_added: int
    old_versions: int
    bytes_removed: int

@final
class ExecutionStrategy:
    DataFusion: ClassVar[ExecutionStrategy]
    LanceNative: ClassVar[ExecutionStrategy]
    def __int__(self) -> int: ...

@final
class SqlDialect:
    Default: ClassVar[SqlDialect]
    Spark: ClassVar[SqlDialect]
    PostgreSql: ClassVar[SqlDialect]
    MySql: ClassVar[SqlDialect]
    Sqlite: ClassVar[SqlDialect]
    def __int__(self) -> int: ...

@final
class DistanceMetric:
    L2: ClassVar[DistanceMetric]
    Cosine: ClassVar[DistanceMetric]
    Dot: ClassVar[DistanceMetric]
    def __int__(self) -> int: ...

@final
class GraphConfig:
    @staticmethod
    def builder() -> GraphConfigBuilder: ...
    def node_labels(self) -> list[str]: ...
    def relationship_types(self) -> list[str]: ...

@final
class GraphConfigBuilder:
    def __init__(self) -> None: ...
    def node_table(self, label: str, primary_key: str) -> Self: ...
    def rel_table(
        self, rel_type: str, from_table: str, to_table: str, src: str, dst: str
    ) -> Self: ...
    def build(self) -> GraphConfig: ...

@final
class CypherQuery:
    def __init__(self, query_text: str) -> None: ...
    def with_config(self, config: GraphConfig) -> Self: ...
    def with_parameter(self, key: str, value: _Parameter) -> Self: ...
    def query_text(self) -> str: ...
    def parameters(self) -> dict[str, _Scalar]: ...
    def to_sql(
        self, datasets: dict[str, _DatasetT], dialect: SqlDialect | None = None
    ) -> str: ...
    def execute(
        self,
        datasets: dict[str, _DatasetT],
        strategy: ExecutionStrategy | None = None,
        *,
        timeout: float | None = None,
    ) -> Table: ...
    def execute_with_namespace(
        self, namespace: DirNamespace, strategy: ExecutionStrategy | None = None
    ) -> Table: ...
    def explain(self, datasets: dict[str, _DatasetT]) -> str: ...
    def variables(self) -> list[str]: ...
    def node_labels(self) -> list[str]: ...
    def relationship_types(self) -> list[str]: ...
    def execute_with_vector_rerank(
        self, datasets: dict[str, _DatasetT], vector_search: VectorSearch
    ) -> Table: ...

@final
class CypherEngine:
    def __init__(
        self,
        config: GraphConfig,
        datasets: dict[str, _DatasetT],
        use_csr: bool = False,
        *,
        target_partitions: int | None = None,
        persist_csr: bool | Literal["load"] = False,
    ) -> None: ...
    def execute(
        self,
        query: str,
        params: dict[str, _ParameterT] | None = None,
        *,
        timeout: float | None = None,
    ) -> Table: ...
    def execute_batches(
        self,
        query: str,
        params: dict[str, _ParameterT] | None = None,
        *,
        timeout: float | None = None,
    ) -> RecordBatchReader: ...
    def explain(
        self,
        query: str,
        params: dict[str, _ParameterT] | None = None,
        *,
        analyze: bool = False,
    ) -> str: ...
    def checkout_latest(self) -> None: ...
    def optimize_csr(self) -> None: ...
    def delete_stored_csr(self) -> int: ...
    def node_index_stats(self) -> dict[str, _NodeIndexStats]: ...
    def csr_stats(self) -> dict[str, _CsrStats]: ...
    def optimize_datasets(
        self, *, cleanup_older_than: timedelta | None = None
    ) -> dict[str, _DatasetOptimizeStats]: ...
    def export_graph(
        self, rel_types: list[str] | None = None, *, directed: bool = True
    ) -> GraphExport: ...
    def write_node_column(
        self,
        export: GraphExport,
        column: str,
        values: Array[Any],
        *,
        overwrite: bool = False,
    ) -> None: ...
    def dataset_versions(self) -> dict[str, int]: ...
    def config(self) -> GraphConfig: ...
    def execute_with_vector_rerank(
        self, query: str, vector_search: VectorSearch
    ) -> Table: ...

@final
class SqlQuery:
    def __init__(self, sql: str) -> None: ...
    def sql(self) -> str: ...
    def execute(self, datasets: dict[str, _DatasetT]) -> Table: ...
    def explain(self, datasets: dict[str, _DatasetT]) -> str: ...

@final
class SqlEngine:
    def __init__(
        self,
        datasets: dict[str, _DatasetT],
        config: GraphConfig | None = None,
        use_csr: bool = False,
    ) -> None: ...
    @staticmethod
    def from_engine(engine: CypherEngine) -> SqlEngine: ...
    def execute(
        self,
        sql: str,
        params: dict[str, _ParameterT] | None = None,
        *,
        timeout: float | None = None,
    ) -> Table: ...
    def explain(
        self,
        sql: str,
        params: dict[str, _ParameterT] | None = None,
        *,
        analyze: bool = False,
    ) -> str: ...
    def checkout_latest(self) -> None: ...

@final
class VectorSearch:
    def __init__(self, column: str) -> None: ...
    def query_vector(self, vector: Sequence[float]) -> Self: ...
    def metric(self, metric: DistanceMetric) -> Self: ...
    def top_k(self, k: int) -> Self: ...
    def include_distance(self, include: bool) -> Self: ...
    def distance_column_name(self, name: str) -> Self: ...
    def use_lance_index(self, enabled: bool) -> Self: ...
    def search(
        self, table: Table | RecordBatch | RecordBatchReader | _ArrowStream
    ) -> Table: ...

@final
class GraphExport:
    @property
    def num_nodes(self) -> int: ...
    @property
    def directed(self) -> bool: ...
    @property
    def node_keys(self) -> Int64Array: ...
    @property
    def label_ranges(self) -> dict[str, tuple[int, int]]: ...
    @property
    def out_indptr(self) -> UInt64Array: ...
    @property
    def out_indices(self) -> UInt64Array: ...
    @property
    def in_indptr(self) -> UInt64Array | None: ...
    @property
    def in_indices(self) -> UInt64Array | None: ...
    @property
    def dropped_edges(self) -> int: ...

@final
class DirNamespace:
    def __init__(self, base_uri: str) -> None: ...
    @property
    def base_uri(self) -> str: ...
