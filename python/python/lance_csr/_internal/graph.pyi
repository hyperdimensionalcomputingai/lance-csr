from collections.abc import Sequence
from datetime import date, datetime
from typing import ClassVar, Literal, Protocol, Self, TypedDict, TypeVar, final

from lance import LanceDataset
from pyarrow import RecordBatch, RecordBatchReader, Table

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
    update: Literal["build", "rebuild", "delta", "reused"]
    version: int | None
    build_ms: float

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
    def with_node_label(self, label: str, id_field: str) -> Self: ...
    def with_relationship(
        self,
        rel_type: str,
        source_field: str,
        target_field: str,
        source_label: str | None = None,
        target_label: str | None = None,
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
        self, config: GraphConfig, datasets: dict[str, _DatasetT], use_csr: bool = False
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
        self, query: str, params: dict[str, _ParameterT] | None = None
    ) -> str: ...
    def refresh(self) -> None: ...
    def node_index_stats(self) -> dict[str, _NodeIndexStats]: ...
    def csr_stats(self) -> dict[str, _CsrStats]: ...
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
    def __init__(self, datasets: dict[str, _DatasetT]) -> None: ...
    def execute(self, sql: str) -> Table: ...

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
class DirNamespace:
    def __init__(self, base_uri: str) -> None: ...
    @property
    def base_uri(self) -> str: ...
