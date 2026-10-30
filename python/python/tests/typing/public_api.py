"""Strict static consumer; checked by make lint, not executed by pytest."""

from datetime import date
from typing import TYPE_CHECKING, TypeAlias, assert_type

from lance import LanceDataset
from lance_csr import (
    CypherEngine,
    CypherQuery,
    DirNamespace,
    DistanceMetric,
    ExecutionStrategy,
    GraphConfig,
    GraphConfigBuilder,
    SqlDialect,
    SqlEngine,
    SqlQuery,
    VectorSearch,
    __version__,
)
from pyarrow import RecordBatch, RecordBatchReader, Table

if TYPE_CHECKING:
    from collections.abc import Mapping

ParameterResult: TypeAlias = (
    int
    | float
    | str
    | bool
    | None
    | list["ParameterResult"]
    | dict[str, "ParameterResult"]
)


def public_api(
    table: Table, batch: RecordBatch, reader: RecordBatchReader, dataset: LanceDataset
) -> None:
    assert_type(__version__, str)
    builder = GraphConfig.builder().node_table("Person", "id")
    assert_type(builder, GraphConfigBuilder)
    builder = builder.rel_table("KNOWS", "Person", "Person", "src", "dst")
    config = builder.build()
    assert_type(config, GraphConfig)
    assert_type(config.node_labels(), list[str])
    assert_type(config.relationship_types(), list[str])
    assert_type(GraphConfigBuilder().build(), GraphConfig)

    datasets = {"Person": table}
    engine = CypherEngine(config, datasets, use_csr=True)
    query = "MATCH (p:Person) WHERE p.id = $id RETURN p.id"
    params = {"id": 1}
    assert_type(engine.execute(query, params), Table)
    assert_type(engine.execute_batches(query, params), RecordBatchReader)
    assert_type(engine.explain(query, params), str)
    assert_type(engine.refresh(), None)
    assert_type(engine.config(), GraphConfig)
    assert_type(engine.dataset_versions(), dict[str, int])
    assert_type(engine.node_index_stats()["person"]["rows"], int)
    assert_type(engine.node_index_stats()["person"]["build_ms"], float)
    assert_type(engine.csr_stats()["knows"]["edges"], int)
    assert_type(engine.csr_stats()["knows"]["version"], int | None)
    CypherEngine(config, {"Person": batch})
    CypherEngine(config, {"Person": reader})
    CypherEngine(config, {"Person": dataset})
    CypherEngine(config, datasets, target_partitions=1)
    CypherEngine(config, datasets, target_partitions=None)

    one = CypherQuery(query).with_config(config).with_parameter("id", 1)
    assert_type(one, CypherQuery)
    assert_type(one.with_parameter("ids", [1, 2]), CypherQuery)
    assert_type(engine.execute(query, timeout=1.0), Table)
    assert_type(engine.execute_batches(query, timeout=1.0), RecordBatchReader)
    assert_type(one.execute(datasets, timeout=1.0), Table)
    one.with_parameter("day", date(2026, 1, 1))
    assert_type(one.query_text(), str)
    parameters: Mapping[str, ParameterResult] = one.parameters()
    assert_type(len(parameters), int)
    assert_type(one.variables(), list[str])
    assert_type(one.node_labels(), list[str])
    assert_type(one.relationship_types(), list[str])
    assert_type(one.execute(datasets, ExecutionStrategy.DataFusion), Table)
    assert_type(one.explain(datasets), str)
    assert_type(one.to_sql(datasets, SqlDialect.PostgreSql), str)
    namespace = DirNamespace("/tmp/graph")
    assert_type(namespace.base_uri, str)
    assert_type(one.execute_with_namespace(namespace), Table)

    vector = (
        VectorSearch("p.embedding")
        .query_vector([1.0, 0.0])
        .metric(DistanceMetric.Cosine)
        .top_k(5)
        .include_distance(True)
        .distance_column_name("distance")
        .use_lance_index(False)
    )
    assert_type(vector, VectorSearch)
    assert_type(vector.search(table), Table)
    assert_type(one.execute_with_vector_rerank(datasets, vector), Table)
    assert_type(engine.execute_with_vector_rerank(query, vector), Table)
    sql = SqlQuery("SELECT id FROM Person")
    assert_type(sql.sql(), str)
    assert_type(sql.execute(datasets), Table)
    assert_type(sql.explain(datasets), str)
    assert_type(SqlEngine(datasets).execute(sql.sql()), Table)

    # Rejected calls are checked too: removing a type restriction makes these unused.
    CypherEngine(config, datasets, target_partitions="1")  # pyright: ignore[reportArgumentType]
    CypherEngine(config, datasets, False, 1)  # pyright: ignore[reportCallIssue]
    engine.execute(query, {"id": object()})  # pyright: ignore[reportArgumentType]
    vector.top_k("5")  # pyright: ignore[reportArgumentType]
    one.with_parameter("ids", {1, 2})  # pyright: ignore[reportArgumentType]
    namespace.base_uri = "elsewhere"  # pyright: ignore[reportAttributeAccessIssue]
