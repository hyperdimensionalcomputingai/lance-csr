"""The seams between the public Python classes and the compiled ones they wrap."""

import pyarrow as pa
import pytest
from lance_csr import (
    CypherEngine,
    CypherQuery,
    DistanceMetric,
    ExecutionStrategy,
    GraphConfig,
    GraphConfigBuilder,
    SqlDialect,
    VectorSearch,
)

DATASETS = {
    "Person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}),
    "KNOWS": pa.table({"src": [1], "dst": [2]}),
}


def config():
    return (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )


def test_builders_return_new_wrappers():
    empty = GraphConfigBuilder()
    with_person = empty.node_table("Person", "id")
    assert type(with_person) is GraphConfigBuilder and with_person is not empty
    assert empty.build().node_labels() == []
    assert with_person.build().node_labels() == ["person"]

    search = VectorSearch("embedding")
    assert type(search.top_k(1)) is VectorSearch
    query = CypherQuery("RETURN $x AS x")
    bound = query.with_parameter("x", 1)
    assert type(bound) is CypherQuery and query.parameters() == {}


def test_engine_config_is_a_public_graph_config():
    engine = CypherEngine(config(), DATASETS)
    assert type(engine.config()) is GraphConfig
    assert sorted(engine.config().relationship_types()) == ["knows"]
    query = CypherQuery("MATCH (p:Person) RETURN p.name ORDER BY p.name")
    rows = query.with_config(engine.config()).execute(DATASETS).to_pylist()
    assert rows == [{"p.name": "Alice"}, {"p.name": "Bob"}]


def test_graph_config_has_no_constructor():
    with pytest.raises(TypeError, match="builder"):
        GraphConfig()


@pytest.mark.parametrize(
    "call",
    [
        lambda: CypherEngine("config", DATASETS),
        lambda: CypherQuery("RETURN 1").with_config(object()),
        lambda: CypherEngine(config(), DATASETS).execute_with_vector_rerank(
            "RETURN 1", "search"
        ),
        lambda: CypherQuery("RETURN 1").execute(DATASETS, strategy=0),
        lambda: VectorSearch("v").metric(1),
    ],
)
def test_wrong_argument_type_raises_type_error(call):
    with pytest.raises(TypeError, match="must be"):
        call()


def test_enums_reach_the_compiled_calls():
    query = CypherQuery("MATCH (p:Person) RETURN p.name").with_config(config())
    assert query.execute(DATASETS, ExecutionStrategy.DataFusion).num_rows == 2
    with pytest.raises(NotImplementedError):
        query.execute(DATASETS, ExecutionStrategy.LanceNative)
    assert "SELECT" in query.to_sql(DATASETS, SqlDialect.PostgreSql)
    table = pa.table({"id": [1, 2], "v": [[1.0, 0.0], [0.0, 1.0]]})
    search = VectorSearch("v").query_vector([0.0, 1.0]).top_k(1)
    for metric in DistanceMetric:
        assert search.metric(metric).search(table)["id"].to_pylist() == [2]
    assert int(SqlDialect.Sqlite) == 4


def test_deprecation_warning_points_at_the_caller():
    with pytest.warns(DeprecationWarning) as record:
        GraphConfig.builder().with_node_label("Person", "id")
    assert record[0].filename == __file__
