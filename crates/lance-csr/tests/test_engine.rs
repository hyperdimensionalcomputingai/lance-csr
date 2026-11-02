use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_csr::{CypherEngine, EngineSource, GraphConfig, GraphError};

fn config() -> GraphConfig {
    GraphConfig::builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
        .build()
        .unwrap()
}

fn people() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David"])),
        ],
    )
    .unwrap()
}

fn knows(edges: &[(i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.0))),
            Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.1))),
        ],
    )
    .unwrap()
}

fn names(batch: &RecordBatch) -> Vec<String> {
    let column = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..column.len())
        .map(|i| column.value(i).to_string())
        .collect()
}

async fn write(batch: RecordBatch, uri: &str, mode: WriteMode) {
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap();
}

const TWO_HOPS: &str =
    "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c:Person) RETURN c.name AS name ORDER BY name";

#[tokio::test]
async fn arrow_input_with_csr() {
    let sources = HashMap::from([
        ("Person".to_string(), EngineSource::Arrow(people())),
        (
            "KNOWS".to_string(),
            EngineSource::Arrow(knows(&[(1, 2), (2, 3), (2, 4)])),
        ),
    ]);
    let engine = CypherEngine::new(config(), sources, true).await.unwrap();

    let batch = engine.execute(TWO_HOPS, HashMap::new()).await.unwrap();
    assert_eq!(names(&batch), ["Carol", "David"]);

    let plan = engine.explain(TWO_HOPS, HashMap::new()).await.unwrap();
    assert!(plan.contains("CsrExpandExec"), "{plan}");

    let csr = &engine.csr_stats()["KNOWS"];
    assert_eq!((csr.edges, csr.update, csr.version), (3, "build", None));
    assert_eq!(engine.node_index_stats()["Person"].rows, 4);
    assert!(engine.dataset_versions().is_empty());
}

#[tokio::test]
async fn parameters_and_streaming() {
    let sources = HashMap::from([
        ("Person".to_string(), EngineSource::Arrow(people())),
        ("KNOWS".to_string(), EngineSource::Arrow(knows(&[(1, 2)]))),
    ]);
    let engine = CypherEngine::new(config(), sources, false).await.unwrap();
    let query = "MATCH (p:Person) WHERE p.id >= $min RETURN p.name AS name ORDER BY name";
    let params = HashMap::from([("min".to_string(), serde_json::json!(3))]);

    let batches: Vec<RecordBatch> = engine
        .execute_stream(query, params)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let streamed: Vec<String> = batches.iter().flat_map(names).collect();
    assert_eq!(streamed, ["Carol", "David"]);
    assert!(engine.csr_stats().is_empty());
}

#[tokio::test]
async fn lance_input_is_pinned_until_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let person_uri = dir.path().join("person.lance");
    let knows_uri = dir.path().join("knows.lance");
    let (person_uri, knows_uri) = (person_uri.to_str().unwrap(), knows_uri.to_str().unwrap());
    write(people(), person_uri, WriteMode::Create).await;
    write(knows(&[(1, 2), (2, 3)]), knows_uri, WriteMode::Create).await;

    let sources = HashMap::from([
        (
            "Person".to_string(),
            EngineSource::open_lance(person_uri, None, HashMap::new())
                .await
                .unwrap(),
        ),
        (
            "KNOWS".to_string(),
            EngineSource::open_lance(knows_uri, None, HashMap::new())
                .await
                .unwrap(),
        ),
    ]);
    let engine = CypherEngine::new(config(), sources, true).await.unwrap();
    assert_eq!(
        names(&engine.execute(TWO_HOPS, HashMap::new()).await.unwrap()),
        ["Carol"]
    );

    // A write after construction stays invisible until refresh().
    write(knows(&[(2, 4)]), knows_uri, WriteMode::Append).await;
    assert_eq!(
        names(&engine.execute(TWO_HOPS, HashMap::new()).await.unwrap()),
        ["Carol"]
    );

    engine.refresh().await.unwrap();
    assert_eq!(
        names(&engine.execute(TWO_HOPS, HashMap::new()).await.unwrap()),
        ["Carol", "David"]
    );
    let csr = &engine.csr_stats()["KNOWS"];
    assert_eq!((csr.edges, csr.version), (3, Some(2)));
    assert_eq!(engine.dataset_versions()["Person"], 1);
}

#[tokio::test]
async fn optimize_csr_folds_the_delta_into_the_base() {
    let dir = tempfile::tempdir().unwrap();
    let person_uri = dir.path().join("person.lance");
    let knows_uri = dir.path().join("knows.lance");
    let (person_uri, knows_uri) = (person_uri.to_str().unwrap(), knows_uri.to_str().unwrap());
    write(people(), person_uri, WriteMode::Create).await;
    // A base large enough that one appended edge stays under the 25% rebuild threshold.
    let base: Vec<_> = [(1, 2), (2, 3)].into_iter().chain([(3, 4); 8]).collect();
    write(knows(&base), knows_uri, WriteMode::Create).await;
    let mut sources = HashMap::new();
    for (name, uri) in [("Person", person_uri), ("KNOWS", knows_uri)] {
        let source = EngineSource::open_lance(uri, None, HashMap::new());
        sources.insert(name.to_string(), source.await.unwrap());
    }
    let engine = CypherEngine::new(config(), sources, true).await.unwrap();

    write(knows(&[(2, 4)]), knows_uri, WriteMode::Append).await;
    engine.refresh().await.unwrap();
    let csr = engine.csr_stats()["KNOWS"].clone();
    assert_eq!(
        (csr.update, csr.delta_edges, csr.unindexed_edges),
        ("delta", 1, 1)
    );
    assert_eq!((csr.version, csr.base_version), (Some(2), Some(1)));

    engine.optimize_csr().await.unwrap();
    let csr = engine.csr_stats()["KNOWS"].clone();
    assert_eq!((csr.update, csr.edges, csr.delta_edges), ("rebuild", 11, 0));
    assert_eq!((csr.tombstones, csr.unindexed_edges), (0, 0));
    assert_eq!((csr.version, csr.base_version), (Some(2), Some(2)));
    assert_eq!(
        names(&engine.execute(TWO_HOPS, HashMap::new()).await.unwrap()),
        ["Carol", "David"]
    );

    // Nothing left to fold: the CSR is reused as it is.
    engine.optimize_csr().await.unwrap();
    assert_eq!(engine.csr_stats()["KNOWS"].build_ms, csr.build_ms);
}

#[tokio::test]
async fn construction_errors_are_config_errors() {
    let empty = CypherEngine::new(config(), HashMap::new(), false).await;
    assert!(matches!(empty, Err(GraphError::ConfigError { .. })));

    let sources = HashMap::from([
        ("Person".to_string(), EngineSource::Arrow(people())),
        ("KNOWS".to_string(), EngineSource::Arrow(people())),
    ]);
    let missing = CypherEngine::new(config(), sources, false).await;
    match missing {
        Err(GraphError::ConfigError { message, .. }) => assert!(message.contains("src_id")),
        other => panic!("expected a ConfigError, got {:?}", other.err()),
    }
}

/// A pattern predicate in an OPTIONAL MATCH's WHERE may read a variable bound earlier
/// (`person`) that its own pattern doesn't mention; each row counts its own person's tags.
#[tokio::test]
async fn optional_match_where_pattern_reads_an_earlier_variable() {
    let config = GraphConfig::builder()
        .node_table("Person", "id")
        .node_table("Post", "id")
        .node_table("Tag", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .rel_table("CREATED", "Post", "Person", "src", "dst")
        .rel_table("HAS_TAG", "Post", "Tag", "src", "dst")
        .rel_table("INTEREST", "Person", "Tag", "src", "dst")
        .build()
        .unwrap();
    let ids = |ids: Vec<i64>| {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap()
    };
    let edges = |pairs: &[(i64, i64)]| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(pairs.iter().map(|p| p.0))),
                Arc::new(Int64Array::from_iter_values(pairs.iter().map(|p| p.1))),
            ],
        )
        .unwrap()
    };
    // Person 1 is interested in tags 100 and 101, person 5 in 102. Post 12 matches
    // person 1 through two tags but counts once; friend 4 has no posts.
    let sources = [
        ("Person", ids(vec![1, 2, 3, 4, 5])),
        ("Post", ids(vec![10, 11, 12, 13])),
        ("Tag", ids(vec![100, 101, 102])),
        ("KNOWS", edges(&[(1, 2), (1, 3), (1, 4), (5, 2)])),
        ("CREATED", edges(&[(10, 2), (11, 2), (12, 3), (13, 3)])),
        (
            "HAS_TAG",
            edges(&[(10, 100), (10, 102), (11, 102), (12, 100), (12, 101)]),
        ),
        ("INTEREST", edges(&[(1, 100), (1, 101), (5, 102)])),
    ];
    let query = "MATCH (person:Person)-[:KNOWS]->(friend:Person) \
                 WITH DISTINCT friend, person \
                 OPTIONAL MATCH (friend)<-[:CREATED]-(post:Post) \
                 WHERE (post)-[:HAS_TAG]->(:Tag)<-[:INTEREST]-(person) \
                 RETURN person.id AS p, friend.id AS f, count(post) AS n ORDER BY p, f";
    for use_csr in [false, true] {
        let sources = sources
            .iter()
            .map(|(name, batch)| (name.to_string(), EngineSource::Arrow(batch.clone())))
            .collect();
        let engine = CypherEngine::new(config.clone(), sources, use_csr)
            .await
            .unwrap();
        let result = engine.execute(query, HashMap::new()).await.unwrap();
        let column = |i: usize| {
            let c = result.column(i).as_any().downcast_ref::<Int64Array>();
            c.unwrap().values().to_vec()
        };
        assert_eq!(column(0), [1, 1, 1, 5], "use_csr={use_csr}");
        assert_eq!(column(1), [2, 3, 4, 2], "use_csr={use_csr}");
        assert_eq!(column(2), [1, 1, 0, 2], "use_csr={use_csr}");
    }
}
