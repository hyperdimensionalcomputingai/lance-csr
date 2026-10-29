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
async fn lance_input_is_pinned_until_checkout_latest() {
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

    // A write after construction stays invisible until checkout_latest().
    write(knows(&[(2, 4)]), knows_uri, WriteMode::Append).await;
    assert_eq!(
        names(&engine.execute(TWO_HOPS, HashMap::new()).await.unwrap()),
        ["Carol"]
    );

    engine.checkout_latest().await.unwrap();
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
    engine.checkout_latest().await.unwrap();
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

/// Pattern predicates walked on the CSR (`PathExists`) give the same rows as the
/// relational marking the engine plans without a CSR.
#[tokio::test]
async fn path_exists_matches_the_join_plan() {
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
    let edges = |pairs: Vec<(i64, i64)>| {
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
    // Persons 1-8 with a self-loop, a duplicate and mutual edges; person 8 knows nobody.
    // Edges to missing nodes (99, 299, 98) match nothing, as nodes join to their tables.
    let knows = vec![
        (1, 2),
        (2, 1),
        (1, 3),
        (3, 3),
        (3, 4),
        (3, 4),
        (4, 5),
        (5, 6),
        (6, 1),
        (7, 2),
        (8, 99),
    ];
    let created: Vec<_> = (100..116).map(|p| (p, 1 + p % 7)).collect();
    // Posts 112-115 have no tags.
    let has_tag: Vec<_> = (100..112)
        .flat_map(|p| [(p, 200 + p % 5), (p, 200 + p % 3)])
        .chain([(113, 299)])
        .collect();
    let interest = vec![
        (1, 200),
        (2, 201),
        (2, 204),
        (3, 202),
        (5, 203),
        (6, 200),
        (6, 204),
        (98, 203),
    ];
    let sources = [
        ("Person", ids((1..=8).collect())),
        ("Post", ids((100..116).collect())),
        ("Tag", ids((200..205).collect())),
        ("KNOWS", edges(knows)),
        ("CREATED", edges(created)),
        ("HAS_TAG", edges(has_tag)),
        ("INTEREST", edges(interest)),
    ];
    let queries = [
        // (query, walked on the CSR)
        (
            "MATCH (a:Person) WHERE (a)-[:KNOWS]->(:Person) RETURN a.id AS a",
            true,
        ),
        (
            "MATCH (a:Person) WHERE NOT (a)<-[:KNOWS]-() RETURN a.id AS a",
            true,
        ),
        (
            "MATCH (a:Person), (b:Person) WHERE a.id < b.id AND NOT (a)-[:KNOWS]-(b) \
             RETURN a.id AS a, b.id AS b",
            true,
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE (a)<-[:KNOWS]-(b) \
             RETURN a.id AS a, b.id AS b",
            true,
        ),
        (
            "MATCH (p:Post), (person:Person) \
             WHERE (p)-[:HAS_TAG]->(:Tag)<-[:INTEREST]-(person) RETURN p.id AS p, person.id AS q",
            true,
        ),
        (
            "MATCH (person:Person)-[:KNOWS]->(friend:Person) WITH DISTINCT friend, person \
             OPTIONAL MATCH (friend)<-[:CREATED]-(post:Post) \
             WITH friend, person, collect(post) AS posts \
             RETURN person.id AS p, friend.id AS f, size(posts) AS n, \
             size([x IN posts WHERE (x)-[:HAS_TAG]->()<-[:INTEREST]-(person)]) AS c",
            true,
        ),
        (
            "MATCH (person:Person)-[:KNOWS]->(friend:Person) WITH DISTINCT friend, person \
             OPTIONAL MATCH (friend)<-[:CREATED]-(post:Post) \
             WITH friend, person, post, \
             CASE WHEN (post)-[:HAS_TAG]->()<-[:INTEREST]-(person) THEN 1 END AS common \
             RETURN person.id AS p, friend.id AS f, count(post) AS n, count(common) AS c",
            true,
        ),
        (
            "MATCH (person:Person)-[:KNOWS]->(friend:Person) WITH DISTINCT friend, person \
             OPTIONAL MATCH (friend)<-[:CREATED]-(post:Post) \
             WHERE (post)-[:HAS_TAG]->(:Tag)<-[:INTEREST]-(person) \
             RETURN person.id AS p, friend.id AS f, count(post) AS n",
            true,
        ),
        // Not walked: one type twice, a named middle node, a WHERE inside the pattern.
        (
            "MATCH (a:Person) WHERE (a)-[:KNOWS]->()-[:KNOWS]->(:Person) RETURN a.id AS a",
            false,
        ),
        (
            "MATCH (a:Person) WHERE (a)<-[:CREATED]-(m:Post)-[:HAS_TAG]->(:Tag) RETURN a.id AS a",
            false,
        ),
    ];
    let mut engines = Vec::new();
    for use_csr in [false, true] {
        let sources = sources
            .iter()
            .map(|(name, batch)| (name.to_string(), EngineSource::Arrow(batch.clone())))
            .collect();
        engines.push(
            CypherEngine::new(config.clone(), sources, use_csr)
                .await
                .unwrap(),
        );
    }
    for (query, walked) in queries {
        let mut rows = Vec::new();
        for engine in &engines {
            let batch = engine.execute(query, HashMap::new()).await.unwrap();
            let mut lines: Vec<String> = arrow::util::pretty::pretty_format_batches(&[batch])
                .unwrap()
                .to_string()
                .lines()
                .map(str::to_string)
                .collect();
            lines.sort();
            rows.push(lines);
        }
        assert_eq!(rows[0], rows[1], "{query}");
        assert!(rows[0].len() > 4, "no rows from {query}");
        let plan = engines[1].explain(query, HashMap::new()).await.unwrap();
        assert_eq!(plan.contains("PathExistsExec"), walked, "{query}\n{plan}");
        let plan = engines[0].explain(query, HashMap::new()).await.unwrap();
        assert!(!plan.contains("PathExistsExec"), "{query}\n{plan}");
    }
}

#[tokio::test]
async fn anonymous_patterns_keep_mixed_label_identity_and_query_semantics() {
    use arrow_array::{cast::AsArray, StructArray};
    for use_csr in [false, true] {
        let project = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef,
            ),
            (
                "title",
                Arc::new(StringArray::from(vec!["Project one"])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .node_table("Project", "id")
            .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
            .rel_table("WORKS_ON", "Person", "Project", "src_id", "dst_id")
            .build()
            .unwrap();
        let engine = CypherEngine::new(
            cfg,
            HashMap::from([
                ("Person".into(), EngineSource::Arrow(people())),
                ("Project".into(), EngineSource::Arrow(project)),
                (
                    "KNOWS".into(),
                    EngineSource::Arrow(knows(&[(1, 2), (1, 2), (1, 1)])),
                ),
                ("WORKS_ON".into(), EngineSource::Arrow(knows(&[(1, 1)]))),
            ]),
            use_csr,
        )
        .await
        .unwrap();
        for query in [
            "MATCH (a)-[]-(b) RETURN a,b",
            "MATCH (a)-[r]-(b) RETURN a,r,b",
            "MATCH p = (a)-[]-(b) RETURN p",
        ] {
            let batch = engine.execute(query, HashMap::new()).await.unwrap();
            assert_eq!(batch.num_rows(), 7, "{query}");
            if let Some(column) = batch.column_by_name("a") {
                let entity = column.as_any().downcast_ref::<StructArray>().unwrap();
                let identity = entity
                    .column_by_name("__lance_csr_identity")
                    .unwrap()
                    .as_struct();
                let labels = identity.column_by_name("label").unwrap().as_string::<i32>();
                assert!((0..labels.len()).any(|row| labels.value(row) == "Person"));
                assert!((0..labels.len()).any(|row| labels.value(row) == "Project"));
            }
        }
        for (query, expected) in [
            ("MATCH (a)-[]-(b) RETURN count(*) AS total", 7),
            (
                "MATCH (a)-[]-(b) WHERE a.name = 'Alice' RETURN count(*) AS total",
                4,
            ),
        ] {
            let batch = engine.execute(query, HashMap::new()).await.unwrap();
            assert_eq!(
                batch
                    .column_by_name("total")
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(0),
                expected
            );
        }
        let alias = engine
            .execute(
                "MATCH (a)-[]-(b) WITH a AS friend RETURN friend",
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(alias.num_rows(), 7);
        assert!(matches!(
            alias.schema().field(0).data_type(),
            DataType::Struct(_)
        ));
        let limit = engine
            .execute(
                "MATCH p = (a)-[]-(b) RETURN p SKIP 2 LIMIT 3",
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(limit.num_rows(), 3);
    }
}
