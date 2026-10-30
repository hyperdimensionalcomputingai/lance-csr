//! Run Cypher over Lance datasets from Rust, with no Python.
//!
//! Writes a small social graph as two Lance datasets in a temporary directory, opens
//! them in a CSR-backed engine, runs a two-hop query, appends an edge and picks it up
//! with `refresh()`, runs a query under a deadline, and repeats the in-memory Arrow
//! examples of the home page and the getting started guide. The documentation includes
//! this file section by section (the `--8<--` markers), so keep it compiling and its printed
//! output in step.
//!
//! ```bash
//! cargo run -p lance-csr --example embedded_query
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::util::pretty::print_batches;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_csr::query::with_execution_deadline;
use lance_csr::{CypherEngine, CypherQuery, EngineSource, GraphConfig};
use tokio::time::{timeout_at, Instant};

fn knows(src: Vec<i64>, dst: Vec<i64>) -> lance_csr::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let columns = vec![
        Arc::new(Int64Array::from(src)) as _,
        Arc::new(Int64Array::from(dst)) as _,
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

async fn write_lance(batch: RecordBatch, uri: &str, mode: WriteMode) -> lance_csr::Result<()> {
    let schema = batch.schema();
    let params = WriteParams {
        mode,
        ..Default::default()
    };
    Dataset::write(
        RecordBatchIterator::new([Ok(batch)], schema),
        uri,
        Some(params),
    )
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> lance_csr::Result<()> {
    let dir = tempfile::tempdir().expect("create a temporary directory");
    let person_uri = dir.path().join("person.lance").display().to_string();
    let knows_uri = dir.path().join("knows.lance").display().to_string();

    let person = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David"])),
        ],
    )?;
    write_lance(person, &person_uri, WriteMode::Create).await?;
    // Alice knows Bob; Bob knows Carol and David; Carol knows David.
    write_lance(
        knows(vec![1, 2, 2, 3], vec![2, 3, 4, 4])?,
        &knows_uri,
        WriteMode::Create,
    )
    .await?;

    // --8<-- [start:engine]
    let config = GraphConfig::builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
        .build()?;
    // Each dataset is pinned to the version opened here until `engine.refresh()`.
    let sources = HashMap::from([
        (
            "Person".to_string(),
            EngineSource::open_lance(person_uri, None, HashMap::new()).await?,
        ),
        (
            "KNOWS".to_string(),
            EngineSource::open_lance(knows_uri.clone(), None, HashMap::new()).await?,
        ),
    ]);
    let engine = CypherEngine::new(config, sources, true).await?;
    // --8<-- [end:engine]

    // --8<-- [start:query]
    let query = "MATCH (a:Person {name: $name})-[:KNOWS]->()-[:KNOWS]->(c:Person)
                 RETURN c.name AS friend_of_friend ORDER BY friend_of_friend";
    let params = || HashMap::from([("name".to_string(), serde_json::json!("Alice"))]);
    let result = engine.execute(query, params()).await?;
    print_batches(&[result])?;
    // +------------------+
    // | friend_of_friend |
    // +------------------+
    // | Carol            |
    // | David            |
    // +------------------+
    // --8<-- [end:query]

    // --8<-- [start:refresh]
    // Bob now knows Alice too. The engine doesn't see the write until refresh().
    write_lance(knows(vec![2], vec![1])?, &knows_uri, WriteMode::Append).await?;
    engine.refresh().await?;
    let result = engine.execute(query, params()).await?;
    print_batches(&[result])?; // Alice, Carol, David

    let stats = &engine.csr_stats()["KNOWS"];
    println!(
        "{} edges, update={}, version={:?}",
        stats.edges, stats.update, stats.version
    );
    // 5 edges, update=delta, version=Some(2)
    // --8<-- [end:refresh]

    // --8<-- [start:timeout]
    // One deadline for both: checks inside nested-loop joins, and a timer that stops
    // everything else (cross joins yield to it between batches).
    let deadline = Instant::now() + Duration::from_secs(5);
    let pairs = engine.execute(
        "MATCH (a:Person), (b:Person) RETURN count(*) AS pairs",
        HashMap::new(),
    );
    match timeout_at(deadline, with_execution_deadline(Some(deadline), pairs)).await {
        Ok(result) => print_batches(&[result?])?,
        Err(_) => println!("query timed out"),
    }
    // +-------+
    // | pairs |
    // +-------+
    // | 16    |
    // +-------+
    // --8<-- [end:timeout]

    // --8<-- [start:quickstart]
    let config = GraphConfig::builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()?;
    let person = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "name",
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as _,
        ),
    ])?;
    let knows = RecordBatch::try_from_iter([
        ("src", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("dst", Arc::new(Int64Array::from(vec![2, 3])) as _),
    ])?;
    let sources = HashMap::from([
        ("Person".to_string(), EngineSource::Arrow(person)),
        ("KNOWS".to_string(), EngineSource::Arrow(knows)),
    ]);
    let engine = CypherEngine::new(config, sources, true).await?;
    let result = engine
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) \
             RETURN b.name AS friend ORDER BY friend",
            HashMap::new(),
        )
        .await?;
    print_batches(&[result])?;
    // +--------+
    // | friend |
    // +--------+
    // | Bob    |
    // | Carol  |
    // +--------+
    // --8<-- [end:quickstart]

    getting_started().await
}

/// The Rust tabs of the getting started guide, one section per Python snippet.
async fn getting_started() -> lance_csr::Result<()> {
    // --8<-- [start:gs_datasets]
    let int = |values: Vec<i64>| Arc::new(Int64Array::from(values)) as ArrayRef;
    let text = |values: Vec<&str>| Arc::new(StringArray::from(values)) as ArrayRef;

    let datasets = HashMap::from([
        (
            "Person".to_string(),
            RecordBatch::try_from_iter([
                ("id", int(vec![1, 2, 3, 4])),
                ("name", text(vec!["Alice", "Bob", "Carol", "David"])),
                ("age", int(vec![28, 34, 29, 42])),
            ])?,
        ),
        (
            "City".to_string(),
            RecordBatch::try_from_iter([
                ("id", int(vec![10, 20])),
                ("name", text(vec!["London", "Sydney"])),
            ])?,
        ),
        (
            "KNOWS".to_string(),
            RecordBatch::try_from_iter([
                ("src", int(vec![1, 1, 2, 3])),
                ("dst", int(vec![2, 3, 4, 4])),
            ])?,
        ),
        (
            "LIVES_IN".to_string(),
            RecordBatch::try_from_iter([
                ("src", int(vec![1, 2, 3, 4])),
                ("dst", int(vec![10, 10, 20, 20])),
            ])?,
        ),
    ]);
    // --8<-- [end:gs_datasets]

    // --8<-- [start:gs_config]
    let config = GraphConfig::builder()
        .node_table("Person", "id")
        .node_table("City", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .rel_table("LIVES_IN", "Person", "City", "src", "dst")
        .build()?;
    // --8<-- [end:gs_config]

    // --8<-- [start:gs_query]
    // The engine reads each dataset as an `EngineSource`; `false` leaves the CSR off.
    let sources: HashMap<_, _> = datasets
        .iter()
        .map(|(name, batch)| (name.clone(), EngineSource::Arrow(batch.clone())))
        .collect();
    let engine = CypherEngine::new(config.clone(), sources.clone(), false).await?;

    let result = engine
        .execute(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City {name: 'London'})
             RETURN p.name AS name, p.age AS age
             ORDER BY age",
            HashMap::new(),
        )
        .await?;
    print_batches(&[result])?;
    // +-------+-----+
    // | name  | age |
    // +-------+-----+
    // | Alice | 28  |
    // | Bob   | 34  |
    // +-------+-----+
    // --8<-- [end:gs_query]

    // --8<-- [start:gs_params]
    let older = engine
        .execute(
            "MATCH (p:Person) WHERE p.age >= $min_age RETURN p.name AS name ORDER BY name",
            HashMap::from([("min_age".to_string(), serde_json::json!(30))]),
        )
        .await?;
    print_batches(&[older])?;
    // +-------+
    // | name  |
    // +-------+
    // | Bob   |
    // | David |
    // +-------+
    // --8<-- [end:gs_params]

    // --8<-- [start:gs_csr]
    let engine = CypherEngine::new(config.clone(), sources, true).await?;

    let friends_of_friends = engine
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c:Person)
             RETURN DISTINCT c.name AS friend_of_friend",
            HashMap::new(),
        )
        .await?;
    print_batches(&[friends_of_friends])?;
    // +------------------+
    // | friend_of_friend |
    // +------------------+
    // | David            |
    // +------------------+
    // --8<-- [end:gs_csr]

    // --8<-- [start:gs_one_off]
    let result =
        CypherQuery::new("MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name ORDER BY name")?
            .with_config(config)
            .execute(datasets, None)
            .await?;
    print_batches(&[result])?;
    // +-------+
    // | name  |
    // +-------+
    // | Bob   |
    // | David |
    // +-------+
    // --8<-- [end:gs_one_off]
    Ok(())
}
