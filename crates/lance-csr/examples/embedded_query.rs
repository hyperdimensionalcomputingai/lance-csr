//! Run Cypher over Lance datasets from Rust, with no Python.
//!
//! Writes a small social graph as two Lance datasets in a temporary directory, opens
//! them in a CSR-backed engine, runs a two-hop query, appends an edge and picks it up
//! with `refresh()`, and runs a query under a deadline. The documentation includes this
//! file section by section (the `--8<--` markers), so keep it compiling and its printed
//! output in step.
//!
//! ```bash
//! cargo run -p lance-csr --example embedded_query
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::util::pretty::print_batches;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_csr::query::with_execution_deadline;
use lance_csr::{CypherEngine, EngineSource, GraphConfig};
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
    Ok(())
}
