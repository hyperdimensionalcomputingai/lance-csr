// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Graph query execution benchmarks with actual data processing
//!
//! This benchmark measures ACTUAL query execution performance:
//! - Creates Arrow datasets
//! - Executes Cypher queries
//! - Processes query results
//! - Full end-to-end execution!
//!
//! Run with:
//! ```
//! cargo bench --bench graph_execution
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_graph::{CypherQuery, GraphConfig};
use tempfile::TempDir;

fn create_people_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("person_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol", "David", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![28, 34, 29, 42, 31])),
        ],
    )
    .unwrap()
}

fn create_friendship_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("person1_id", DataType::Int32, false),
        Field::new("person2_id", DataType::Int32, false),
        Field::new("friendship_type", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 4, 5])),
            Arc::new(StringArray::from(vec![
                "close", "casual", "close", "casual", "close",
            ])),
        ],
    )
    .unwrap()
}

// Execute query using CypherQuery::execute against in-memory batches
fn execute_cypher_query(
    rt: &tokio::runtime::Runtime,
    q: &CypherQuery,
    datasets: HashMap<String, RecordBatch>,
) -> RecordBatch {
    rt.block_on(async move { q.execute(datasets, None).await.unwrap() })
}

fn make_people_batch(n: usize) -> RecordBatch {
    if n == 5 {
        return create_people_batch();
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("person_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let ids: Vec<i32> = (0..n as i32).collect();
    let names: Vec<String> = (0..n).map(|i| format!("name_{}", i)).collect();
    let ages: Vec<i32> = (0..n as i32).map(|i| 20 + (i % 60)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(ages)),
        ],
    )
    .unwrap()
}

fn make_friendship_batch(n: usize) -> RecordBatch {
    if n == 5 {
        return create_friendship_batch();
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("person1_id", DataType::Int32, false),
        Field::new("person2_id", DataType::Int32, false),
        Field::new("friendship_type", DataType::Utf8, false),
    ]));
    let src: Vec<i32> = (0..n as i32).collect();
    let dst: Vec<i32> = (0..n as i32).map(|i| (i + 1) % n as i32).collect();
    let ftype: Vec<&str> = std::iter::repeat_n("friend", n).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(src)),
            Arc::new(Int32Array::from(dst)),
            Arc::new(StringArray::from(ftype)),
        ],
    )
    .unwrap()
}

fn bench_cypher_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("cypher_execution");
    let sizes = [100usize, 10_000usize, 1_000_000usize];

    // Global runtime reused across iterations
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Helper function to create graph config
    let make_config = || {
        GraphConfig::builder()
            .node_table("Person", "person_id")
            .rel_table("FRIEND_OF", "Person", "Person", "person1_id", "person2_id")
            .build()
            .unwrap()
    };

    // Prebuild queries (reuse per iteration)
    let q_basic = CypherQuery::new("MATCH (n:Person) WHERE n.age > 50 RETURN n.name")
        .unwrap()
        .with_config(make_config());
    let q_single_hop = CypherQuery::new("MATCH (a:Person)-[:FRIEND_OF]->(b:Person) RETURN b.name")
        .unwrap()
        .with_config(make_config());
    let q_two_hop = CypherQuery::new(
        "MATCH (a:Person)-[:FRIEND_OF]->(b:Person)-[:FRIEND_OF]->(c:Person) RETURN c.name",
    )
    .unwrap()
    .with_config(make_config());

    // Prebuild small (100) datasets once and reuse
    let person_small = make_people_batch(100);
    let friendship_small = make_friendship_batch(100);

    // Persist medium datasets once and reuse
    let (_medium_tmpdir, person_medium, friendship_medium): (TempDir, RecordBatch, RecordBatch) = {
        let tmpdir = tempfile::tempdir().unwrap();
        let person_path = tmpdir.path().join("person.lance");
        let friend_path = tmpdir.path().join("friendship.lance");

        let person_b = make_people_batch(10_000);
        let friend_b = make_friendship_batch(10_000);

        rt.block_on(async {
            Dataset::write(
                RecordBatchIterator::new(vec![Ok(person_b.clone())].into_iter(), person_b.schema()),
                person_path.to_str().unwrap(),
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            Dataset::write(
                RecordBatchIterator::new(vec![Ok(friend_b.clone())].into_iter(), friend_b.schema()),
                friend_path.to_str().unwrap(),
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

            let person_ds = Dataset::open(person_path.to_str().unwrap()).await.unwrap();
            let p_batches = person_ds
                .scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let person_one = p_batches.into_iter().next().unwrap();

            let friend_ds = Dataset::open(friend_path.to_str().unwrap()).await.unwrap();
            let f_batches = friend_ds
                .scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let friend_one = f_batches.into_iter().next().unwrap();

            (tmpdir, person_one, friend_one)
        })
    };

    // Persist large (1_000_000) datasets once and reuse
    let (_large_tmpdir, person_large, friendship_large): (TempDir, RecordBatch, RecordBatch) = {
        let tmpdir = tempfile::tempdir().unwrap();
        let person_path = tmpdir.path().join("person.lance");
        let friend_path = tmpdir.path().join("friendship.lance");

        let person_b = make_people_batch(1_000_000);
        let friend_b = make_friendship_batch(1_000_000);

        rt.block_on(async {
            Dataset::write(
                RecordBatchIterator::new(vec![Ok(person_b.clone())].into_iter(), person_b.schema()),
                person_path.to_str().unwrap(),
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            Dataset::write(
                RecordBatchIterator::new(vec![Ok(friend_b.clone())].into_iter(), friend_b.schema()),
                friend_path.to_str().unwrap(),
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

            let person_ds = Dataset::open(person_path.to_str().unwrap()).await.unwrap();
            let p_batches = person_ds
                .scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let person_one = p_batches.into_iter().next().unwrap();

            let friend_ds = Dataset::open(friend_path.to_str().unwrap()).await.unwrap();
            let f_batches = friend_ds
                .scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let friend_one = f_batches.into_iter().next().unwrap();

            (tmpdir, person_one, friend_one)
        })
    };

    // 1) Basic node filter + projection
    for &n in &sizes {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("basic_node_filter", n), &n, |b, &n| {
            b.iter(|| {
                let people_batch = match n {
                    100 => person_small.clone(),
                    10_000 => person_medium.clone(),
                    _ => person_large.clone(),
                };
                let mut ds = HashMap::new();
                ds.insert("Person".to_string(), people_batch);
                let out = execute_cypher_query(&rt, &q_basic, ds);
                black_box(out.num_rows());
            })
        });
    }

    // 2) Single-hop relationship expansion
    for &n in &sizes {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("single_hop_expand", n), &n, |b, &n| {
            b.iter(|| {
                let people_batch = match n {
                    100 => person_small.clone(),
                    10_000 => person_medium.clone(),
                    _ => person_large.clone(),
                };
                let friendship = match n {
                    100 => friendship_small.clone(),
                    10_000 => friendship_medium.clone(),
                    _ => friendship_large.clone(),
                };
                let mut ds = HashMap::new();
                ds.insert("Person".to_string(), people_batch);
                ds.insert("FRIEND_OF".to_string(), friendship);
                let out = execute_cypher_query(&rt, &q_single_hop, ds);
                black_box(out.num_rows());
            })
        });
    }

    // 3) Two-hop relationship expansion
    for &n in &sizes {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("two_hop_expand", n), &n, |b, &n| {
            b.iter(|| {
                let people_batch = match n {
                    100 => person_small.clone(),
                    10_000 => person_medium.clone(),
                    _ => person_large.clone(),
                };
                let friendship = match n {
                    100 => friendship_small.clone(),
                    10_000 => friendship_medium.clone(),
                    _ => friendship_large.clone(),
                };
                let mut ds = HashMap::new();
                ds.insert("Person".to_string(), people_batch);
                ds.insert("FRIEND_OF".to_string(), friendship);
                let out = execute_cypher_query(&rt, &q_two_hop, ds);
                black_box(out.num_rows());
            })
        });
    }

    group.finish();
}

// A fixed snapshot isolates template reuse from dataset/catalog construction.
// The result count and checksum are checked outside the timed loops.
fn bench_query_templates(c: &mut Criterion) {
    use datafusion::datasource::{DefaultTableSource, MemTable};
    use datafusion::execution::context::SessionContext;
    use lance_graph::{GraphSourceCatalog, InMemoryCatalog};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let context = SessionContext::new();
    let batch = make_people_batch(10_000);
    let provider = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap());
    let catalog: Arc<dyn GraphSourceCatalog> = Arc::new(
        InMemoryCatalog::new()
            .with_node_source("Person", Arc::new(DefaultTableSource::new(provider))),
    );
    let config = GraphConfig::builder()
        .node_table("Person", "person_id")
        .build()
        .unwrap();
    let text = "MATCH (p:Person) WHERE p.age >= $minimum RETURN p.person_id AS id ORDER BY id";
    let fresh = || {
        CypherQuery::new(text)
            .unwrap()
            .with_config(config.clone())
            .with_parameter("minimum", 50)
    };
    let mut template = fresh();
    assert!(template.prepare_cached_plan(catalog.clone()).unwrap());
    let expected = rt
        .block_on(fresh().execute_with_catalog_and_context(catalog.clone(), context.clone()))
        .unwrap();
    let cached = rt
        .block_on(template.execute_with_catalog_and_context(catalog.clone(), context.clone()))
        .unwrap();
    assert_eq!(cached, expected);
    let expected_ids: Vec<i32> = (0..10_000).filter(|id| 20 + id % 60 >= 50).collect();
    assert_eq!(cached.num_rows(), expected_ids.len());
    let ids = cached
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        ids.values().iter().map(|id| i64::from(*id)).sum::<i64>(),
        expected_ids.iter().map(|id| i64::from(*id)).sum::<i64>()
    );
    let mut group = c.benchmark_group("query_templates");
    group.bench_function("cold_parse_plan_execute", |b| {
        b.iter(|| {
            black_box(
                rt.block_on(
                    fresh().execute_with_catalog_and_context(catalog.clone(), context.clone()),
                )
                .unwrap(),
            );
        })
    });
    group.bench_function("cached_bind_plan_execute", |b| {
        b.iter(|| {
            let query = template.clone().with_parameter("minimum", black_box(50));
            black_box(
                rt.block_on(
                    query.execute_with_catalog_and_context(catalog.clone(), context.clone()),
                )
                .unwrap(),
            );
        })
    });
    group.finish();
}

fn bench_comprehension_counts(c: &mut Criterion) {
    use arrow_array::Int64Array;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = GraphConfig::builder()
        .node_table("Person", "person_id")
        .build()
        .unwrap();
    let data = HashMap::from([("Person".to_string(), make_people_batch(1000))]);
    let mut group = c.benchmark_group("comprehension_count");
    for length in [16, 256] {
        let query = |map| {
            CypherQuery::new(&format!(
            "MATCH (p:Person) RETURN p.person_id AS id,              size([x IN range(0, {}) WHERE x % 2 = 0 {map}]) AS n ORDER BY id", length - 1
        )).unwrap().with_config(config.clone())
        };
        let count = query("");
        let reference = query("| x");
        let actual = execute_cypher_query(&rt, &count, data.clone());
        let expected = execute_cypher_query(&rt, &reference, data.clone());
        assert_eq!(actual.columns(), expected.columns());
        assert_eq!(actual.num_rows(), 1000);
        let counts = actual
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values().iter().sum::<i64>(), 1000 * length / 2);
        for (name, query) in [
            ("filtered_count", &count),
            ("materialized_reference", &reference),
        ] {
            group.bench_with_input(BenchmarkId::new(name, length), &length, |b, _| {
                b.iter(|| black_box(execute_cypher_query(&rt, query, data.clone())));
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_cypher_execution,
    bench_query_templates,
    bench_comprehension_counts
);
criterion_main!(benches);
