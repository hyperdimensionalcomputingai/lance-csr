// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, BenchmarkId, Criterion};
use lance_csr::{CypherEngine, EngineSource, GraphConfig};

fn batch(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|(n, _)| Field::new(*n, DataType::Int64, false))
                .collect::<Vec<_>>(),
        )),
        columns
            .iter()
            .map(|(_, v)| Arc::new(Int64Array::from(v.clone())) as _)
            .collect(),
    )
    .unwrap()
}

fn rows(batch: &RecordBatch) -> Vec<(i64, i64)> {
    let values = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    };
    let mut rows: Vec<_> = (0..batch.num_rows())
        .map(|i| (values(0).value(i), values(1).value(i)))
        .collect();
    rows.sort();
    rows
}

/// Cached planning plus execution against a path-enumerating reference on the same engine.
pub fn bench_grouped_two_hop(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = GraphConfig::builder()
        .node_table("N", "id")
        .rel_table("R", "N", "N", "src", "dst")
        .rel_table("S", "N", "N", "src", "dst")
        .build()
        .unwrap();
    let mut group = c.benchmark_group("grouped_two_hop");
    for (name, degree, skewed, same_type, selective) in [
        ("balanced", 32, false, false, false),
        ("skewed", 32, true, false, false),
        ("same_type", 32, false, true, false),
        ("low_fanout", 1, false, false, false),
        ("selective", 32, false, false, true),
    ] {
        let n = 256_i64;
        let pairs: Vec<_> = if skewed {
            (0..n).flat_map(|i| [(i, 0), (0, i)]).collect()
        } else {
            (0..n)
                .flat_map(|i| (0..degree).map(move |j| (i, (i + j) % n)))
                .collect()
        };
        let edges = batch(&[
            ("src", pairs.iter().map(|p| p.0).collect()),
            ("dst", pairs.iter().map(|p| p.1).collect()),
        ]);
        let nodes = batch(&[
            ("id", (0..n).collect()),
            ("bucket", (0..n).map(|i| i % 8).collect()),
        ]);
        let sources = HashMap::from([
            ("N".to_string(), EngineSource::Arrow(nodes)),
            ("R".to_string(), EngineSource::Arrow(edges.clone())),
            ("S".to_string(), EngineSource::Arrow(edges)),
        ]);
        let engine = rt
            .block_on(CypherEngine::new(config.clone(), sources, true))
            .unwrap();
        let relationship = if same_type { "R" } else { "S" };
        let filter = if selective {
            "WHERE a.id = 0 AND c.id = 1"
        } else {
            ""
        };
        for key in ["a.id", "b.id", "a.bucket", "b.bucket"] {
            let query = format!("MATCH (a:N)-[:R]->(b:N)-[:{relationship}]->(c:N) {filter} RETURN {key}, count(*) AS n");
            let reference = query.replace("count(*)", "(count(*) + 0)");
            let execute = |query: &str| rt.block_on(engine.execute(query, HashMap::new())).unwrap();
            let actual = execute(&query);
            assert_eq!(rows(&actual), rows(&execute(&reference)), "{query}");
            let plan = rt.block_on(engine.explain(&query, HashMap::new())).unwrap();
            assert_eq!(
                plan.contains("__paths"),
                !skewed && degree > 1 && !(selective && key.starts_with("a.")),
                "{query}: {plan}"
            );
            let plan = rt
                .block_on(engine.explain(&reference, HashMap::new()))
                .unwrap();
            assert!(
                !plan.contains("__paths") && !plan.contains("degrees="),
                "{plan}"
            );
            for (implementation, query) in [("weighted", query), ("enumerated", reference)] {
                group.bench_function(
                    BenchmarkId::new(format!("{name}/{key}"), implementation),
                    |b| {
                        b.iter(|| black_box(execute(&query)));
                    },
                );
            }
        }
    }
    group.finish();
}
