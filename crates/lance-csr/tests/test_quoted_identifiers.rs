use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{cast::AsArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance_csr::{CypherEngine, EngineSource, GraphConfig, SqlEngine};

fn edges(edges: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.0))),
            Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.1))),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn backtick_quoted_identifiers() {
    let config = GraphConfig::builder()
        .node_table("Device Type", "id")
        .rel_table("PART OF", "Device Type", "Device Type", "src_id", "dst_id")
        .build()
        .unwrap();
    let devices = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("asset id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a-1", "a-2", "a-3"])),
            Arc::new(StringArray::from(vec!["pump", "valve", "tank"])),
        ],
    )
    .unwrap();
    for use_csr in [false, true] {
        let sources = HashMap::from([
            (
                "Device Type".to_string(),
                EngineSource::Arrow(devices.clone()),
            ),
            (
                "PART OF".to_string(),
                EngineSource::Arrow(edges(&[(1, 3), (2, 3)])),
            ),
        ]);
        let engine = CypherEngine::new(config.clone(), sources, use_csr)
            .await
            .unwrap();
        let strings = |batch: &RecordBatch, column: &str| -> Vec<String> {
            let column = batch.column_by_name(column).unwrap().as_string::<i32>();
            column.iter().map(|v| v.unwrap().to_string()).collect()
        };

        let keys = HashMap::from([("keys".to_string(), serde_json::json!(["a-1", "a-3"]))]);
        let batch = engine
            .execute(
                "MATCH (x:`Device Type`) WHERE x.`asset id` IN $keys \
                 RETURN x.`asset id` AS `asset id`, x.name AS `a``b` ORDER BY `asset id`",
                keys,
            )
            .await
            .unwrap();
        assert_eq!(strings(&batch, "asset id"), ["a-1", "a-3"]);
        assert_eq!(strings(&batch, "a`b"), ["pump", "tank"]);

        let batch = engine
            .execute(
                "MATCH (`match`:`Device Type`)-[:`PART OF`]->(`return`) \
                 RETURN `match`.name AS `where` ORDER BY `where`",
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            strings(&batch, "where"),
            ["pump", "valve"],
            "use_csr={use_csr}"
        );

        let batch = engine
            .execute(
                "MATCH (x:`Device Type` {`asset id`: 'a-2'}) RETURN x.name AS name",
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(strings(&batch, "name"), ["valve"]);

        // SQL and GRAPH_TABLE quote with double quotes; tables are registered lowercase.
        let sql = SqlEngine::from_engine(Arc::new(engine));
        let batch = sql
            .execute(
                "SELECT g.\"asset id\", d.name AS \"select\" FROM GRAPH_TABLE (
                     MATCH (\"match\":\"Device Type\")-[:`PART OF`]->(t) WHERE t.name = 'tank'
                     COLUMNS (\"match\".\"asset id\" AS \"asset id\", t.id AS tid)
                 ) g JOIN \"device type\" d ON d.id = g.tid
                 WHERE d.\"asset id\" = 'a-3' ORDER BY g.\"asset id\"",
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(strings(&batch, "asset id"), ["a-1", "a-2"]);
        assert_eq!(strings(&batch, "select"), ["tank", "tank"]);
    }
}
