// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Resumable variable-length walks over immutable, typed CSR snapshots.
//!
//! A variable-length expand is a one-segment [`TrailWalk`]: the same depth-first search,
//! edge identity, last-hop batching and cooperative yields as a multi-segment pattern.

use crate::ast::RelationshipDirection;
use crate::config::GraphConfig;
use crate::csr_expand::{AdjacencyCatalog, TrailWalk, WalkSegmentSpec};

#[derive(Debug)]
pub(crate) struct Traversal {
    walk: TrailWalk,
    pub min_hops: u32,
    pub max_hops: u32,
}

impl Traversal {
    /// Every intermediate label needs a node index: joins drop dangling edges at every hop.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &GraphConfig,
        catalog: &AdjacencyCatalog,
        types: &[String],
        direction: &RelationshipDirection,
        source_label: &str,
        target_label: &str,
        min_hops: u32,
        max_hops: u32,
    ) -> Option<Self> {
        if types.is_empty() {
            return None;
        }
        let segment = WalkSegmentSpec {
            types,
            direction,
            end_label: target_label,
            min: min_hops,
            max: max_hops,
            tie: None,
        };
        let walk = TrailWalk::new(config, catalog, source_label, &[segment])?;
        Some(Self {
            walk,
            min_hops,
            max_hops,
        })
    }

    pub(crate) fn walk(&self) -> &TrailWalk {
        &self.walk
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr_expand::{Adjacency, NodeRows, TrailCursor};
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::Arc;

    /// The next end, one at a time.
    async fn next(cursor: &mut TrailCursor, graph: &Traversal) -> Option<i64> {
        let mut ends = Vec::new();
        cursor
            .next_group(graph.walk(), &[], 1, &mut ends)
            .await
            .then(|| ends[0])
    }

    async fn graph() -> (GraphConfig, AdjacencyCatalog) {
        let config = GraphConfig::builder()
            .node_table("Tag", "id")
            .node_table("Class", "id")
            .rel_table("hasType", "Tag", "Class", "src", "dst")
            .rel_table("isSubclass", "Class", "Class", "src", "dst")
            .build()
            .unwrap();
        let mut catalog = AdjacencyCatalog::default();
        for (name, ids) in [("Tag", vec![1, 2]), ("Class", vec![1, 2, 3])] {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();
            catalog.insert_nodes(
                name,
                Arc::new(NodeRows::from_batch(batch, "id").unwrap().unwrap()),
            );
        }
        for (name, src, dst) in [
            ("hasType", vec![1, 1, 2, 1], vec![1, 1, 2, 99]),
            ("isSubclass", vec![1, 2, 3], vec![2, 3, 1]),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("src", DataType::Int64, false),
                Field::new("dst", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(src)),
                    Arc::new(Int64Array::from(dst)),
                ],
            )
            .unwrap();
            for (outgoing, from, to) in [(true, "src", "dst"), (false, "dst", "src")] {
                let stream = Box::pin(RecordBatchStreamAdapter::new(
                    schema.clone(),
                    futures::stream::iter([Ok(batch.clone())]),
                ));
                catalog.insert(
                    name,
                    outgoing,
                    Arc::new(Adjacency::build(stream, from, to).await.unwrap()),
                );
            }
        }
        (config, catalog)
    }

    #[tokio::test]
    async fn typed_walk_keeps_duplicates_and_skips_missing_intermediate_nodes() {
        let (config, catalog) = graph().await;
        let walk = Traversal::new(
            &config,
            &catalog,
            &["hasType".into(), "isSubclass".into()],
            &RelationshipDirection::Outgoing,
            "Tag",
            "Class",
            0,
            3,
        )
        .unwrap();
        let mut cursor = TrailCursor::new(walk.walk(), 1);
        let mut out = Vec::new();
        while let Some(key) = next(&mut cursor, &walk).await {
            out.push(key);
            assert!(cursor.depth() <= 4);
        }
        out.sort();
        // Tag 1 and Class 1 are different nodes; no extra zero-hop Class 1 result.
        assert_eq!(out, vec![1, 1, 2, 2, 3, 3]);
        assert_eq!(cursor.depth(), 0);
    }

    #[tokio::test]
    async fn last_hop_groups_ends_up_to_the_limit() {
        let (config, catalog) = graph().await;
        // Tag 1 has two parallel hasType edges to Class 1: one group, two ends.
        let walk = Traversal::new(
            &config,
            &catalog,
            &["hasType".into()],
            &RelationshipDirection::Outgoing,
            "Tag",
            "Class",
            1,
            1,
        )
        .unwrap();
        let mut cursor = TrailCursor::new(walk.walk(), 1);
        let mut ends = Vec::new();
        assert!(cursor.next_group(walk.walk(), &[], 8, &mut ends).await);
        assert_eq!(ends, vec![1, 1]);
        assert!(!cursor.next_group(walk.walk(), &[], 8, &mut ends).await);
    }

    #[tokio::test]
    async fn cancellation_visits_raw_tombstones_before_returning_the_live_path() {
        let (config, mut catalog) = graph().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1; 5000])),
                Arc::new(Int64Array::from(vec![1; 5000])),
            ],
        )
        .unwrap();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok(batch)]),
        ));
        let base = Adjacency::build(stream, "src", "dst").await.unwrap();
        catalog.insert(
            "hasType",
            true,
            Arc::new(base.with_changes(&[], &{
                let mut removed = crate::csr_expand::RemovedRows::default();
                removed.rows(0, 0..4999);
                removed
            })),
        );
        let walk = Traversal::new(
            &config,
            &catalog,
            &["hasType".into()],
            &RelationshipDirection::Outgoing,
            "Tag",
            "Class",
            1,
            1,
        )
        .unwrap();
        let mut cursor = TrailCursor::new(walk.walk(), 1);
        let mut pending = Box::pin(next(&mut cursor, &walk));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        assert_eq!(next(&mut cursor, &walk).await, Some(1));
        assert_eq!(next(&mut cursor, &walk).await, None);
    }

    #[tokio::test]
    async fn zero_hop_is_identity_and_a_cycle_can_only_be_traversed_once() {
        let (config, catalog) = graph().await;
        let subclass = |max_hops| {
            Traversal::new(
                &config,
                &catalog,
                &["isSubclass".into()],
                &RelationshipDirection::Outgoing,
                "Class",
                "Class",
                0,
                max_hops,
            )
            .unwrap()
        };
        let walk = subclass(20);
        let mut cursor = TrailCursor::new(walk.walk(), 1);
        for depth in 0..=3 {
            assert_eq!(next(&mut cursor, &walk).await, Some(1 + depth % 3));
            assert_eq!(cursor.depth(), depth as usize + 1);
        }
        assert_eq!(next(&mut cursor, &walk).await, None);
        let zero = subclass(0);
        let mut cursor = TrailCursor::new(zero.walk(), 2);
        assert_eq!(next(&mut cursor, &zero).await, Some(2));
        assert_eq!(next(&mut cursor, &zero).await, None);
    }

    #[tokio::test]
    async fn walk_yields_while_no_candidate_can_match_the_target_label() {
        let (config, mut catalog) = graph().await;
        let adjacency = catalog.get("isSubclass", true).unwrap().with_changes(
            &(0..4096).map(|row| (1, 1, row)).collect::<Vec<_>>(),
            &Default::default(),
        );
        catalog.insert("isSubclass", true, Arc::new(adjacency));
        let walk = Traversal::new(
            &config,
            &catalog,
            &["isSubclass".into()],
            &RelationshipDirection::Outgoing,
            "Class",
            "Tag",
            1,
            2,
        )
        .unwrap();
        let mut cursor = TrailCursor::new(walk.walk(), 1);
        let mut pending = Box::pin(next(&mut cursor, &walk));
        assert!(futures::poll!(&mut pending).is_pending());
        drop(pending);
        assert!(cursor.depth() <= 3);
    }

    #[tokio::test]
    async fn untyped_walk_declines() {
        let (config, catalog) = graph().await;
        let direction = RelationshipDirection::Outgoing;
        assert!(Traversal::new(&config, &catalog, &[], &direction, "Tag", "Class", 1, 2).is_none());
    }
}
