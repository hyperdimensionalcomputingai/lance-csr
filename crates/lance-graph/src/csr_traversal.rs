// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Resumable variable-length walks over immutable, typed CSR snapshots.

use std::sync::Arc;

use crate::ast::RelationshipDirection;
use crate::config::GraphConfig;
use crate::csr_expand::{
    cooperative_yield, AdjacencyCatalog, NeighborPosition, Neighbors, NodeRows,
};

#[derive(Debug)]
struct Step {
    from: usize,
    to: usize,
    adjacency: Neighbors,
}

#[derive(Debug)]
pub(crate) struct Traversal {
    steps: Vec<Step>,
    nodes: Vec<Arc<NodeRows>>,
    start: usize,
    target: usize,
    pub min_hops: u32,
    pub max_hops: u32,
    undirected: bool,
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
        let mut labels: Vec<String> = Vec::new();
        let mut nodes = Vec::new();
        let mut label = |name: &str| -> Option<usize> {
            if let Some(index) = labels.iter().position(|l| l.eq_ignore_ascii_case(name)) {
                return Some(index);
            }
            nodes.push(catalog.nodes(name)?.clone());
            labels.push(name.to_owned());
            Some(labels.len() - 1)
        };
        let (start, target) = (label(source_label)?, label(target_label)?);
        let mut steps = Vec::new();
        for name in types {
            let mapping = config.get_relationship_mapping(name)?;
            let (from, to) = (
                label(mapping.source_label.as_deref()?)?,
                label(mapping.target_label.as_deref()?)?,
            );
            match direction {
                RelationshipDirection::Outgoing => steps.push(Step {
                    from,
                    to,
                    adjacency: Neighbors::directed(catalog.get(name, true)?.clone()),
                }),
                RelationshipDirection::Incoming => steps.push(Step {
                    from: to,
                    to: from,
                    adjacency: Neighbors::directed(catalog.get(name, false)?.clone()),
                }),
                RelationshipDirection::Undirected if from == to => steps.push(Step {
                    from,
                    to,
                    adjacency: Neighbors::undirected(
                        catalog.get(name, true)?.clone(),
                        catalog.get(name, false)?.clone(),
                    ),
                }),
                RelationshipDirection::Undirected => {
                    steps.push(Step {
                        from,
                        to,
                        adjacency: Neighbors::directed(catalog.get(name, true)?.clone()),
                    });
                    steps.push(Step {
                        from: to,
                        to: from,
                        adjacency: Neighbors::directed(catalog.get(name, false)?.clone()),
                    });
                }
            }
        }
        (!steps.is_empty()).then_some(Self {
            steps,
            nodes,
            start,
            target,
            min_hops,
            max_hops,
            undirected: *direction == RelationshipDirection::Undirected,
        })
    }
}

struct Frame {
    label: usize,
    key: i64,
    step: usize,
    position: NeighborPosition,
    emit: bool,
}

impl Frame {
    fn new(label: usize, key: i64) -> Self {
        Self {
            label,
            key,
            step: 0,
            position: NeighborPosition::default(),
            emit: true,
        }
    }
}

pub(crate) struct TraversalCursor {
    stack: Vec<Frame>,
}

impl TraversalCursor {
    pub fn new(graph: &Traversal, key: i64) -> Self {
        Self {
            stack: vec![Frame::new(graph.start, key)],
        }
    }

    /// Keep one frame per hop, and cooperate even when candidate edges produce no rows.
    pub async fn next(&mut self, graph: &Traversal) -> Option<i64> {
        let mut work = 0;
        while !self.stack.is_empty() {
            if work == 1024 {
                cooperative_yield().await;
                work = 0;
            }
            work += 1;
            let depth = self.stack.len() as u32 - 1;
            let frame = self.stack.last_mut().expect("nonempty stack");
            if frame.emit {
                frame.emit = false;
                if depth >= graph.min_hops && frame.label == graph.target {
                    return Some(frame.key);
                }
            }
            if depth == graph.max_hops || frame.step == graph.steps.len() {
                self.stack.pop();
                continue;
            }
            let step = &graph.steps[frame.step];
            let next = (step.from == frame.label)
                .then(|| {
                    step.adjacency
                        .neighbors_from(frame.key, &mut frame.position)
                        .next()
                })
                .flatten();
            let Some(key) = next.map(|n| n as i64) else {
                frame.step += 1;
                frame.position = NeighborPosition::default();
                continue;
            };
            if graph.nodes[step.to].position(key).is_none() {
                continue;
            }
            // Preserve the existing undirected immediate-backtracking rule, using typed ids.
            // shortcut: full relationship uniqueness needs edge row ids, as on the join path.
            if graph.undirected
                && self.stack.len() >= 2
                && self.stack[self.stack.len() - 2].label == step.to
                && self.stack[self.stack.len() - 2].key == key
            {
                continue;
            }
            self.stack.push(Frame::new(step.to, key));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr_expand::Adjacency;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

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
        let mut cursor = TraversalCursor::new(&walk, 1);
        let mut out = Vec::new();
        while let Some(key) = cursor.next(&walk).await {
            out.push(key);
            assert!(cursor.stack.len() <= 4);
        }
        out.sort();
        // Tag 1 and Class 1 are different nodes; no extra zero-hop Class 1 result.
        assert_eq!(out, vec![1, 1, 2, 2, 3, 3]);
        assert!(cursor.stack.is_empty());
    }

    #[tokio::test]
    async fn zero_hop_is_identity_and_depth_bound_survives_cycles() {
        let (config, catalog) = graph().await;
        let walk = Traversal::new(
            &config,
            &catalog,
            &["isSubclass".into()],
            &RelationshipDirection::Outgoing,
            "Class",
            "Class",
            0,
            20,
        )
        .unwrap();
        let mut cursor = TraversalCursor::new(&walk, 1);
        for depth in 0..=20 {
            assert_eq!(cursor.next(&walk).await, Some(1 + depth % 3));
            assert_eq!(cursor.stack.len(), depth as usize + 1);
        }
        assert_eq!(cursor.next(&walk).await, None);
        let zero = Traversal {
            max_hops: 0,
            ..walk
        };
        let mut cursor = TraversalCursor::new(&zero, 2);
        assert_eq!(cursor.next(&zero).await, Some(2));
        assert_eq!(cursor.next(&zero).await, None);
    }

    #[tokio::test]
    async fn walk_yields_while_no_candidate_can_match_the_target_label() {
        let (config, mut catalog) = graph().await;
        let adjacency = catalog
            .get("isSubclass", true)
            .unwrap()
            .with_changes(&vec![(1, 1); 4096], &[]);
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
        let mut cursor = TraversalCursor::new(&walk, 1);
        let mut next = Box::pin(cursor.next(&walk));
        assert!(futures::poll!(&mut next).is_pending());
        drop(next);
        assert!(cursor.stack.len() <= 3);
    }
}
