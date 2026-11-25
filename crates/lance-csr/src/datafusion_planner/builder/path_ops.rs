// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

use super::trail_joins::ChainSegment;
use crate::ast::{NodePattern, PathPattern, PropertyValue, RelationshipPattern};
use crate::case_insensitive::qualify_column;
use crate::csr_expand::{Tie, TrailWalk, TrailWalkNode, WalkSegmentSpec};
use crate::datafusion_planner::{analysis::PlanningContext, DataFusionPlanner};
use crate::error::{GraphError, Result};
use crate::logical_plan::LogicalOperator;
use crate::path_inventory::{EdgeSource, NodeSource};
use crate::path_match::{Binding, PathMatchNode, PathSpec, Step};
use datafusion::common::Column;
use datafusion::logical_expr::{col, Expr, Extension, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashMap;
use std::sync::Arc;

/// The hop range of one segment: a single hop without a length.
fn hop_range(rel: &RelationshipPattern) -> [u32; 2] {
    match &rel.length {
        None => [1, 1],
        Some(length) => [
            length.min.unwrap_or(1),
            length.max.unwrap_or(crate::MAX_VARIABLE_LENGTH_HOPS),
        ],
    }
}

impl DataFusionPlanner {
    /// A path without a path variable whose variable-length segment shares a relationship
    /// type with another segment (`(a)-[:R*1..2]-(f)-[:R]-(g)`). The segments must not
    /// share an edge, so they are planned as one trail instead of separate expansions: a
    /// CSR walk seeded at each start row ([`TrailWalkNode`]) when every type and label is
    /// indexed and no segment filters relationship properties, otherwise one chain of
    /// joins per combination of segment lengths ([`Self::build_trail_chain`]). Either way
    /// the work follows the rows' start nodes; no edge inventory is built.
    ///
    /// `None` leaves the pattern to the edge inventory ([`PathMatchNode`]), which then
    /// covers only these shapes: a relationship variable on any segment, an unresolved
    /// boundary label or start column, a range the inventory rejects, and, without the
    /// CSR, a pattern whose segment lengths combine into more than 64 join branches or
    /// whose labels admit no path.
    fn build_overlapping_trail(
        &self,
        ctx: &PlanningContext,
        input: &LogicalPlan,
        path: &PathPattern,
    ) -> Result<Option<LogicalPlan>> {
        let segments = &path.segments;
        if !path.needs_trail_across_segments()
            || segments
                .iter()
                .any(|s| s.relationship.types.is_empty() || s.relationship.variable.is_some())
        {
            return Ok(None);
        }
        let resolve = |node: &NodePattern| -> Option<(String, String)> {
            let variable = node.variable.clone()?;
            let label = ctx
                .analysis
                .var_to_label
                .get(&variable)
                .or_else(|| node.labels.first())?
                .clone();
            self.config.get_node_mapping(&label)?;
            Some((variable, label))
        };
        let Some((start, start_label)) = resolve(&path.start_node) else {
            return Ok(None);
        };
        let mut ends = Vec::with_capacity(segments.len());
        for segment in segments {
            let [min, max] = hop_range(&segment.relationship);
            match resolve(&segment.end_node) {
                Some(end) if min <= max && max <= crate::MAX_VARIABLE_LENGTH_HOPS => ends.push(end),
                _ => return Ok(None),
            }
        }
        let start_map = self
            .config
            .get_node_mapping(&start_label)
            .ok_or_else(|| GraphError::internal("a resolved start label"))?;
        let start_key = qualify_column(&start, &start_map.id_field);
        if input
            .schema()
            .field_with_unqualified_name(&start_key)
            .is_err()
        {
            return Ok(None);
        }
        let input = self.filter_node_properties(
            input.clone(),
            &start,
            &start_label,
            &path.start_node.properties,
        )?;
        let walked = self.try_build_trail_walk(&input, [&start, &start_label], path, &ends)?;
        let mut plan = match walked {
            Some(plan) => plan,
            None => {
                let chain: Vec<ChainSegment> = segments
                    .iter()
                    .zip(&ends)
                    .map(|(segment, (end, end_label))| ChainSegment {
                        types: &segment.relationship.types,
                        direction: &segment.relationship.direction,
                        range: hop_range(&segment.relationship),
                        properties: &segment.relationship.properties,
                        end,
                        end_label,
                    })
                    .collect();
                match self.build_trail_chain(&input, [&start, &start_label], &chain)? {
                    Some(plan) => plan,
                    None => return Ok(None),
                }
            }
        };
        for (segment, (end, label)) in segments.iter().zip(&ends) {
            plan = self.filter_node_properties(plan, end, label, &segment.end_node.properties)?;
        }
        Ok(Some(plan))
    }

    /// The CSR walk of [`Self::build_overlapping_trail`], with each new boundary node's
    /// other columns looked up through its node index (and dropped when unused). `None`
    /// when a type, direction or label has no index, a segment filters relationship
    /// properties, or a start or bound column is not an integer id.
    fn try_build_trail_walk(
        &self,
        input: &LogicalPlan,
        [start, start_label]: [&str; 2],
        path: &PathPattern,
        ends: &[(String, String)],
    ) -> Result<Option<LogicalPlan>> {
        let Some(catalog) = &self.adjacency else {
            return Ok(None);
        };
        if path
            .segments
            .iter()
            .any(|s| !s.relationship.properties.is_empty())
        {
            return Ok(None);
        }
        let schema = input.schema();
        let id_column = |name: &str| {
            schema
                .field_with_unqualified_name(name)
                .ok()
                .map(|f| crate::csr_expand::is_supported_id_type(f.data_type()))
        };
        let node_map = |label: &str| {
            self.config
                .get_node_mapping(label)
                .ok_or_else(|| GraphError::internal("a resolved boundary label"))
        };
        let start_key = qualify_column(start, &node_map(start_label)?.id_field);
        if id_column(&start_key) != Some(true) {
            return Ok(None);
        }
        // Boundary k is the start for k = 0, else the end of segment k - 1.
        let mut boundaries = vec![start.to_lowercase()];
        let mut bound = Vec::new();
        let mut specs = Vec::with_capacity(path.segments.len());
        let mut outputs = Vec::new();
        for (s, (segment, (end, label))) in path.segments.iter().zip(ends).enumerate() {
            let key = qualify_column(end, &node_map(label)?.id_field);
            let tie = if let Some(k) = boundaries.iter().position(|b| *b == end.to_lowercase()) {
                Some(Tie::Boundary(k))
            } else {
                match id_column(&key) {
                    Some(true) => {
                        bound.push(key.clone());
                        Some(Tie::Input(bound.len() - 1))
                    }
                    Some(false) => return Ok(None),
                    None => None,
                }
            };
            if tie.is_none() {
                outputs.push((s, end.as_str(), label.as_str(), key));
            }
            boundaries.push(end.to_lowercase());
            let [min, max] = hop_range(&segment.relationship);
            specs.push(WalkSegmentSpec {
                types: &segment.relationship.types,
                direction: &segment.relationship.direction,
                end_label: label,
                min,
                max,
                tie,
            });
        }
        let Some(walk) = TrailWalk::new(&self.config, catalog, start_label, &specs) else {
            return Ok(None);
        };
        let walk = Arc::new(walk);
        let cat = self.get_catalog()?;
        let mut fields = Vec::with_capacity(outputs.len());
        for (_, _, label, key) in &outputs {
            let id = &node_map(label)?.id_field;
            let source = cat
                .node_source(label)
                .ok_or_else(|| GraphError::internal("an indexed boundary label's table"))?;
            let schema = source.schema();
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(id))
                .ok_or_else(|| GraphError::internal("a boundary node's id column"))?;
            fields.push(field.as_ref().clone().with_name(key).with_nullable(true));
        }
        let node = TrailWalkNode::try_new(input.clone(), start_key, bound, walk.clone(), fields)?;
        let mut plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        for (s, end, label, key) in outputs {
            let id = &node_map(label)?.id_field;
            let rows = walk.end_rows(s).clone();
            let wanted: Vec<(String, String)> = rows
                .schema()
                .fields()
                .iter()
                .filter(|f| !f.name().eq_ignore_ascii_case(id))
                .map(|f| (qualify_column(end, f.name()), f.name().clone()))
                .collect();
            if wanted.is_empty() {
                continue;
            }
            let lookup = crate::csr_expand::NodeLookupNode::try_new(
                plan,
                key,
                end.to_lowercase(),
                rows,
                &wanted,
            )?;
            plan = LogicalPlan::Extension(Extension {
                node: Arc::new(lookup),
            });
        }
        Ok(Some(plan))
    }

    /// `plan` keeping the rows whose node `variable` has the inline property values.
    fn filter_node_properties(
        &self,
        plan: LogicalPlan,
        variable: &str,
        label: &str,
        properties: &HashMap<String, PropertyValue>,
    ) -> Result<LogicalPlan> {
        let schema = self.get_catalog()?.node_source(label).map(|s| s.schema());
        let Some(predicate) = properties
            .iter()
            .map(|(key, value)| {
                let field = schema
                    .as_ref()
                    .and_then(|s| {
                        s.fields()
                            .iter()
                            .find(|f| f.name().eq_ignore_ascii_case(key))
                    })
                    .map_or(key.as_str(), |f| f.name().as_str());
                let value = self
                    .unresolved_value_expr(&crate::ast::ValueExpression::Literal(value.clone()));
                col(Column::new_unqualified(qualify_column(variable, field))).eq(value)
            })
            .reduce(Expr::and)
        else {
            return Ok(plan);
        };
        LogicalPlanBuilder::from(plan)
            .filter(predicate)
            .and_then(|b| b.build())
            .map_err(|e| self.plan_error("Failed to filter path node properties", e))
    }

    pub(crate) fn build_path_match(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        path: &PathPattern,
        node_values: bool,
    ) -> Result<LogicalPlan> {
        let error = |message: String| GraphError::UnsupportedFeature {
            feature: message,
            location: snafu::Location::new(file!(), line!(), column!()),
        };
        let catalog = self.get_catalog()?;
        let input = self.build_operator(ctx, input)?;
        if !node_values {
            if let Some(plan) = self.build_overlapping_trail(ctx, &input, path)? {
                return Ok(plan);
            }
        }
        let mut node_labels = Vec::<String>::new();
        let mut edge_labels = Vec::<String>::new();
        for segment in &path.segments {
            if segment.relationship.types.is_empty() {
                return Err(error("named paths need explicit relationship types".into()));
            }
            for kind in &segment.relationship.types {
                if edge_labels.iter().any(|t| t.eq_ignore_ascii_case(kind)) {
                    continue;
                }
                let mapping = self.get_relationship_mapping(kind)?;
                for label in [&mapping.source_label, &mapping.target_label] {
                    let label = label.as_ref().ok_or_else(|| {
                        error("named paths need configured endpoint labels".into())
                    })?;
                    if !node_labels.iter().any(|l| l.eq_ignore_ascii_case(label)) {
                        node_labels.push(label.clone());
                    }
                }
                edge_labels.push(mapping.relationship_type.clone());
            }
        }
        let mut plans = vec![input];
        let mut nodes = Vec::new();
        for label in &node_labels {
            let mapping = self
                .config
                .get_node_mapping(label)
                .ok_or_else(|| error(format!("missing node mapping {label}")))?;
            let source = catalog
                .node_source(label)
                .ok_or_else(|| error(format!("missing node table {label}")))?;
            let schema = source.schema();
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(&mapping.id_field))
                .ok_or_else(|| error(format!("missing node ID {}", mapping.id_field)))?;
            if !crate::csr_expand::is_supported_id_type(field.data_type()) {
                return Err(error(format!(
                    "path node ID '{}' has unsupported type {}",
                    field.name(),
                    field.data_type()
                )));
            }
            nodes.push(NodeSource {
                label: mapping.label.clone(),
                id: mapping.id_field.clone(),
                schema,
            });
            plans.push(
                LogicalPlanBuilder::scan(label.to_lowercase(), source, None)
                    .and_then(|b| b.build())
                    .map_err(|e| self.plan_error("Failed to scan path nodes", e))?,
            );
        }
        let mut edges = Vec::new();
        for label in &edge_labels {
            let mapping = self.get_relationship_mapping(label)?;
            let source = catalog
                .relationship_source(label)
                .ok_or_else(|| error(format!("missing relationship table {label}")))?;
            let schema = source.schema();
            for id in [&mapping.source_id_field, &mapping.target_id_field] {
                let field = schema
                    .fields()
                    .iter()
                    .find(|f| f.name().eq_ignore_ascii_case(id))
                    .ok_or_else(|| error(format!("missing relationship ID {id}")))?;
                if !crate::csr_expand::is_supported_id_type(field.data_type()) {
                    return Err(error(format!(
                        "path relationship ID '{id}' has unsupported type {}",
                        field.data_type()
                    )));
                }
            }
            // node_labels was deduplicated ignoring case, so look endpoints up the same way.
            let endpoint = |label: &Option<String>| {
                node_labels
                    .iter()
                    .position(|l| label.as_ref().is_some_and(|s| l.eq_ignore_ascii_case(s)))
                    .ok_or_else(|| error(format!("missing endpoint label for {label:?}")))
            };
            edges.push(EdgeSource {
                label: mapping.relationship_type.clone(),
                source_label: endpoint(&mapping.source_label)?,
                target_label: endpoint(&mapping.target_label)?,
                source_id: mapping.source_id_field.clone(),
                target_id: mapping.target_id_field.clone(),
                schema,
            });
            plans.push(
                LogicalPlanBuilder::scan(label.to_lowercase(), source, None)
                    .and_then(|b| b.build())
                    .map_err(|e| self.plan_error("Failed to scan path edges", e))?,
            );
        }
        let mut bindings = Vec::new();
        for node in
            std::iter::once(&path.start_node).chain(path.segments.iter().map(|s| &s.end_node))
        {
            let variable = node
                .variable
                .clone()
                .ok_or_else(|| error("named path endpoint was not assigned a variable".into()))?;
            let label = ctx
                .analysis
                .var_to_label
                .get(&variable)
                .or_else(|| node.labels.first())
                .ok_or_else(|| {
                    error(format!("path variable '{variable}' needs a resolved label"))
                })?;
            let label = node_labels
                .iter()
                .position(|l| l.eq_ignore_ascii_case(label))
                .ok_or_else(|| {
                    error(format!(
                        "path label '{label}' is incompatible with its relationship types"
                    ))
                })?;
            bindings.push(Binding {
                id_column: qualify_column(&variable, &nodes[label].id),
                variable,
                label,
            });
        }
        let shortest = path.shortest.as_ref().map(|s| s.all);
        let mut steps = Vec::new();
        for segment in &path.segments {
            let rel = &segment.relationship;
            let min = rel.length.as_ref().and_then(|l| l.min).unwrap_or(1);
            let max = rel
                .length
                .as_ref()
                .map(|l| {
                    l.max.unwrap_or(if shortest.is_some() {
                        u32::MAX
                    } else {
                        crate::MAX_VARIABLE_LENGTH_HOPS
                    })
                })
                .unwrap_or(1);
            if min > max || (shortest.is_none() && max > crate::MAX_VARIABLE_LENGTH_HOPS) {
                return Err(error(format!("invalid named path range {min}..{max}; ordinary variable-length paths allow at most {} hops", crate::MAX_VARIABLE_LENGTH_HOPS)));
            }
            steps.push(Step {
                types: rel
                    .types
                    .iter()
                    .map(|t| {
                        edge_labels
                            .iter()
                            .position(|l| l.eq_ignore_ascii_case(t))
                            .ok_or_else(|| error(format!("unknown relationship type {t}")))
                    })
                    .collect::<Result<_>>()?,
                direction: rel.direction.clone(),
                min,
                max,
                relationship: rel.variable.clone(),
            });
        }
        let variable = match &path.shortest {
            Some(shortest) => Some(
                shortest
                    .variable
                    .clone()
                    .ok_or_else(|| error("rich path needs a variable".into()))?,
            ),
            None => path.variable.clone(),
        };
        let node = PathMatchNode::try_new(
            plans,
            PathSpec {
                variable,
                node_values,
                nodes,
                edges,
                bindings,
                steps,
                shortest,
            },
            std::sync::Arc::clone(&ctx.path_sources),
            std::iter::once(&path.start_node.properties)
                .chain(
                    path.segments
                        .iter()
                        .map(|segment| &segment.end_node.properties),
                )
                .chain(
                    path.segments
                        .iter()
                        .map(|segment| &segment.relationship.properties),
                ),
        )
        .map_err(|e| self.plan_error("Failed to plan path value", e))?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{CypherEngine, EngineSource, GraphConfig};

    fn ints(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, false))
            .collect();
        let arrays = columns
            .iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values.clone())) as _)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    /// Nodes 1, 2, 3; `R` edges 1->2 twice (parallel, so distinct) and 2->3.
    async fn engine(use_csr: bool) -> CypherEngine {
        let config = GraphConfig::builder()
            .node_table("N", "id")
            .rel_table("R", "N", "N", "src", "dst")
            .build()
            .unwrap();
        let sources = HashMap::from([
            (
                "N".to_string(),
                EngineSource::Arrow(ints(&[("id", vec![1, 2, 3])])),
            ),
            (
                "R".to_string(),
                EngineSource::Arrow(ints(&[("src", vec![1, 1, 2]), ("dst", vec![2, 2, 3])])),
            ),
        ]);
        CypherEngine::new(config, sources, use_csr).await.unwrap()
    }

    async fn rows(engine: &CypherEngine, query: &str) -> Vec<Vec<i64>> {
        let batch = engine.execute(query, HashMap::new()).await.unwrap();
        let columns: Vec<&Int64Array> = batch
            .columns()
            .iter()
            .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect();
        let mut out: Vec<Vec<i64>> = (0..batch.num_rows())
            .map(|i| columns.iter().map(|c| c.value(i)).collect())
            .collect();
        out.sort();
        out
    }

    /// The rows of `query` with and without the CSR, which must agree. Neither plan may
    /// build the edge inventory; the CSR plan walks the pattern from each start row.
    async fn trail(query: &str) -> Vec<Vec<i64>> {
        let mut results = Vec::new();
        for use_csr in [false, true] {
            let engine = engine(use_csr).await;
            let plan = engine.explain(query, HashMap::new()).await.unwrap();
            assert!(!plan.contains("PathMatchExec"), "{query}: {plan}");
            assert_eq!(plan.contains("TrailWalkExec"), use_csr, "{query}: {plan}");
            results.push(rows(&engine, query).await);
        }
        assert_eq!(results[0], results[1], "{query}");
        results.remove(0)
    }

    #[tokio::test]
    async fn hop_after_a_trail_of_its_type_keeps_the_edges_apart() {
        // From 2 the last hop returns to 1 over the parallel edge the trail did not take.
        let query = "MATCH (:N {id: 1})-[:R*1..2]-(f:N)-[:R]-(g:N)";
        assert_eq!(
            trail(&format!("{query} RETURN f.id, g.id")).await,
            vec![vec![2, 1], vec![2, 1], vec![2, 3], vec![2, 3]]
        );
        assert_eq!(
            trail(&format!("{query} RETURN count(*)")).await,
            vec![vec![4]]
        );
    }

    #[tokio::test]
    async fn segments_in_different_directions_and_of_zero_length() {
        let query = "MATCH (:N {id: 1})-[:R*0..1]->(f:N)<-[:R]-(g:N) RETURN f.id, g.id";
        assert_eq!(trail(query).await, vec![vec![2, 1], vec![2, 1]]);
        let query = "MATCH (:N {id: 3})<-[:R]-(f:N)<-[:R*1..2]-(g:N) RETURN f.id, g.id";
        assert_eq!(trail(query).await, vec![vec![2, 1], vec![2, 1]]);
    }

    #[tokio::test]
    async fn segment_ends_bound_in_the_pattern_or_by_earlier_rows() {
        let query = "MATCH (a:N {id: 1})-[:R*1..2]-(f:N)-[:R]-(a) RETURN f.id";
        assert_eq!(trail(query).await, vec![vec![2], vec![2]]);
        let query = "MATCH (g:N {id: 3}) \
                     MATCH (:N {id: 1})-[:R*1..2]-(f:N)-[:R]-(g) RETURN f.id, g.id";
        assert_eq!(trail(query).await, vec![vec![2, 3], vec![2, 3]]);
        let query = "MATCH (:N {id: 1})-[:R*1..2]-(f:N {id: 2})-[:R]-(g:N) RETURN g.id";
        assert_eq!(trail(query).await, vec![vec![1], vec![1], vec![3], vec![3]]);
    }

    #[tokio::test]
    async fn residual_shapes_keep_the_edge_inventory() {
        let expected = vec![vec![2, 1], vec![2, 1], vec![2, 3], vec![2, 3]];
        // A relationship variable on a segment.
        let named = "MATCH (:N {id: 1})-[:R*1..2]-(f:N)-[r:R]-(g:N) RETURN f.id, g.id";
        // Without the CSR, 10 × 10 segment lengths exceed the joins' branch limit.
        let long = "MATCH (:N {id: 1})-[:R*1..10]-(f:N)-[:R*1..10]-(g:N) RETURN f.id, g.id";
        for use_csr in [false, true] {
            let engine = engine(use_csr).await;
            for (query, inventory) in [(named, true), (long, !use_csr)] {
                let plan = engine.explain(query, HashMap::new()).await.unwrap();
                assert_eq!(plan.contains("PathMatchExec"), inventory, "{query}: {plan}");
                assert_eq!(rows(&engine, query).await, expected, "{query}");
            }
        }
    }
}
