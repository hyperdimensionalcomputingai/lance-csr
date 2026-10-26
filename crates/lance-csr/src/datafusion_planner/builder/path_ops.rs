// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

use crate::ast::PathPattern;
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::{analysis::PlanningContext, DataFusionPlanner};
use crate::error::{GraphError, Result};
use crate::logical_plan::LogicalOperator;
use crate::path_inventory::{EdgeSource, NodeSource};
use crate::path_match::{Binding, PathMatchNode, PathSpec, Step};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;

impl DataFusionPlanner {
    pub(crate) fn build_path_match(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        path: &PathPattern,
    ) -> Result<LogicalPlan> {
        let error = |message: String| GraphError::UnsupportedFeature {
            feature: message,
            location: snafu::Location::new(file!(), line!(), column!()),
        };
        let catalog = self.get_catalog()?;
        let input = self.build_operator(ctx, input)?;
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
            edges.push(EdgeSource {
                label: mapping.relationship_type.clone(),
                source_label: node_labels
                    .iter()
                    .position(|l| Some(l) == mapping.source_label.as_ref())
                    .unwrap(),
                target_label: node_labels
                    .iter()
                    .position(|l| Some(l) == mapping.target_label.as_ref())
                    .unwrap(),
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
                properties: node.properties.clone(),
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
                            .unwrap()
                    })
                    .collect(),
                direction: rel.direction.clone(),
                min,
                max,
                relationship: rel.variable.clone(),
                properties: rel.properties.clone(),
            });
        }
        let variable = path
            .variable
            .as_ref()
            .or_else(|| path.shortest.as_ref().and_then(|s| s.variable.as_ref()))
            .cloned()
            .ok_or_else(|| error("rich path needs a variable".into()))?;
        let node = PathMatchNode::try_new(
            plans,
            PathSpec {
                variable,
                sources: ctx.path_sources.clone(),
                nodes,
                edges,
                bindings,
                steps,
                shortest,
            },
        )
        .map_err(|e| self.plan_error("Failed to plan path value", e))?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}
