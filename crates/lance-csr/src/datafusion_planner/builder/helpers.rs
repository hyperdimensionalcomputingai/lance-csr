// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Helper utilities for plan building

use crate::datafusion_planner::DataFusionPlanner;
use crate::logical_plan::*;

impl DataFusionPlanner {
    /// Extract all variables from a logical operator tree
    ///
    /// Variables come from:
    /// - Node variables: ScanByLabel, Expand source/target
    /// - Relationship variables: Expand and VariableLengthExpand relationship_variable
    pub(crate) fn extract_variables(&self, op: &LogicalOperator) -> Vec<String> {
        let mut vars = Vec::new();
        Self::collect_variables(op, &mut vars);
        vars.sort();
        vars.dedup();
        vars
    }

    /// Recursively collect variables from a logical operator
    ///
    /// Collects both node variables and relationship variables to support
    /// join key inference when patterns share relationship aliases.
    fn collect_variables(op: &LogicalOperator, vars: &mut Vec<String>) {
        match op {
            // Base case: ScanByLabel introduces a node variable
            LogicalOperator::ScanByLabel { variable, .. } => {
                vars.push(variable.clone());
            }
            // Unary operators: recurse into input
            LogicalOperator::Filter { input, .. } => {
                Self::collect_variables(input, vars);
            }
            LogicalOperator::Project { input, .. } => {
                Self::collect_variables(input, vars);
            }
            LogicalOperator::Distinct { input } => {
                Self::collect_variables(input, vars);
            }
            LogicalOperator::Sort { input, .. } => {
                Self::collect_variables(input, vars);
            }
            LogicalOperator::Limit { input, .. } => {
                Self::collect_variables(input, vars);
            }
            LogicalOperator::Offset { input, .. } => {
                Self::collect_variables(input, vars);
            }
            // Expand: recurse into input and add source, target, and relationship variables
            LogicalOperator::Expand {
                input,
                source_variable,
                target_variable,
                relationship_variable,
                ..
            } => {
                Self::collect_variables(input, vars);
                vars.push(source_variable.clone());
                vars.push(target_variable.clone());
                // Also collect relationship variable if present
                if let Some(rel_var) = relationship_variable {
                    vars.push(rel_var.clone());
                }
            }
            LogicalOperator::VariableLengthExpand {
                input,
                source_variable,
                target_variable,
                relationship_variable,
                ..
            } => {
                Self::collect_variables(input, vars);
                vars.push(source_variable.clone());
                vars.push(target_variable.clone());
                // Also collect relationship variable if present
                if let Some(rel_var) = relationship_variable {
                    vars.push(rel_var.clone());
                }
            }
            // Binary operator: recurse into both left and right
            LogicalOperator::Join { left, right, .. }
            | LogicalOperator::QueryUnion { left, right, .. } => {
                Self::collect_variables(left, vars);
                Self::collect_variables(right, vars);
            }
            LogicalOperator::Unwind { input, alias, .. } => {
                if let Some(op) = input {
                    Self::collect_variables(op, vars);
                }
                vars.push(alias.clone());
            }
            // The optional side is planned on top of the input's variables.
            LogicalOperator::OptionalMatch { optional, .. } => {
                Self::collect_variables(optional, vars);
            }
            LogicalOperator::PathMatch { input, path, .. } => {
                Self::collect_variables(input, vars);
                vars.extend(
                    std::iter::once(&path.start_node)
                        .chain(path.segments.iter().map(|s| &s.end_node))
                        .filter_map(|n| n.variable.clone()),
                );
                vars.extend(path.variable.clone());
            }
            LogicalOperator::ShortestPath {
                input,
                path_variable,
                ..
            } => {
                Self::collect_variables(input, vars);
                vars.push(path_variable.clone());
            }
            // Every branch binds the same variables.
            LogicalOperator::Union { inputs } => {
                if let Some(first) = inputs.first() {
                    Self::collect_variables(first, vars);
                }
            }
            LogicalOperator::ListComprehension { input, .. } => {
                Self::collect_variables(input, vars)
            }
            LogicalOperator::Apply { input, labels, .. } => {
                Self::collect_variables(input, vars);
                vars.extend(labels.keys().cloned());
            }
            LogicalOperator::CallInput { labels } => vars.extend(labels.keys().cloned()),
            LogicalOperator::ComprehensionElement | LogicalOperator::SingleRow => {}
        }
    }

    /// Labels that one branch of a [`LogicalOperator::Union`] binds to its variables.
    pub(crate) fn collect_labels(
        op: &LogicalOperator,
        labels: &mut std::collections::HashMap<String, String>,
    ) {
        match op {
            LogicalOperator::ScanByLabel {
                variable, label, ..
            } => {
                labels.insert(variable.clone(), label.clone());
            }
            LogicalOperator::Expand {
                input,
                target_variable,
                target_label,
                ..
            } => {
                Self::collect_labels(input, labels);
                labels.insert(target_variable.clone(), target_label.clone());
            }
            LogicalOperator::VariableLengthExpand {
                input,
                source_variable,
                target_variable,
                target_label,
                ..
            } => {
                Self::collect_labels(input, labels);
                let label = crate::datafusion_planner::analysis::variable_length_target_label(
                    target_label,
                    labels.get(source_variable),
                );
                labels.insert(target_variable.clone(), label);
            }
            LogicalOperator::Filter { input, .. }
            | LogicalOperator::Project { input, .. }
            | LogicalOperator::Sort { input, .. }
            | LogicalOperator::Limit { input, .. }
            | LogicalOperator::Offset { input, .. }
            | LogicalOperator::Distinct { input } => Self::collect_labels(input, labels),
            LogicalOperator::Join { left, right, .. }
            | LogicalOperator::QueryUnion { left, right, .. } => {
                Self::collect_labels(left, labels);
                Self::collect_labels(right, labels);
            }
            LogicalOperator::Unwind {
                input,
                alias,
                label,
                ..
            } => {
                if let Some(op) = input {
                    Self::collect_labels(op, labels);
                }
                if let Some(label) = label {
                    labels.insert(alias.clone(), label.clone());
                }
            }
            LogicalOperator::OptionalMatch { optional, .. } => {
                Self::collect_labels(optional, labels)
            }
            LogicalOperator::PathMatch { input, path, .. } => {
                Self::collect_labels(input, labels);
                for node in std::iter::once(&path.start_node)
                    .chain(path.segments.iter().map(|s| &s.end_node))
                {
                    if let (Some(variable), Some(label)) = (&node.variable, node.labels.first()) {
                        labels.insert(variable.clone(), label.clone());
                    }
                }
            }
            LogicalOperator::ShortestPath { input, .. } => Self::collect_labels(input, labels),
            LogicalOperator::Union { .. } => {}
            LogicalOperator::ListComprehension { input, .. } => Self::collect_labels(input, labels),
            LogicalOperator::Apply {
                input,
                labels: exports,
                ..
            } => {
                Self::collect_labels(input, labels);
                labels.extend(exports.clone());
            }
            LogicalOperator::CallInput { labels: imports } => labels.extend(imports.clone()),
            LogicalOperator::ComprehensionElement | LogicalOperator::SingleRow => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::RelationshipDirection;
    use crate::datafusion_planner::{
        test_fixtures::{make_catalog, person_config},
        DataFusionPlanner,
    };
    use crate::logical_plan::{JoinType, LogicalOperator};
    use std::collections::HashMap;

    #[test]
    fn test_collect_variables_includes_relationship_variables() {
        // Test that collect_variables now captures relationship variables
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        // Build: (a:Person)-[r:KNOWS]->(b:Person)
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };
        let expand = LogicalOperator::Expand {
            input: Box::new(scan_a),
            source_variable: "a".to_string(),
            target_variable: "b".to_string(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".to_string()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: Some("r".to_string()),
            properties: Default::default(),
            target_properties: Default::default(),
        };

        let vars = planner.extract_variables(&expand);

        // Should contain: a (source), b (target), r (relationship)
        assert!(
            vars.contains(&"a".to_string()),
            "Should contain source variable 'a'"
        );
        assert!(
            vars.contains(&"b".to_string()),
            "Should contain target variable 'b'"
        );
        assert!(
            vars.contains(&"r".to_string()),
            "Should contain relationship variable 'r'"
        );
    }

    #[test]
    fn test_extract_variables_deduplicates_and_sorts() {
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        // Left pattern: (a)-[r]->(b)
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };
        let left_expand = LogicalOperator::Expand {
            input: Box::new(scan_a),
            source_variable: "a".to_string(),
            target_variable: "b".to_string(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".to_string()],
            direction: RelationshipDirection::Outgoing,
            relationship_variable: Some("r".to_string()),
            properties: Default::default(),
            target_properties: Default::default(),
        };

        // Right pattern: (b)-[r2]->(c)
        let scan_b = LogicalOperator::ScanByLabel {
            variable: "b".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };
        let right_expand = LogicalOperator::Expand {
            input: Box::new(scan_b),
            source_variable: "b".to_string(),
            target_variable: "c".to_string(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".to_string()],
            direction: RelationshipDirection::Outgoing,
            relationship_variable: Some("r2".to_string()),
            properties: Default::default(),
            target_properties: Default::default(),
        };

        let join_plan = LogicalOperator::Join {
            left: Box::new(left_expand),
            right: Box::new(right_expand),
            join_type: JoinType::Inner,
        };

        let vars = planner.extract_variables(&join_plan);

        assert_eq!(vars, vec!["a", "b", "c", "r", "r2"]);
    }

    #[test]
    fn test_extract_variables_handles_varlength_expand_without_relationship_var() {
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "a".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        let varlength = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(1),
            max_length: Some(3),
            target_properties: HashMap::new(),
        };

        let vars = planner.extract_variables(&varlength);

        assert_eq!(vars, vec!["a", "b"]);
    }
}
