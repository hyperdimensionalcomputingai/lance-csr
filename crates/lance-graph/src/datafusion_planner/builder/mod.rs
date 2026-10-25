// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Plan Building Phase
//!
//! Converts logical operators into DataFusion logical plans
//!
//! This module is split into several submodules for better organization:
//! - `basic_ops`: Basic operations (filter, project, sort, limit, offset, distinct)
//! - `expand_ops`: Graph traversal operations (expand, variable-length expand)
//! - `aggregate_ops`: Aggregation and grouping operations
//! - `join_builder`: Join inference and building
//! - `helpers`: Utility functions

mod aggregate_ops;
mod basic_ops;
mod comprehension_ops;
mod expand_ops;
mod helpers;
mod join_builder;
mod path_ops;

use super::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};

use super::analysis::PlanningContext;

impl DataFusionPlanner {
    /// Phase 2: Build DataFusion LogicalPlan from logical operator with context
    pub(crate) fn build_operator(
        &self,
        ctx: &mut PlanningContext,
        op: &LogicalOperator,
    ) -> Result<LogicalPlan> {
        match op {
            LogicalOperator::SingleRow => LogicalPlanBuilder::empty(true)
                .build()
                .map_err(|e| self.plan_error("Failed to build singleton row", e)),
            LogicalOperator::ScanByLabel {
                variable,
                label,
                properties,
                ..
            } => self.build_scan(ctx, variable, label, properties),
            LogicalOperator::Filter { input, predicate } => {
                self.build_filter(ctx, input, predicate)
            }
            LogicalOperator::Project { input, projections } => {
                self.build_project(ctx, input, projections)
            }
            LogicalOperator::Distinct { input } => self.build_distinct(ctx, input),
            LogicalOperator::Sort { input, sort_items } => self.build_sort(ctx, input, sort_items),
            LogicalOperator::Limit { input, count } => self.build_limit(ctx, input, count),
            LogicalOperator::Offset { input, offset } => self.build_offset(ctx, input, offset),
            LogicalOperator::Expand {
                input,
                source_variable,
                target_variable,
                target_label,
                relationship_types,
                direction,
                relationship_variable,
                properties,
                target_properties,
            } => self.build_expand(
                ctx,
                input,
                source_variable,
                target_variable,
                target_label,
                relationship_types,
                direction,
                relationship_variable.as_deref(),
                properties,
                target_properties,
            ),
            LogicalOperator::VariableLengthExpand {
                input,
                source_variable,
                target_variable,
                relationship_types,
                direction,
                min_length,
                max_length,
                target_properties,
                ..
            } => self.build_variable_length_expand(
                ctx,
                input,
                source_variable,
                target_variable,
                relationship_types,
                direction,
                *min_length,
                *max_length,
                target_properties,
            ),
            LogicalOperator::Join {
                left,
                right,
                join_type,
            } => self.build_join(ctx, left, right, join_type),
            LogicalOperator::Unwind {
                input,
                expression,
                alias,
                label,
            } => self.build_unwind(ctx, input, expression, alias, label.as_deref()),
            LogicalOperator::Union { inputs } => self.build_union(ctx, inputs),
            LogicalOperator::QueryUnion { left, right, all } => {
                self.build_query_union(ctx, left, right, *all)
            }
            LogicalOperator::ListComprehension {
                input,
                list,
                variable,
                label,
                element,
                keep,
                map,
                output,
            } => self.build_list_comprehension(
                ctx,
                input,
                list,
                (variable, label.as_deref()),
                element,
                keep.as_ref(),
                map,
                output,
            ),
            LogicalOperator::ComprehensionElement => Ok(ctx
                .comprehension_elements
                .last()
                .expect("element rows are built inside their comprehension")
                .clone()),
            LogicalOperator::OptionalMatch {
                input,
                optional,
                keys,
            } => self.build_optional_match(ctx, input, optional, keys),
            LogicalOperator::PathMatch { input, path } => self.build_path_match(ctx, input, path),
            LogicalOperator::ShortestPath {
                input,
                source_variable,
                target_variable,
                relationship_type,
                direction,
                min_hops,
                max_hops,
                all,
                path_variable,
            } => self.build_shortest_path(
                ctx,
                input,
                [source_variable.as_str(), target_variable.as_str()],
                relationship_type,
                direction,
                (*min_hops, *max_hops),
                *all,
                path_variable,
            ),
        }
    }
}
