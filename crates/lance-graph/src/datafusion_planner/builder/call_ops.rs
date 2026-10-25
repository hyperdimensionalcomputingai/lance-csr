// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

use super::super::{analysis, DataFusionPlanner, PlanningContext};
use crate::apply::ApplyNode;
use crate::error::Result;
use crate::logical_plan::LogicalOperator;
use datafusion::logical_expr::{
    col, expr::Placeholder, Expr, Extension, LogicalPlan, LogicalPlanBuilder,
};
use std::sync::Arc;

impl DataFusionPlanner {
    pub(crate) fn build_apply(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        inner: &LogicalOperator,
        names: &[String],
        id: usize,
    ) -> Result<LogicalPlan> {
        let input = self.build_operator(ctx, input)?;
        // A scalar named x wins over entity-shaped names such as x__property.
        let entity_prefixes: Vec<_> = names
            .iter()
            .filter(|name| {
                !input
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == *name)
            })
            .map(|name| format!("{name}__"))
            .collect();
        let mut imports = Vec::new();
        let mut values = Vec::new();
        for (index, (_, field)) in input.schema().iter().enumerate() {
            if names.contains(field.name())
                || entity_prefixes
                    .iter()
                    .any(|prefix| field.name().starts_with(prefix))
            {
                // User parameters have already become literals. The monotonically assigned
                // CALL id also separates nested and sibling import placeholders.
                let parameter = format!("__call_{id}_{index}");
                imports.push((field.name().clone(), parameter.clone()));
                values.push(Expr::Placeholder(Placeholder::new_with_field(
                    format!("${parameter}"),
                    Some(field.clone()),
                )));
            }
        }
        // DataFusion reconstructs VALUES with column1, column2, ... when binding
        // placeholders. Keep those names and restore the imported names in a projection.
        let seed =
            if imports.is_empty() {
                LogicalPlanBuilder::empty(true).build()
            } else {
                LogicalPlanBuilder::values(vec![values]).and_then(|builder| {
                    builder
                        .project(imports.iter().enumerate().map(|(index, (name, _))| {
                            col(format!("column{}", index + 1)).alias(name)
                        }))?
                        .build()
                })
            }
            .map_err(|error| self.plan_error("CALL imports", error))?;
        let analysis = analysis::analyze(inner)?;
        let mut child_ctx = PlanningContext::new(&analysis);
        child_ctx.call_input = Some(seed);
        let child = self.build_operator(&mut child_ctx, inner)?;
        let node = ApplyNode::try_new(input, child, imports)
            .map_err(|error| self.plan_error("CALL output", error))?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}
