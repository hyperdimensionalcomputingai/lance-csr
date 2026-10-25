// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Graph traversal operations: Expand and Variable-Length Expand

use crate::ast::RelationshipDirection;
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::analysis::PlanningContext;
use crate::datafusion_planner::join_ops::{SourceJoinParams, TargetJoinParams};
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashMap;
use std::sync::Arc;

impl DataFusionPlanner {
    /// Build a relationship expansion (graph traversal) as a series of joins
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_expand(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        source_variable: &str,
        target_variable: &str,
        target_label: &str,
        relationship_types: &[String],
        direction: &RelationshipDirection,
        relationship_variable: Option<&str>,
        relationship_properties: &HashMap<String, crate::ast::PropertyValue>,
        target_properties: &HashMap<String, crate::ast::PropertyValue>,
    ) -> Result<LogicalPlan> {
        let left_plan = self.build_operator(ctx, input)?;
        self.expand_plan(
            ctx,
            left_plan,
            source_variable,
            target_variable,
            target_label,
            relationship_types,
            direction,
            relationship_variable,
            relationship_properties,
            target_properties,
        )
    }

    /// One relationship hop on top of `left_plan`: a CSR expansion when one applies,
    /// otherwise relationship scan + joins. Shared by single hops and by every hop of
    /// a variable-length path.
    #[allow(clippy::too_many_arguments)]
    fn expand_plan(
        &self,
        ctx: &mut PlanningContext,
        left_plan: LogicalPlan,
        source_variable: &str,
        target_variable: &str,
        target_label: &str,
        relationship_types: &[String],
        direction: &RelationshipDirection,
        relationship_variable: Option<&str>,
        relationship_properties: &HashMap<String, crate::ast::PropertyValue>,
        target_properties: &HashMap<String, crate::ast::PropertyValue>,
    ) -> Result<LogicalPlan> {
        // Get the unique relationship instance for this expand operation
        let Some(cat) = &self.catalog else {
            // Fallback: pass-through if catalog not available
            return Ok(left_plan);
        };

        let Some(rel_type) = relationship_types.first() else {
            return Ok(left_plan);
        };

        let rel_instance = ctx.next_relationship_instance(rel_type)?;
        // Use case-insensitive lookups
        let Some(rel_map) = self.config.get_relationship_mapping(rel_type) else {
            return Ok(left_plan);
        };

        // Resolves the `_temp_` variables of variable-length paths too.
        let Ok((_, node_map)) = self.get_target_node_mapping(ctx, source_variable) else {
            return Ok(left_plan);
        };

        let Some(rel_source) = cat.relationship_source(&rel_map.relationship_type) else {
            return Ok(left_plan);
        };

        // A bound relationship variable or relationship property filter needs the
        // relationship's other columns, which the CSR does not carry.
        let csr_eligible = relationship_variable.is_none()
            && relationship_properties.is_empty()
            && relationship_types.len() == 1;

        // Join relationship with target node using the explicit target_label (case-insensitive)
        let target_node_map = self.config.get_node_mapping(target_label).ok_or_else(|| {
            crate::error::GraphError::ConfigError {
                message: format!("No mapping found for target label: {}", target_label),
                location: snafu::Location::new(file!(), line!(), column!()),
            }
        })?;

        // Inline target property filters are applied by the target scan, so they keep the join.
        let target_lookup = if csr_eligible && target_properties.is_empty() {
            self.csr_target_lookup(
                cat,
                &left_plan,
                target_variable,
                target_label,
                target_node_map,
            )
        } else {
            None
        };
        let fused_target = target_lookup.is_some();
        // A target bound earlier in the pattern makes this a closing edge (a cycle):
        // the CSR keeps only neighbours equal to that id instead of expanding them all.
        let bound_target = qualify_column(target_variable, &target_node_map.id_field);
        let closing = csr_eligible
            && target_properties.is_empty()
            && left_plan
                .schema()
                .field_with_unqualified_name(&bound_target)
                .is_ok_and(|f| crate::csr_expand::is_supported_id_type(f.data_type()));
        let csr_plan = if csr_eligible {
            self.try_build_csr_expand(
                &left_plan,
                &rel_instance.alias,
                rel_type,
                rel_map,
                &qualify_column(source_variable, &node_map.id_field),
                &rel_source.schema(),
                direction,
                target_lookup,
            )?
        } else {
            None
        };

        let builder = match csr_plan {
            // The CSR node already fetched the target rows: no target join.
            Some(plan) if fused_target => return Ok(plan),
            Some(plan) if closing => return Self::with_closing_column(plan, bound_target),
            Some(plan) => LogicalPlanBuilder::from(plan),
            None => {
                // Build relationship scan with qualified columns and property filters
                let rel_scan = self.build_relationship_scan(
                    &rel_instance,
                    rel_source,
                    relationship_properties,
                    rel_map,
                    direction,
                )?;

                // Join source node with relationship
                let source_params = SourceJoinParams {
                    source_variable,
                    rel_qualifier: &rel_instance.alias,
                    node_id_field: &node_map.id_field,
                    rel_map,
                    direction,
                };
                self.join_source_to_relationship(left_plan, rel_scan, &source_params)?
            }
        };

        let target_params = TargetJoinParams {
            target_variable,
            target_label,
            rel_qualifier: &rel_instance.alias,
            node_map: target_node_map,
            rel_map,
            direction,
            target_properties,
        };
        self.join_relationship_to_target(builder, cat, &target_params)
    }

    /// Replace "relationship scan + source join" with a CSR lookup, if an adjacency
    /// exists for this relationship and direction and every id column is an integer.
    ///
    /// The node outputs the two endpoint columns under the names the relationship
    /// scan would use, so the target join that follows is unchanged.
    #[allow(clippy::too_many_arguments)]
    fn try_build_csr_expand(
        &self,
        left_plan: &LogicalPlan,
        rel_alias: &str,
        rel_type: &str,
        rel_map: &crate::config::RelationshipMapping,
        key_column: &str,
        rel_schema: &arrow_schema::SchemaRef,
        direction: &RelationshipDirection,
        target: Option<crate::csr_expand::TargetLookup>,
    ) -> Result<Option<LogicalPlan>> {
        use crate::csr_expand::{is_supported_id_type, CsrExpandNode, Neighbors};
        use datafusion::logical_expr::Extension;

        let Some(catalog) = self.adjacency.as_ref() else {
            return Ok(None);
        };
        let adjacency = match direction {
            RelationshipDirection::Outgoing => catalog
                .get(rel_type, true)
                .cloned()
                .map(Neighbors::directed),
            RelationshipDirection::Incoming => catalog
                .get(rel_type, false)
                .cloned()
                .map(Neighbors::directed),
            // Outgoing endpoint names, as the reversed-edge scan of the join path uses.
            RelationshipDirection::Undirected => catalog
                .get(rel_type, true)
                .zip(catalog.get(rel_type, false))
                .map(|(out, inc)| Neighbors::undirected(out.clone(), inc.clone())),
        };
        let Some(adjacency) = adjacency else {
            return Ok(None);
        };
        let rel_field = |name: &str| {
            rel_schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(name))
                .cloned()
        };
        let key_field = rel_field(Self::get_source_join_key(direction, rel_map));
        let neighbor_field = rel_field(Self::get_target_join_key(direction, rel_map));
        let source_key = left_plan
            .schema()
            .field_with_unqualified_name(key_column)
            .ok();
        let (Some(key_field), Some(neighbor_field), Some(source_key)) =
            (key_field, neighbor_field, source_key)
        else {
            return Ok(None);
        };
        if ![
            key_field.data_type(),
            neighbor_field.data_type(),
            source_key.data_type(),
        ]
        .iter()
        .all(|t| is_supported_id_type(t))
        {
            return Ok(None);
        }

        let output = |f: &arrow_schema::Field| {
            arrow_schema::Field::new(
                qualify_column(rel_alias, f.name()),
                f.data_type().clone(),
                true,
            )
        };
        let node = CsrExpandNode::try_new(
            left_plan.clone(),
            key_column.to_string(),
            output(&key_field),
            output(&neighbor_field),
            adjacency,
            target,
        )?;
        Ok(Some(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    /// Mark a freshly built CSR expansion as a closing edge on `bound_target`.
    fn with_closing_column(plan: LogicalPlan, bound_target: String) -> Result<LogicalPlan> {
        use crate::csr_expand::CsrExpandNode;
        use datafusion::logical_expr::Extension;

        let LogicalPlan::Extension(ext) = &plan else {
            return Ok(plan);
        };
        let Some(node) = ext.node.as_any().downcast_ref::<CsrExpandNode>() else {
            return Ok(plan);
        };
        let node = node.clone().with_closing_column(bound_target);
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// A fused target lookup for `target_variable`, if a node index exists for its
    /// label and the variable is not already bound (reuse needs the join's filter).
    fn csr_target_lookup(
        &self,
        cat: &Arc<dyn crate::GraphSourceCatalog>,
        left_plan: &LogicalPlan,
        target_variable: &str,
        label: &str,
        target_node_map: &crate::config::NodeMapping,
    ) -> Option<crate::csr_expand::TargetLookup> {
        let rows = self.adjacency.as_ref()?.nodes(label)?.clone();
        let bound = qualify_column(target_variable, &target_node_map.id_field);
        if left_plan
            .schema()
            .field_with_unqualified_name(&bound)
            .is_ok()
        {
            return None;
        }
        let fields = cat
            .node_source(label)?
            .schema()
            .fields()
            .iter()
            .map(|f| {
                arrow_schema::Field::new(
                    qualify_column(target_variable, f.name()),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();
        Some(crate::csr_expand::TargetLookup::new(
            target_variable.to_string(),
            rows,
            fields,
        ))
    }

    /// A [`crate::shortest_path::ShortestPathNode`] over `input`: the CSR when the engine has
    /// one for this type and direction, else a scan of the relationship's endpoint columns.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_shortest_path(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        [source_variable, target_variable]: [&str; 2],
        rel_type: &str,
        direction: &RelationshipDirection,
        (min_hops, max_hops): (u32, u32),
        all: bool,
        path_variable: &str,
    ) -> Result<LogicalPlan> {
        use crate::csr_expand::Neighbors;
        use crate::shortest_path::{SearchDirection, ShortestPathNode, ShortestPathSpec};
        use datafusion::logical_expr::Extension;

        let input_plan = self.build_operator(ctx, input)?;
        let rel_map = self.get_relationship_mapping(rel_type)?;
        let (_, source_map) = self.get_target_node_mapping(ctx, source_variable)?;
        let (_, target_map) = self.get_target_node_mapping(ctx, target_variable)?;
        let search = match direction {
            RelationshipDirection::Outgoing => SearchDirection::Outgoing,
            RelationshipDirection::Incoming => SearchDirection::Incoming,
            RelationshipDirection::Undirected => SearchDirection::Undirected,
        };
        let adjacency = self.adjacency.as_ref().and_then(|cat| match search {
            SearchDirection::Outgoing => cat.get(rel_type, true).cloned().map(Neighbors::directed),
            SearchDirection::Incoming => cat.get(rel_type, false).cloned().map(Neighbors::directed),
            SearchDirection::Undirected => cat
                .get(rel_type, true)
                .zip(cat.get(rel_type, false))
                .map(|(out, inc)| Neighbors::undirected(out.clone(), inc.clone())),
        });
        let source = self
            .get_catalog()?
            .relationship_source(&rel_map.relationship_type)
            .ok_or_else(|| crate::error::GraphError::ConfigError {
                message: format!("No table source found for relationship: {rel_type}"),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        let schema = source.schema();
        let field = |name: &str| {
            schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(name))
                .ok_or_else(|| crate::error::GraphError::ConfigError {
                    message: format!("Relationship column '{name}' not found"),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })
        };
        let (src, dst) = (
            field(&rel_map.source_id_field)?,
            field(&rel_map.target_id_field)?,
        );
        let edges = match adjacency {
            Some(_) => None,
            None => Some(
                LogicalPlanBuilder::scan(rel_type.to_lowercase(), source, Some(vec![src, dst]))
                    .and_then(|b| b.build())
                    .map_err(|e| self.plan_error("Failed to scan edges for shortestPath", e))?,
            ),
        };
        let spec = ShortestPathSpec {
            source_column: qualify_column(source_variable, &source_map.id_field),
            target_column: qualify_column(target_variable, &target_map.id_field),
            direction: search,
            min_hops,
            max_hops,
            all,
            length_column: qualify_column(path_variable, "length"),
            nodes_column: qualify_column(path_variable, "nodes"),
            edge_source: schema.field(src).name().clone(),
            edge_target: schema.field(dst).name().clone(),
        };
        let node = ShortestPathNode::try_new(input_plan, edges, adjacency, spec)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Use one iterative CSR expansion when indexed; otherwise union the fixed-length plans.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_variable_length_expand(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        source_variable: &str,
        target_variable: &str,
        relationship_types: &[String],
        direction: &RelationshipDirection,
        min_length: Option<u32>,
        max_length: Option<u32>,
        target_properties: &HashMap<String, crate::ast::PropertyValue>,
    ) -> Result<LogicalPlan> {
        let min_hops = min_length.unwrap_or(1);
        let max_hops = max_length.unwrap_or(crate::MAX_VARIABLE_LENGTH_HOPS);

        // Validate range
        if min_hops > max_hops {
            return Err(crate::error::GraphError::InvalidPattern {
                message: format!(
                    "Invalid variable-length range: min {} > max {}",
                    min_hops, max_hops
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        if max_hops > crate::MAX_VARIABLE_LENGTH_HOPS {
            return Err(crate::error::GraphError::UnsupportedFeature {
                feature: format!(
                    "Variable-length paths with max length > {} (got {})",
                    crate::MAX_VARIABLE_LENGTH_HOPS,
                    max_hops
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        // Build the input plan (source node scan)
        let input_plan = self.build_operator(ctx, input)?;

        // Derive expected column names from source and target node schemas
        // This ensures we only project columns that actually belong to source/target nodes
        let mut expected_columns =
            self.get_expected_varlength_columns(ctx, source_variable, target_variable)?;
        // Everything the input carries (other nodes, scalars from a WITH) passes through.
        expected_columns.extend(
            input_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone()),
        );

        if let Some(plan) = self.try_build_csr_traversal(
            ctx,
            input_plan.clone(),
            source_variable,
            target_variable,
            relationship_types,
            direction,
            min_hops,
            max_hops,
            target_properties,
        )? {
            let projection: Vec<_> = plan
                .schema()
                .fields()
                .iter()
                .filter(|f| expected_columns.contains(f.name().as_str()))
                .map(|f| col(f.name()))
                .collect();
            return LogicalPlanBuilder::from(plan)
                .project(projection)
                .and_then(|p| p.build())
                .map_err(|e| self.plan_error("Failed to project variable-length CSR", e));
        }

        // Generate a plan for each hop count and UNION them
        let mut plans = Vec::new();

        for hop_count in min_hops..=max_hops {
            let mut plan = self.build_fixed_length_path(
                ctx,
                input_plan.clone(),
                source_variable,
                target_variable,
                relationship_types,
                direction,
                hop_count,
                target_properties,
            )?;

            // Project only source and target columns to ensure consistent schema for UNION
            // This removes intermediate node columns that vary by hop count
            // Use the pre-computed expected column set derived from actual node schemas
            let projection: Vec<datafusion::logical_expr::Expr> = plan
                .schema()
                .fields()
                .iter()
                .filter(|f| expected_columns.contains(f.name().as_str()))
                .map(|f| col(f.name()))
                .collect();

            plan = LogicalPlanBuilder::from(plan)
                .project(projection)
                .map_err(|e| crate::error::GraphError::PlanError {
                    message: format!("Failed to project for UNION: {}", e),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?
                .build()
                .map_err(|e| crate::error::GraphError::PlanError {
                    message: format!("Failed to build projection: {}", e),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

            plans.push(plan);
        }

        // UNION all plans together
        if plans.len() == 1 {
            Ok(plans.into_iter().next().unwrap())
        } else {
            // Build UNION of all plans
            let mut union_plan = plans[0].clone();
            for plan in plans.into_iter().skip(1) {
                union_plan = LogicalPlanBuilder::from(union_plan)
                    .union(plan)
                    .map_err(|e| crate::error::GraphError::PlanError {
                        message: format!("Failed to UNION variable-length paths: {}", e),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?
                    .build()
                    .map_err(|e| crate::error::GraphError::PlanError {
                        message: format!("Failed to build UNION plan: {}", e),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;
            }
            Ok(union_plan)
        }
    }

    /// Build a fixed-length path of N hops
    ///
    /// For hop_count=3: (a)-[:KNOWS]->(temp1)-[:KNOWS]->(temp2)-[:KNOWS]->(b)
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_fixed_length_path(
        &self,
        ctx: &mut PlanningContext,
        input_plan: LogicalPlan,
        source_variable: &str,
        target_variable: &str,
        relationship_types: &[String],
        direction: &RelationshipDirection,
        hop_count: u32,
        target_properties: &HashMap<String, crate::ast::PropertyValue>,
    ) -> Result<LogicalPlan> {
        if hop_count == 0 {
            let (source_label, source_map) = self.get_target_node_mapping(ctx, source_variable)?;
            let (target_label, target_map) = self.get_target_node_mapping(ctx, target_variable)?;
            let source = col(qualify_column(source_variable, &source_map.id_field));
            let target = qualify_column(target_variable, &target_map.id_field);
            let same_label = source_label.eq_ignore_ascii_case(&target_label);
            let mut builder = LogicalPlanBuilder::from(input_plan);
            if builder
                .schema()
                .field_with_unqualified_name(&target)
                .is_ok()
            {
                if !target_properties.is_empty() {
                    return Err(crate::error::GraphError::PlanError {
                        message: format!("Property filters on reused variable '{target_variable}' are not yet supported"),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    });
                }
                builder = builder
                    .filter(
                        source
                            .eq(col(&target))
                            .and(datafusion::logical_expr::lit(same_label)),
                    )
                    .map_err(|e| self.plan_error("Failed to filter zero-hop identity", e))?;
            } else {
                let target_source =
                    self.get_catalog()?
                        .node_source(&target_label)
                        .ok_or_else(|| crate::error::GraphError::PlanError {
                            message: format!("No node source for {target_label}"),
                            location: snafu::Location::new(file!(), line!(), column!()),
                        })?;
                let scan = self.build_target_scan(
                    target_source,
                    &target_label,
                    target_variable,
                    target_properties,
                )?;
                builder = builder
                    .join_on(
                        scan,
                        datafusion::logical_expr::JoinType::Inner,
                        [source
                            .eq(col(&target))
                            .and(datafusion::logical_expr::lit(same_label))],
                    )
                    .map_err(|e| self.plan_error("Failed to join zero-hop identity", e))?;
            }
            return builder
                .build()
                .map_err(|e| self.plan_error("Failed to build zero-hop identity", e));
        }
        let mut current_plan = input_plan;
        let mut current_source = source_variable.to_string();
        let mut path_nodes = vec![current_source.clone()];

        for hop_index in 0..hop_count {
            let is_last_hop = hop_index == hop_count - 1;

            // Target variable: use actual target on last hop, temp variable otherwise
            let current_target = if is_last_hop {
                target_variable.to_string()
            } else {
                format!("_temp_{}_{}", source_variable, hop_index + 1)
            };

            // Build the expansion on top of current plan
            // Apply target properties only on the last hop
            let props_to_apply = if is_last_hop {
                target_properties
            } else {
                &HashMap::new()
            };

            current_plan = self.build_expand_on_plan(
                ctx,
                current_plan,
                &current_source,
                &current_target,
                relationship_types,
                direction,
                props_to_apply,
            )?;

            // Move to next hop
            path_nodes.push(current_target.clone());
            current_source = current_target;
        }

        if *direction != RelationshipDirection::Undirected {
            return Ok(current_plan);
        }
        // Cypher never reuses a relationship within a path, but an undirected hop can walk
        // straight back over the edge it just took (u-v-u). Rule that out by requiring each
        // node to differ from the one two hops earlier.
        // shortcut: exact only while no two distinct edges join the same pair of nodes (and
        // for paths of at most 3 hops); upgrade by carrying edge row ids through the hops.
        let mut builder = LogicalPlanBuilder::from(current_plan);
        for pair in path_nodes.windows(3) {
            let id = |var: &str| -> Result<_> {
                let (_, node_map) = self.get_target_node_mapping(ctx, var)?;
                Ok(col(qualify_column(var, &node_map.id_field)))
            };
            builder = builder
                .filter(id(&pair[0])?.not_eq(id(&pair[2])?))
                .map_err(|e| self.plan_error("Failed to filter backtracking hops", e))?;
        }
        builder
            .build()
            .map_err(|e| self.plan_error("Failed to build undirected path", e))
    }

    #[allow(clippy::too_many_arguments)]
    fn try_build_csr_traversal(
        &self,
        ctx: &mut PlanningContext,
        input: LogicalPlan,
        source_variable: &str,
        target_variable: &str,
        types: &[String],
        direction: &RelationshipDirection,
        min_hops: u32,
        max_hops: u32,
        target_properties: &HashMap<String, crate::ast::PropertyValue>,
    ) -> Result<Option<LogicalPlan>> {
        use crate::csr_expand::{CsrExpandNode, Neighbors};
        use datafusion::logical_expr::Extension;
        if max_hops == 0 {
            return Ok(None);
        }
        let Some(catalog) = &self.adjacency else {
            return Ok(None);
        };
        let (source_label, source_map) = self.get_target_node_mapping(ctx, source_variable)?;
        let (target_label, target_map) = self.get_target_node_mapping(ctx, target_variable)?;
        let Some(walk) = crate::csr_traversal::Traversal::new(
            &self.config,
            catalog,
            types,
            direction,
            &source_label,
            &target_label,
            min_hops,
            max_hops,
        ) else {
            return Ok(None);
        };
        let rel_type = &types[0];
        let rel_map = self.get_relationship_mapping(rel_type)?;
        let source_key = qualify_column(source_variable, &source_map.id_field);
        if !input
            .schema()
            .field_with_unqualified_name(&source_key)
            .is_ok_and(|f| crate::csr_expand::is_supported_id_type(f.data_type()))
        {
            return Ok(None);
        }
        let adjacency = match direction {
            RelationshipDirection::Outgoing => Neighbors::directed(
                catalog
                    .get(rel_type, true)
                    .expect("validated traversal")
                    .clone(),
            ),
            RelationshipDirection::Incoming => Neighbors::directed(
                catalog
                    .get(rel_type, false)
                    .expect("validated traversal")
                    .clone(),
            ),
            RelationshipDirection::Undirected => Neighbors::undirected(
                catalog
                    .get(rel_type, true)
                    .expect("validated traversal")
                    .clone(),
                catalog
                    .get(rel_type, false)
                    .expect("validated traversal")
                    .clone(),
            ),
        };
        let cat = self.get_catalog()?;
        let target = target_properties
            .is_empty()
            .then(|| {
                self.csr_target_lookup(cat, &input, target_variable, &target_label, target_map)
            })
            .flatten();
        let fused = target.is_some();
        let bound_target = qualify_column(target_variable, &target_map.id_field);
        let closing = input
            .schema()
            .field_with_unqualified_name(&bound_target)
            .is_ok();
        let alias = ctx.next_relationship_instance(rel_type)?.alias.clone();
        let output = |name| {
            arrow_schema::Field::new(
                qualify_column(&alias, name),
                arrow_schema::DataType::Int64,
                true,
            )
        };
        let mut node = CsrExpandNode::try_new(
            input,
            source_key,
            output(Self::get_source_join_key(direction, rel_map)),
            output(Self::get_target_join_key(direction, rel_map)),
            adjacency,
            target,
        )?
        .with_traversal(walk);
        if closing && target_properties.is_empty() {
            node = node.with_closing_column(bound_target);
        }
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        if fused || (closing && target_properties.is_empty()) {
            return Ok(Some(plan));
        }
        self.join_relationship_to_target(
            LogicalPlanBuilder::from(plan),
            cat,
            &TargetJoinParams {
                target_variable,
                target_label: &target_label,
                rel_qualifier: &alias,
                node_map: target_map,
                rel_map,
                direction,
                target_properties,
            },
        )
        .map(Some)
    }

    /// Build a single-hop expansion on top of an existing plan (one hop of a
    /// variable-length path). Uses the same planning as a single hop, so a hop gets
    /// the CSR expansion whenever a plain `(a)-[:R]->(b)` would.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_expand_on_plan(
        &self,
        ctx: &mut PlanningContext,
        input_plan: LogicalPlan,
        source_variable: &str,
        target_variable: &str,
        relationship_types: &[String],
        direction: &RelationshipDirection,
        target_properties: &HashMap<String, crate::ast::PropertyValue>,
    ) -> Result<LogicalPlan> {
        if relationship_types.is_empty() {
            return Err(crate::error::GraphError::InvalidPattern {
                message: "Expand requires at least one relationship type".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        // Fail on a missing mapping or catalog here: `expand_plan` passes the input
        // through in those cases, which would silently drop the hop.
        self.get_relationship_mapping(&relationship_types[0])?;
        self.get_catalog()?;
        let (target_label, _) = self.get_target_node_mapping(ctx, target_variable)?;
        self.expand_plan(
            ctx,
            input_plan,
            source_variable,
            target_variable,
            &target_label,
            relationship_types,
            direction,
            None,
            &HashMap::new(),
            target_properties,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{
        BooleanExpression, ComparisonOperator, PropertyRef, PropertyValue, ValueExpression,
    };
    use crate::datafusion_planner::{
        test_fixtures::{make_catalog, person_knows_config, person_scan},
        DataFusionPlanner, GraphPhysicalPlanner,
    };
    use crate::logical_plan::{LogicalOperator, ProjectionItem};
    use std::collections::HashMap;

    #[test]
    fn test_df_planner_expand_creates_join_filter() {
        // MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name
        let scan_a = person_scan("a");
        let expand = LogicalOperator::Expand {
            input: Box::new(scan_a),
            source_variable: "a".to_string(),
            target_variable: "b".to_string(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".to_string()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            properties: Default::default(),
            target_properties: Default::default(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(expand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };

        let cfg = person_knows_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&project).unwrap();

        let s = format!("{:?}", df_plan);
        assert!(
            s.contains("Join(") && s.contains("Inner"),
            "plan missing Inner Join: {}",
            s
        );
        assert!(
            s.contains("TableScan") && s.contains("person"),
            "plan missing person scan: {}",
            s
        );
        assert!(
            s.contains("TableScan") && (s.contains("KNOWS") || s.contains("knows")),
            "plan missing relationship scan: {}",
            s
        );
    }

    #[test]
    fn test_varlength_expand_placeholder_builds() {
        // MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name
        let scan_a = person_scan("a");
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: Some("r".into()),
            min_length: Some(1),
            max_length: Some(2),
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "a".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let planner = DataFusionPlanner::with_catalog(person_knows_config(), make_catalog());
        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(
            s.contains("Join(") && s.contains("Inner"),
            "missing Inner Join: {}",
            s
        );
    }

    #[test]
    fn test_varlength_expand_single_hop() {
        // MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) - equivalent to single hop
        let scan_a = person_scan("a");
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(1),
            max_length: Some(1),
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let planner = DataFusionPlanner::with_catalog(person_knows_config(), make_catalog());
        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should have joins but no UNION (only 1 hop)
        assert!(s.contains("Join("));
        // Single hop shouldn't have Union
        assert!(!s.contains("Union"));
    }

    #[test]
    fn test_varlength_expand_with_union() {
        // MATCH (a:Person)-[:KNOWS*2..3]->(b:Person) - should have UNION
        let scan_a = person_scan("a");
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(2),
            max_length: Some(3),
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table(
                "KNOWS",
                "Person",
                "Person",
                "src_person_id",
                "dst_person_id",
            )
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should have UNION for multiple hop counts
        assert!(s.contains("Union") || s.contains("union"));
        assert!(s.contains("Join("));
    }

    #[test]
    fn test_varlength_expand_default_min() {
        // MATCH (a:Person)-[:KNOWS*..3]->(b:Person) - min defaults to 1
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".into(),
            label: "Person".into(),
            properties: Default::default(),
        };
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: None, // Should default to 1
            max_length: Some(3),
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table(
                "KNOWS",
                "Person",
                "Person",
                "src_person_id",
                "dst_person_id",
            )
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should build successfully with default min
        assert!(s.contains("Join("));
    }

    #[test]
    fn test_varlength_expand_default_max() {
        // MATCH (a:Person)-[:KNOWS*2..]->(b:Person) - max defaults to 20
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".into(),
            label: "Person".into(),
            properties: Default::default(),
        };
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(2),
            max_length: None, // Should default to MAX_VARIABLE_LENGTH_HOPS (20)
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table(
                "KNOWS",
                "Person",
                "Person",
                "src_person_id",
                "dst_person_id",
            )
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should build successfully with default max
        assert!(s.contains("Union") || s.contains("union"));
        assert!(s.contains("Join("));
    }

    #[test]
    fn test_varlength_expand_invalid_range() {
        // MATCH (a:Person)-[:KNOWS*3..2]->(b:Person) - min > max should error
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".into(),
            label: "Person".into(),
            properties: Default::default(),
        };
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(3),
            max_length: Some(2), // Invalid: min > max
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table(
                "KNOWS",
                "Person",
                "Person",
                "src_person_id",
                "dst_person_id",
            )
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let result = planner.plan(&project);

        // Should return error
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Invalid variable-length range"));
    }

    #[test]
    fn test_varlength_expand_exceeds_max() {
        // MATCH (a:Person)-[:KNOWS*1..25]->(b:Person) - exceeds MAX (20)
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".into(),
            label: "Person".into(),
            properties: Default::default(),
        };
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(1),
            max_length: Some(25), // Exceeds MAX_VARIABLE_LENGTH_HOPS
            target_properties: HashMap::new(),
        };
        let project = LogicalOperator::Project {
            input: Box::new(vlexpand),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table(
                "KNOWS",
                "Person",
                "Person",
                "src_person_id",
                "dst_person_id",
            )
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let result = planner.plan(&project);

        // Should return error
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Variable-length paths with max length > 20"));
    }

    #[test]
    fn test_varlength_expand_with_filter() {
        // MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) WHERE b.age > 30 RETURN b.name
        let scan_a = LogicalOperator::ScanByLabel {
            variable: "a".into(),
            label: "Person".into(),
            properties: Default::default(),
        };
        let vlexpand = LogicalOperator::VariableLengthExpand {
            input: Box::new(scan_a),
            source_variable: "a".into(),
            target_variable: "b".into(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".into()],
            direction: crate::ast::RelationshipDirection::Outgoing,
            relationship_variable: None,
            min_length: Some(1),
            max_length: Some(2),
            target_properties: HashMap::new(),
        };
        let filter = LogicalOperator::Filter {
            input: Box::new(vlexpand),
            predicate: BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "age".into(),
                }),
                operator: ComparisonOperator::GreaterThan,
                right: ValueExpression::Literal(PropertyValue::Integer(30)),
            },
        };
        let project = LogicalOperator::Project {
            input: Box::new(filter),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "b".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };
        let cfg = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table(
                "KNOWS",
                "Person",
                "Person",
                "src_person_id",
                "dst_person_id",
            )
            .build()
            .unwrap();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should have filter and joins
        assert!(s.contains("Filter") || s.contains("filter"));
        assert!(s.contains("Join("));
    }
}
