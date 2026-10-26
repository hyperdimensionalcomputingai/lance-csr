// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Arrow values for paths and their entities. Properties live in a marked child so
//! user property names cannot collide with identity or endpoint metadata.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListArray, StructArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::compute::take;
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::Result;

pub(crate) const PROPERTIES: &str = "__lance_graph_properties";
pub(crate) const IDENTITY: &str = "__lance_graph_identity";
pub(crate) const START: &str = "__lance_graph_start";
pub(crate) const END: &str = "__lance_graph_end";
const ENTITY: &str = "lance_graph.entity";

pub(crate) fn entity_kind(data_type: &DataType) -> Option<&str> {
    let DataType::Struct(fields) = data_type else {
        return None;
    };
    fields.first()?.metadata().get(ENTITY).map(String::as_str)
}

pub(crate) fn entity_type(
    properties: Fields,
    kind: &str,
    node_type: Option<&DataType>,
) -> DataType {
    let mut fields = vec![
        Field::new(PROPERTIES, DataType::Struct(properties), false)
            .with_metadata(HashMap::from([(ENTITY.into(), kind.into())])),
        Field::new(
            IDENTITY,
            DataType::Struct(
                vec![
                    Field::new("label", DataType::Utf8, false),
                    Field::new("id", DataType::Int64, false),
                ]
                .into(),
            ),
            false,
        ),
    ];
    if let Some(node_type) = node_type {
        fields.push(Field::new(START, node_type.clone(), true));
        fields.push(Field::new(END, node_type.clone(), true));
    }
    DataType::Struct(fields.into())
}

pub(crate) fn path_type(node_type: DataType, relationship_type: DataType) -> DataType {
    let list = |t| DataType::List(Arc::new(Field::new("item", t, true)));
    DataType::Struct(
        vec![
            Field::new("length", DataType::Int64, false),
            Field::new("nodes", list(DataType::Int64), false),
            Field::new("node_records", list(node_type), false),
            Field::new("relationships", list(relationship_type), false),
        ]
        .into(),
    )
}

pub(crate) fn struct_array(data_type: &DataType, columns: Vec<ArrayRef>) -> StructArray {
    let DataType::Struct(fields) = data_type else {
        unreachable!("entity/path struct")
    };
    StructArray::new(fields.clone(), columns, None)
}

/// Ordered row references, independent of property equality or edge endpoints.
#[derive(Debug, Clone)]
pub(crate) struct PathRows {
    pub nodes: Vec<u32>,
    pub edges: Vec<u32>,
    /// Node positions at the written segment boundaries (variable segments may have many hops).
    pub boundaries: Vec<usize>,
}

pub(crate) fn paths_array(
    paths: &[PathRows],
    nodes: &StructArray,
    edges: &StructArray,
) -> Result<ArrayRef> {
    let node_rows =
        UInt32Array::from_iter_values(paths.iter().flat_map(|p| p.nodes.iter().copied()));
    let edge_rows =
        UInt32Array::from_iter_values(paths.iter().flat_map(|p| p.edges.iter().copied()));
    let selected_nodes = take(nodes, &node_rows, None)?;
    let selected_edges = take(edges, &edge_rows, None)?;
    let ids = selected_nodes
        .as_struct()
        .column(1)
        .as_struct()
        .column(1)
        .clone();
    let list = |values: ArrayRef, lengths: Vec<usize>| -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::from_lengths(lengths),
            values,
            None,
        ))
    };
    let node_lengths: Vec<_> = paths.iter().map(|p| p.nodes.len()).collect();
    let edge_lengths: Vec<_> = paths.iter().map(|p| p.edges.len()).collect();
    let data_type = path_type(nodes.data_type().clone(), edges.data_type().clone());
    Ok(Arc::new(struct_array(
        &data_type,
        vec![
            Arc::new(Int64Array::from_iter_values(
                paths.iter().map(|p| p.edges.len() as i64),
            )),
            list(ids, node_lengths.clone()),
            list(selected_nodes, node_lengths),
            list(selected_edges, edge_lengths),
        ],
    )))
}

/// Null tests need only the compact path's validity, not its entity inventory.
fn mask_path_null_checks(predicate: &mut crate::ast::BooleanExpression, names: &HashSet<String>) {
    use crate::ast::{BooleanExpression as B, PropertyValue, ValueExpression as V};
    match predicate {
        B::IsNull(value) | B::IsNotNull(value) => {
            if matches!(&*value, V::Variable(name) if names.contains(&name.to_lowercase())) {
                *value = V::Literal(PropertyValue::Null);
            }
        }
        B::And(left, right) | B::Or(left, right) => {
            mask_path_null_checks(left, names);
            mask_path_null_checks(right, names);
        }
        B::Not(inner) => mask_path_null_checks(inner, names),
        _ => {}
    }
}

/// Homogeneous length/nodes queries, including WITH aliases, keep their CSR plan.
/// Returned path values, relationship access and containers need actual path records.
pub(crate) fn needs_values(
    query: &crate::ast::CypherQuery,
    config: &crate::config::GraphConfig,
) -> bool {
    use crate::ast::{GraphPattern, ReadingClause, ValueExpression as V};
    let clauses: Vec<_> = query
        .reading_clauses
        .iter()
        .chain(&query.post_with_reading_clauses)
        .chain(
            query
                .further_with_stages
                .iter()
                .flat_map(|s| &s.reading_clauses),
        )
        .collect();
    let mut names = std::collections::HashSet::new();
    for clause in &clauses {
        if let ReadingClause::Match(m) = clause {
            for pattern in &m.patterns {
                if let GraphPattern::Path(path) = pattern {
                    if path.variable.is_some() {
                        return true;
                    }
                    if path.shortest.is_some() {
                        // Numeric keys alone cannot distinguish nodes in different labels,
                        // even when both endpoints have the same label on an undirected path.
                        let crosses_labels = path.segments.iter().any(|segment| {
                            segment.relationship.types.iter().any(|kind| {
                                config
                                    .get_relationship_mapping(kind)
                                    .is_some_and(|mapping| {
                                        mapping
                                            .source_label
                                            .as_deref()
                                            .zip(mapping.target_label.as_deref())
                                            .is_some_and(|(source, target)| {
                                                !source.eq_ignore_ascii_case(target)
                                            })
                                    })
                            })
                        });
                        let different_ends = path
                            .start_node
                            .labels
                            .first()
                            .zip(path.segments.last().and_then(|s| s.end_node.labels.first()))
                            .is_some_and(|(source, target)| !source.eq_ignore_ascii_case(target));
                        if crosses_labels || different_ends {
                            return true;
                        }
                        if let Some(name) = path.shortest.as_ref().and_then(|s| s.variable.as_ref())
                        {
                            names.insert(name.to_lowercase());
                        }
                    }
                }
            }
        }
    }
    // A bare WITH only renames/carries a path; inspect its eventual consumers instead.
    for with in query
        .with_clause
        .iter()
        .chain(query.further_with_stages.iter().map(|s| &s.with_clause))
    {
        for item in &with.items {
            if let (V::Variable(name), Some(alias)) = (&item.expression, &item.alias) {
                if names.contains(&name.to_lowercase()) {
                    names.insert(alias.to_lowercase());
                }
            }
        }
    }
    let mut needed = false;
    let mut inspect = |expression: &V| {
        let mut value = expression.clone();
        crate::ast::transform_value(&mut value, &mut |v| match v {
            V::Boolean(predicate) => mask_path_null_checks(predicate, &names),
            V::Case { whens, .. } => {
                for (predicate, _) in whens {
                    mask_path_null_checks(predicate, &names);
                }
            }
            V::ListComprehension {
                filter: Some(predicate),
                ..
            }
            | V::PatternComprehension {
                filter: Some(predicate),
                ..
            } => {
                mask_path_null_checks(predicate, &names);
            }
            V::ScalarFunction { name, args }
                if matches!(name.to_lowercase().as_str(), "length" | "nodes") =>
            {
                args.clear()
            }
            V::AggregateFunction { args, .. }
                if args.iter().any(
                    |v| matches!(v, V::Variable(name) if names.contains(&name.to_lowercase())),
                ) =>
            {
                needed = true;
            }
            V::ScalarFunction { name, .. }
                if matches!(
                    name.to_lowercase().as_str(),
                    "relationships" | "startnode" | "endnode"
                ) =>
            {
                needed = true
            }
            _ => {}
        });
        crate::ast::transform_value(&mut value, &mut |v| {
            if matches!(v, V::Variable(name) if names.contains(&name.to_lowercase())) {
                needed = true;
            }
        });
    };
    for clause in clauses {
        match clause {
            ReadingClause::Unwind(u) => inspect(&u.expression),
            ReadingClause::Match(m) => {
                if let Some(w) = &m.where_clause {
                    inspect(&V::Boolean(Box::new(w.expression.clone())));
                }
            }
        }
    }
    for with in query
        .with_clause
        .iter()
        .chain(query.further_with_stages.iter().map(|s| &s.with_clause))
    {
        for item in &with.items {
            if !matches!(&item.expression, V::Variable(name) if names.contains(&name.to_lowercase()))
            {
                inspect(&item.expression);
            }
        }
        if let Some(order) = &with.order_by {
            for item in &order.items {
                inspect(&item.expression);
            }
        }
        if let Some(filter) = &with.where_clause {
            inspect(&V::Boolean(Box::new(filter.expression.clone())));
        }
    }
    for item in &query.return_clause.items {
        inspect(&item.expression);
    }
    if let Some(order) = &query.order_by {
        for item in &order.items {
            inspect(&item.expression);
        }
    }
    for filter in query
        .where_clause
        .iter()
        .chain(&query.post_with_where_clause)
        .chain(
            query
                .further_with_stages
                .iter()
                .filter_map(|s| s.where_clause.as_ref()),
        )
    {
        inspect(&V::Boolean(Box::new(filter.expression.clone())));
    }
    needed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_shortest_queries_keep_the_csr_route() {
        let config = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("R", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let prefix = "MATCH p = shortestPath((a:Person)-[:R*]-(b:Person)) ";
        for (suffix, rich) in [
            ("RETURN length(p), nodes(p)", false),
            ("RETURN p IS NULL, p IS NOT NULL", false),
            (
                "RETURN CASE p IS NULL WHEN true THEN -1 ELSE length(p) END",
                false,
            ),
            (
                "RETURN CASE WHEN p IS NOT NULL THEN length(p) ELSE -1 END",
                false,
            ),
            (
                "WITH p AS q WHERE q IS NOT NULL OR NOT q IS NULL RETURN length(q)",
                false,
            ),
            ("RETURN p, p IS NULL", true),
            ("RETURN relationships(p) IS NULL", true),
            ("RETURN [n IN nodes(p) | n.id]", false),
            ("WITH p RETURN nodes(p)", false),
            (
                "WITH p AS q WITH q AS r RETURN [n IN nodes(r) | n.id]",
                false,
            ),
            ("WITH p AS q RETURN q", true),
            ("WITH p AS q RETURN relationships(q)", true),
            (
                "WITH p AS q WITH collect(q) AS ps UNWIND ps AS r RETURN nodes(r)",
                true,
            ),
            ("RETURN p", true),
            ("RETURN relationships(p)", true),
            ("RETURN length(head(collect(p)))", true),
            ("WITH collect(p) AS ps UNWIND ps AS p RETURN nodes(p)", true),
        ] {
            let query = crate::parser::parse_cypher_query(&format!("{prefix}{suffix}")).unwrap();
            assert_eq!(needs_values(&query, &config), rich, "{suffix}");
        }
    }

    #[test]
    fn cross_label_shortest_paths_need_typed_identity() {
        let config = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .node_table("Place", "id")
            .rel_table("LIVES", "Person", "Place", "src", "dst")
            .build()
            .unwrap();
        for ends in ["(b:Place)", "(b:Person)", "(b)"] {
            let query = crate::parser::parse_cypher_query(&format!(
                "MATCH p = shortestPath((a:Person)-[:LIVES*0..]-{ends}) RETURN nodes(p)"
            ))
            .unwrap();
            assert!(needs_values(&query, &config));
        }
    }
}
