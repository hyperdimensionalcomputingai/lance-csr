// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Arrow values for paths and their entities. Properties live in a marked child so
//! user property names cannot collide with identity or endpoint metadata.

use std::collections::HashMap;
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

/// A direct length/nodes query can keep its existing CSR plan. Values carried through
/// containers and node-property comprehensions need the actual path records.
pub(crate) fn needs_values(query: &crate::ast::CypherQuery) -> bool {
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
                    if let Some(name) = path.shortest.as_ref().and_then(|s| s.variable.as_ref()) {
                        names.insert(name.to_lowercase());
                    }
                }
            }
        }
    }
    let mut needed = false;
    let mut inspect = |expression: &V| {
        let mut value = expression.clone();
        crate::ast::transform_value(&mut value, &mut |v| match v {
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
            inspect(&item.expression);
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
        let prefix = "MATCH p = shortestPath((a:Person)-[:R*]-(b:Person)) ";
        for (suffix, rich) in [
            ("RETURN length(p), nodes(p)", false),
            ("RETURN [n IN nodes(p) | n.id]", false),
            ("RETURN p", true),
            ("RETURN relationships(p)", true),
            ("RETURN length(head(collect(p)))", true),
            ("WITH collect(p) AS ps UNWIND ps AS p RETURN nodes(p)", true),
        ] {
            let query = crate::parser::parse_cypher_query(&format!("{prefix}{suffix}")).unwrap();
            assert_eq!(needs_values(&query), rich, "{suffix}");
        }
    }
}
