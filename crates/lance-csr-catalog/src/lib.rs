// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The table lookups behind [lance-csr](https://docs.rs/lance-csr), a Cypher graph query
//! engine for Lance datasets built on Apache DataFusion.
//!
//! lance-csr reads a graph from ordinary tables: one per node label and one per relationship
//! type. This crate answers the two questions that raises:
//!
//! - [`GraphSourceCatalog`]: which DataFusion `TableSource` holds a node label or relationship
//!   type, so the planner can build scans. Lookups ignore case. [`InMemoryCatalog`] is a
//!   map-backed implementation, and [`SimpleTableSource`] is a table source with a fixed schema
//!   and no data, for planning and tests.
//! - [`DirNamespace`]: where a named Lance dataset lives, as Lance's `LanceNamespace` for a
//!   directory of datasets. The table `person` under `s3://bucket/graph` is
//!   `s3://bucket/graph/person.lance`.
//!
//! lance-csr re-exports all four types, so most programs only depend on `lance-csr`. Depend on
//! this crate directly to implement your own catalog without pulling in the query engine.
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow_schema::{DataType, Field, Schema};
//! use lance_csr_catalog::{GraphSourceCatalog, InMemoryCatalog, SimpleTableSource};
//!
//! let person = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
//! let catalog = InMemoryCatalog::new()
//!     .with_node_source("Person", Arc::new(SimpleTableSource::new(person)));
//! assert!(catalog.node_source("person").is_some());
//! ```

pub mod namespace;
pub mod source_catalog;

// Existing exports
pub use namespace::DirNamespace;
pub use source_catalog::{GraphSourceCatalog, InMemoryCatalog, SimpleTableSource};
