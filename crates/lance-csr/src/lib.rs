// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Graph Query Engine
//!
//! This crate provides graph query capabilities for Lance datasets using Cypher syntax.
//! It interprets Lance datasets as property graphs and translates Cypher queries into
//! DataFusion SQL queries for execution.
//!
//! **Experimental.** The Rust API is not stable yet: breaking changes are possible over the
//! next few weeks while the CSR work lands. Pin an exact version if you depend on it.
//!
//! # Features
//!
//! - Cypher query parsing and AST representation
//! - Graph pattern matching on columnar data
//! - Property graph interpretation of Lance datasets
//! - Translation to optimized SQL via DataFusion
//! - Support for nodes, relationships, and properties
//! - A CSR (compressed sparse row) adjacency index for multi-hop traversal
//!
//! # Example
//!
//! [`CypherEngine`] registers datasets once, optionally builds the CSR, and serves
//! many queries. See [`engine`] for the full API and `examples/embedded_query.rs`
//! for a runnable program.
//!
//! ```no_run
//! use std::collections::HashMap;
//! use lance_csr::{CypherEngine, EngineSource, GraphConfig, Result};
//!
//! # async fn example() -> Result<()> {
//! let config = GraphConfig::builder()
//!     .node_table("Person", "person_id")
//!     .rel_table("KNOWS", "Person", "Person", "src_person_id", "dst_person_id")
//!     .build()?;
//! let sources = HashMap::from([
//!     ("Person".to_string(), EngineSource::open_lance("person.lance", None, HashMap::new()).await?),
//!     ("KNOWS".to_string(), EngineSource::open_lance("knows.lance", None, HashMap::new()).await?),
//! ]);
//! let engine = CypherEngine::new(config, sources, true).await?;
//! let batch = engine
//!     .execute("MATCH (p:Person) WHERE p.age > 30 RETURN p.name", HashMap::new())
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod apply;
pub mod ast;
pub mod case_insensitive;
pub mod config;
pub mod csr_expand;
pub mod csr_index;
mod csr_traversal;
pub mod datafusion_planner;
pub mod engine;
pub mod error;
pub mod key_statistics;
pub mod lance_vector_search;
pub mod logical_plan;
pub mod parameter_substitution;
pub mod parser;
mod path_inventory;
mod path_match;
mod path_value;
mod pattern_alternatives;
mod pattern_order;
pub mod query;
pub mod semantic;
pub mod shortest_path;
pub mod spark_dialect;
pub mod sql_query;

/// Maximum allowed hops for variable-length relationship expansion (e.g., *1..N)
pub const MAX_VARIABLE_LENGTH_HOPS: u32 = 20;

pub use config::{GraphConfig, NodeMapping, RelationshipMapping};
pub use csr_index::{CsrIndex, CsrIndexBuilder};
pub use engine::{CypherEngine, EngineSource};
pub use error::{GraphError, Result};
pub use lance_csr_catalog::{DirNamespace, GraphSourceCatalog, InMemoryCatalog, SimpleTableSource};
pub use lance_vector_search::VectorSearch;
pub use query::{CypherQuery, ExecutionStrategy, SqlDialect};
pub use sql_query::SqlQuery;
