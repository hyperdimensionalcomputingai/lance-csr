// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Catalog and namespace utilities for Lance Graph.
//!
//! **Experimental.** The Rust API is not stable yet: breaking changes are possible over the
//! next few weeks while the CSR work lands. Pin an exact version if you depend on it.

pub mod namespace;
pub mod source_catalog;

// Existing exports
pub use namespace::DirNamespace;
pub use source_catalog::{GraphSourceCatalog, InMemoryCatalog, SimpleTableSource};
