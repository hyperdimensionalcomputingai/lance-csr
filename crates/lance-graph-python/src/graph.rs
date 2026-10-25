// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Graph query functionality for Lance datasets

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use arrow::compute::concat_batches;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use futures::StreamExt;
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::Dataset;
use lance_graph::csr_expand::{
    edge_changes, is_supported_id_type, Adjacency, AdjacencyCatalog, CsrQueryPlanner, NodeRows,
    PruneCsrTargetColumns, PushCsrTargetFilters,
};
use lance_graph::key_statistics::KeyStatisticsProvider;
use lance_graph::{
    ast::{DistanceMetric as RustDistanceMetric, GraphPattern, ReadingClause},
    CypherQuery as RustCypherQuery, ExecutionStrategy as RustExecutionStrategy,
    GraphConfig as RustGraphConfig, GraphError as RustGraphError, InMemoryCatalog,
    SqlDialect as RustSqlDialect, SqlQuery as RustSqlQuery, VectorSearch as RustVectorSearch,
};
use pyo3::{
    exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyDict, PyInt, PyList},
    IntoPyObject,
};
use serde_json::Value as JsonValue;

use crate::executor::{QueryDeadline, QUERY_TIMEOUT_MESSAGE};
use crate::namespace::PyDirNamespace;
use crate::RT;

/// Execution strategy for Cypher queries
#[pyclass(name = "ExecutionStrategy", module = "lance_csr.graph", from_py_object)]
#[derive(Clone, Copy)]
pub enum ExecutionStrategy {
    /// Use DataFusion query planner (default, full feature support)
    DataFusion,
    /// Use Lance native executor (not yet implemented)
    LanceNative,
}

impl From<ExecutionStrategy> for RustExecutionStrategy {
    fn from(strategy: ExecutionStrategy) -> Self {
        match strategy {
            ExecutionStrategy::DataFusion => RustExecutionStrategy::DataFusion,
            ExecutionStrategy::LanceNative => RustExecutionStrategy::LanceNative,
        }
    }
}

/// SQL dialect for generating SQL from Cypher queries
#[pyclass(name = "SqlDialect", module = "lance_csr.graph", from_py_object)]
#[derive(Clone, Copy)]
pub enum SqlDialect {
    /// Generic SQL (DataFusion default dialect)
    Default,
    /// Spark SQL dialect
    Spark,
    /// PostgreSQL dialect
    PostgreSql,
    /// MySQL dialect
    MySql,
    /// SQLite dialect
    Sqlite,
}

impl From<SqlDialect> for RustSqlDialect {
    fn from(dialect: SqlDialect) -> Self {
        match dialect {
            SqlDialect::Default => RustSqlDialect::Default,
            SqlDialect::Spark => RustSqlDialect::Spark,
            SqlDialect::PostgreSql => RustSqlDialect::PostgreSql,
            SqlDialect::MySql => RustSqlDialect::MySql,
            SqlDialect::Sqlite => RustSqlDialect::Sqlite,
        }
    }
}

/// Distance metric for vector similarity search
#[pyclass(name = "DistanceMetric", module = "lance_csr.graph", from_py_object)]
#[derive(Clone, Copy)]
pub enum DistanceMetric {
    /// L2 (Euclidean) distance - smaller is more similar
    L2,
    /// Cosine distance - smaller is more similar (1 - cosine_similarity)
    Cosine,
    /// Dot product distance - for normalized vectors, larger is more similar
    Dot,
}

impl From<DistanceMetric> for RustDistanceMetric {
    fn from(metric: DistanceMetric) -> Self {
        match metric {
            DistanceMetric::L2 => RustDistanceMetric::L2,
            DistanceMetric::Cosine => RustDistanceMetric::Cosine,
            DistanceMetric::Dot => RustDistanceMetric::Dot,
        }
    }
}

/// Vector similarity search builder
///
/// This class provides an explicit API for vector similarity search that can work with:
/// - In-memory PyArrow tables (brute-force search)
/// - Lance datasets (ANN search with indices)
///
/// This is distinct from the UDF-based vector search (`vector_distance()`, `vector_similarity()`)
/// which is integrated into Cypher queries.
///
/// Examples
/// --------
/// >>> from lance_csr import VectorSearch, DistanceMetric
/// >>>
/// >>> # Basic vector search on a PyArrow table
/// >>> results = VectorSearch("embedding") \
/// ...     .query_vector([0.1, 0.2, 0.3]) \
/// ...     .metric(DistanceMetric.Cosine) \
/// ...     .top_k(10) \
/// ...     .search(table)
#[pyclass(name = "VectorSearch", module = "lance_csr.graph", from_py_object)]
#[derive(Clone)]
pub struct VectorSearch {
    inner: RustVectorSearch,
    /// Flag to enable vector-first Lance ANN execution path.
    /// This is stored separately because RustVectorSearch doesn't have this concept.
    use_lance_index: bool,
}

#[pymethods]
impl VectorSearch {
    /// Create a new VectorSearch builder
    ///
    /// Parameters
    /// ----------
    /// column : str
    ///     Name of the vector column to search
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new VectorSearch builder
    #[new]
    fn new(column: &str) -> Self {
        Self {
            inner: RustVectorSearch::new(column),
            use_lance_index: false,
        }
    }

    /// Set the query vector
    ///
    /// Parameters
    /// ----------
    /// vector : list[float]
    ///     The query vector for similarity computation
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new builder with the query vector set
    fn query_vector(&self, vector: Vec<f32>) -> Self {
        Self {
            inner: self.inner.clone().query_vector(vector),
            use_lance_index: self.use_lance_index,
        }
    }

    /// Set the distance metric
    ///
    /// Parameters
    /// ----------
    /// metric : DistanceMetric
    ///     The distance metric to use (L2, Cosine, or Dot)
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new builder with the metric set
    fn metric(&self, metric: DistanceMetric) -> Self {
        Self {
            inner: self.inner.clone().metric(metric.into()),
            use_lance_index: self.use_lance_index,
        }
    }

    /// Set the number of results to return
    ///
    /// Parameters
    /// ----------
    /// k : int
    ///     Number of top results to return
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new builder with top_k set
    fn top_k(&self, k: usize) -> Self {
        Self {
            inner: self.inner.clone().top_k(k),
            use_lance_index: self.use_lance_index,
        }
    }

    /// Whether to include the distance column in results
    ///
    /// Parameters
    /// ----------
    /// include : bool
    ///     If True, adds a distance column to results (default: True)
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new builder with the setting applied
    fn include_distance(&self, include: bool) -> Self {
        Self {
            inner: self.inner.clone().include_distance(include),
            use_lance_index: self.use_lance_index,
        }
    }

    /// Set the name of the distance column
    ///
    /// Parameters
    /// ----------
    /// name : str
    ///     Name for the distance column (default: "_distance")
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new builder with the column name set
    fn distance_column_name(&self, name: &str) -> Self {
        Self {
            inner: self.inner.clone().distance_column_name(name),
            use_lance_index: self.use_lance_index,
        }
    }

    /// Use Lance ANN index when datasets are Lance datasets.
    ///
    /// This enables a vector-first execution path that queries the Lance index
    /// and then runs the Cypher query on the top-k results. This can be much faster
    /// for large datasets but may change semantics when the Cypher query includes
    /// filters or additional constraints.
    ///
    /// Parameters
    /// ----------
    /// enabled : bool
    ///     If True, use Lance ANN index for vector search when possible.
    ///
    /// Returns
    /// -------
    /// VectorSearch
    ///     A new builder with the setting applied
    fn use_lance_index(&self, enabled: bool) -> Self {
        Self {
            inner: self.inner.clone(),
            use_lance_index: enabled,
        }
    }

    /// Execute brute-force vector search on a PyArrow table
    ///
    /// Parameters
    /// ----------
    /// table : pyarrow.Table
    ///     The table containing vectors to search
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Results sorted by similarity with top_k rows
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If query vector is not set or column not found
    /// RuntimeError
    ///     If search execution fails
    fn search(&self, py: Python, table: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let batch = python_any_to_record_batch(table)?;
        let batch = normalize_record_batch(batch)?;

        let inner = self.inner.clone();
        let result = RT
            .block_on(Some(py), inner.search(&batch))?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result)
    }

    fn __repr__(&self) -> String {
        "VectorSearch(...)".to_string()
    }
}

/// Convert GraphError to PyErr
pub(crate) fn graph_error_to_pyerr(err: RustGraphError) -> PyErr {
    match &err {
        RustGraphError::ParseError { .. }
        | RustGraphError::ConfigError { .. }
        | RustGraphError::PlanError { .. }
        | RustGraphError::InvalidPattern { .. } => PyValueError::new_err(err.to_string()),
        RustGraphError::UnsupportedFeature { .. } => {
            PyNotImplementedError::new_err(err.to_string())
        }
        _ => PyRuntimeError::new_err(err.to_string()),
    }
}

/// Graph configuration for interpreting Lance datasets as property graphs
#[pyclass(name = "GraphConfig", module = "lance_csr.graph", from_py_object)]
#[derive(Clone)]
pub struct GraphConfig {
    inner: RustGraphConfig,
}

#[pymethods]
impl GraphConfig {
    /// Create a new GraphConfig builder
    #[staticmethod]
    fn builder() -> GraphConfigBuilder {
        GraphConfigBuilder::new()
    }

    /// Get node labels
    fn node_labels(&self) -> Vec<String> {
        // Extract from the config's node mappings
        self.inner
            .iter_node_mappings()
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get relationship types
    fn relationship_types(&self) -> Vec<String> {
        // Extract from the config's relationship mappings
        self.inner
            .iter_relationship_mappings()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "GraphConfig(nodes={:?}, relationships={:?})",
            self.node_labels(),
            self.relationship_types()
        )
    }
}

/// Builder for GraphConfig
#[pyclass(
    name = "GraphConfigBuilder",
    module = "lance_csr.graph",
    from_py_object
)]
#[derive(Clone)]
pub struct GraphConfigBuilder {
    inner: lance_graph::config::GraphConfigBuilder,
}

/// Raise a ``DeprecationWarning`` pointing at the caller's line.
fn deprecated(py: Python, message: &std::ffi::CStr) -> PyResult<()> {
    let category = py.get_type::<pyo3::exceptions::PyDeprecationWarning>();
    PyErr::warn(py, &category, message, 1)
}

#[pymethods]
impl GraphConfigBuilder {
    #[new]
    fn new() -> Self {
        Self {
            inner: RustGraphConfig::builder(),
        }
    }

    /// Add a node table: the nodes with ``label``, one per row of the dataset of the
    /// same name, identified by its ``primary_key`` column.
    ///
    /// Parameters
    /// ----------
    /// label : str
    ///     The node label, and the name of its dataset (e.g., "Person")
    /// primary_key : str
    ///     The column that identifies each node
    ///
    /// Returns
    /// -------
    /// GraphConfigBuilder
    ///     A new builder with the node table added
    fn node_table(&self, label: &str, primary_key: &str) -> Self {
        Self {
            inner: self.inner.clone().node_table(label, primary_key),
        }
    }

    /// Add a relationship table: edges of type ``rel_type`` from ``from_table`` nodes to
    /// ``to_table`` nodes, one per row of the dataset of the same name.
    ///
    /// Parameters
    /// ----------
    /// rel_type : str
    ///     The relationship type, and the name of its dataset (e.g., "KNOWS")
    /// from_table : str
    ///     The node table at the source end
    /// to_table : str
    ///     The node table at the target end
    /// src : str
    ///     The column holding the source node's key
    /// dst : str
    ///     The column holding the target node's key
    ///
    /// Returns
    /// -------
    /// GraphConfigBuilder
    ///     A new builder with the relationship table added
    fn rel_table(
        &self,
        rel_type: &str,
        from_table: &str,
        to_table: &str,
        src: &str,
        dst: &str,
    ) -> Self {
        Self {
            inner: self
                .inner
                .clone()
                .rel_table(rel_type, from_table, to_table, src, dst),
        }
    }

    /// Deprecated: use ``node_table``.
    fn with_node_label(&self, py: Python, label: &str, id_field: &str) -> PyResult<Self> {
        deprecated(py, c"with_node_label() is deprecated; use node_table()")?;
        Ok(self.node_table(label, id_field))
    }

    /// Deprecated: use ``rel_table``, which also takes the node tables at both ends.
    #[pyo3(signature = (rel_type, source_field, target_field, source_label=None, target_label=None))]
    fn with_relationship(
        &self,
        py: Python,
        rel_type: &str,
        source_field: &str,
        target_field: &str,
        source_label: Option<&str>,
        target_label: Option<&str>,
    ) -> PyResult<Self> {
        deprecated(
            py,
            c"with_relationship() is deprecated; use rel_table(), which also takes the node tables at both ends",
        )?;
        #[allow(deprecated)]
        let inner = self
            .inner
            .clone()
            .with_relationship(rel_type, source_field, target_field);
        #[allow(deprecated)]
        let inner = match (source_label, target_label) {
            (Some(src), Some(dst)) => inner.with_relationship_endpoints(rel_type, src, dst),
            (None, None) => inner,
            _ => {
                return Err(PyValueError::new_err(
                    "source_label and target_label must be given together",
                ))
            }
        };
        Ok(Self { inner })
    }

    /// Build the GraphConfig
    ///
    /// Returns
    /// -------
    /// GraphConfig
    ///     The configured graph config
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the configuration is invalid
    fn build(&self) -> PyResult<GraphConfig> {
        let config = self.inner.clone().build().map_err(graph_error_to_pyerr)?;
        Ok(GraphConfig { inner: config })
    }
}

/// Cypher query interface for Lance datasets
#[pyclass(name = "CypherQuery", module = "lance_csr.graph", from_py_object)]
#[derive(Clone)]
pub struct CypherQuery {
    inner: RustCypherQuery,
}

#[pymethods]
impl CypherQuery {
    /// Create a new Cypher query
    ///
    /// Parameters
    /// ----------
    /// query_text : str
    ///     The Cypher query string
    ///
    /// Returns
    /// -------
    /// CypherQuery
    ///     The parsed query
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the query cannot be parsed
    #[new]
    fn new(query_text: &str) -> PyResult<Self> {
        let query = RustCypherQuery::new(query_text).map_err(graph_error_to_pyerr)?;
        Ok(Self { inner: query })
    }

    /// Set the graph configuration
    ///
    /// Parameters
    /// ----------
    /// config : GraphConfig
    ///     The graph configuration
    ///
    /// Returns
    /// -------
    /// CypherQuery
    ///     A new query instance with the config set
    fn with_config(&self, config: &GraphConfig) -> Self {
        Self {
            inner: self.inner.clone().with_config(config.inner.clone()),
        }
    }

    /// Add a query parameter
    ///
    /// Parameters
    /// ----------
    /// key : str
    ///     Parameter name
    /// value : object
    ///     Parameter value (will be converted to JSON)
    ///
    /// Returns
    /// -------
    /// CypherQuery
    ///     A new query instance with the parameter added
    fn with_parameter(&self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Convert Python value to JSON
        let json_value = python_to_json(value)?;
        Ok(Self {
            inner: self.inner.clone().with_parameter(key, json_value),
        })
    }

    /// Get the query text
    fn query_text(&self) -> &str {
        self.inner.query_text()
    }

    /// Get query parameters
    fn parameters(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in self.inner.parameters() {
            let py_value = json_to_python(py, value)?;
            dict.set_item(key, py_value)?;
        }
        Ok(dict.unbind())
    }

    /// Export supported Cypher expressions as SQL
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to Lance datasets
    /// dialect : SqlDialect, optional
    ///     SQL dialect to use. Defaults to SqlDialect.Default (generic DataFusion SQL).
    ///
    /// Returns
    /// -------
    /// str
    ///     The generated SQL query
    ///
    /// Raises
    /// ------
    /// NotImplementedError
    ///     If a runtime-only Cypher function has no faithful SQL translation, or a
    ///     typed-null literal is exported outside the Default dialect
    /// RuntimeError
    ///     If SQL generation fails
    #[pyo3(signature = (datasets, dialect=None))]
    fn to_sql(
        &self,
        py: Python,
        datasets: &Bound<'_, PyDict>,
        dialect: Option<SqlDialect>,
    ) -> PyResult<String> {
        let sql_dialect = dialect.map(|d| d.into());

        // Convert datasets to Arrow RecordBatch map
        let arrow_datasets = python_datasets_to_batches(datasets)?;

        // Clone for async move
        let inner_query = self.inner.clone();

        // Execute via runtime
        let sql = RT
            .block_on(Some(py), inner_query.to_sql(arrow_datasets, sql_dialect))?
            .map_err(graph_error_to_pyerr)?;

        Ok(sql)
    }

    /// Execute query against Lance datasets
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to Lance datasets
    /// strategy : ExecutionStrategy, optional
    ///     Execution strategy to use (defaults to DataFusion)
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Query results as Arrow table
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If query execution fails
    ///
    /// Examples
    /// --------
    /// >>> # Default strategy (DataFusion)
    /// >>> result = query.execute(datasets)
    ///
    /// >>> # Explicit strategy
    /// >>> from lance.graph import ExecutionStrategy
    /// >>> result = query.execute(datasets, strategy=ExecutionStrategy.DataFusion)
    #[pyo3(signature = (datasets, strategy=None, *, timeout=None))]
    fn execute(
        &self,
        py: Python,
        datasets: &Bound<'_, PyDict>,
        strategy: Option<ExecutionStrategy>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        let deadline = QueryDeadline::new(timeout)?;
        deadline.check()?;
        // Convert datasets to Arrow batches while holding the GIL
        let arrow_datasets = python_datasets_to_batches(datasets)?;

        // Convert Python strategy to Rust strategy
        let rust_strategy = strategy.map(|s| s.into());

        // Clone the inner query for use in the async block
        let inner_query = self.inner.clone();

        // Use RT.block_on with Some(py) like the scanner to_pyarrow method
        let result_batch = RT
            .block_on_until(
                Some(py),
                deadline,
                inner_query.execute(arrow_datasets, rust_strategy),
            )?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result_batch)
    }

    /// Execute query using a namespace resolver.
    ///
    /// Parameters
    /// ----------
    /// namespace : DirNamespace
    ///     Directory-backed namespace that resolves table names to Lance datasets.
    /// strategy : ExecutionStrategy, optional
    ///     Execution strategy to use (defaults to DataFusion)
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Query results as Arrow table
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If query execution fails
    #[pyo3(signature = (namespace, strategy=None))]
    fn execute_with_namespace(
        &self,
        py: Python,
        namespace: &Bound<'_, PyDirNamespace>,
        strategy: Option<ExecutionStrategy>,
    ) -> PyResult<Py<PyAny>> {
        let rust_strategy = strategy.map(|s| s.into());
        let inner_query = self.inner.clone();
        let namespace_arc = namespace.borrow().inner.clone();

        let result_batch = RT
            .block_on(
                Some(py),
                inner_query.execute_with_namespace_arc(namespace_arc, rust_strategy),
            )?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result_batch)
    }

    /// Explain query using the DataFusion planner with in-memory datasets
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to in-memory tables (pyarrow.Table, LanceDataset, etc.)
    ///     Keys should match node labels and relationship types in the graph config.
    ///
    /// Returns
    /// -------
    /// str
    ///     Query graph logical plan, DataFusion logical plan, DataFusion physical plan as string
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If the query is invalid or datasets are missing
    /// RuntimeError
    ///     If query explain fails
    fn explain(&self, py: Python, datasets: &Bound<'_, PyDict>) -> PyResult<String> {
        // Convert datasets to Arrow RecordBatch map
        let arrow_datasets = python_datasets_to_batches(datasets)?;

        // Clone for async move
        let inner_query = self.inner.clone();

        // Execute via runtime
        let plan = RT
            .block_on(Some(py), inner_query.explain(arrow_datasets))?
            .map_err(graph_error_to_pyerr)?;

        Ok(plan)
    }

    /// Get variables used in the query
    fn variables(&self) -> Vec<String> {
        self.inner.variables()
    }

    /// Get node labels referenced in the query
    fn node_labels(&self) -> Vec<String> {
        self.inner.ast().get_node_labels()
    }

    /// Get relationship types referenced in the query
    fn relationship_types(&self) -> Vec<String> {
        self.inner.ast().get_relationship_types()
    }

    /// Execute Cypher query, then apply vector search reranking on results
    ///
    /// This is a convenience method for the common GraphRAG pattern:
    /// 1. Run Cypher query to get candidate entities via graph traversal
    /// 2. Rerank candidates by vector similarity
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to Lance datasets or PyArrow tables
    /// vector_search : VectorSearch
    ///     VectorSearch configuration for reranking
    ///     (Use VectorSearch.use_lance_index(True) to enable a vector-first
    ///     execution path when datasets are Lance datasets.)
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Results sorted by vector similarity with top_k rows
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If the query is invalid or vector search is misconfigured
    /// RuntimeError
    ///     If query execution fails
    ///
    /// Examples
    /// --------
    /// >>> from lance_csr import CypherQuery, VectorSearch, DistanceMetric, GraphConfig
    /// >>>
    /// >>> config = GraphConfig.builder().node_table("Document", "id").build()
    /// >>> query = CypherQuery("MATCH (d:Document) WHERE d.category = 'tech' RETURN d.id, d.name, d.embedding")
    /// >>> query = query.with_config(config)
    /// >>>
    /// >>> results = query.execute_with_vector_rerank(
    /// ...     datasets,
    /// ...     VectorSearch("d.embedding")
    /// ...         .query_vector([0.1, 0.2, 0.3])
    /// ...         .metric(DistanceMetric.Cosine)
    /// ...         .top_k(10)
    /// ... )
    fn execute_with_vector_rerank(
        &self,
        py: Python,
        datasets: &Bound<'_, PyDict>,
        vector_search: &VectorSearch,
    ) -> PyResult<Py<PyAny>> {
        if vector_search.use_lance_index {
            if let Some(result) =
                try_execute_with_lance_index(py, &self.inner, datasets, vector_search)?
            {
                return record_batch_to_python_table(py, &result);
            }
        }

        // Convert datasets to Arrow batches
        let arrow_datasets = python_datasets_to_batches(datasets)?;

        // Clone for async move
        let inner_query = self.inner.clone();
        let vs = vector_search.inner.clone();

        // Execute via runtime
        let result = RT
            .block_on(
                Some(py),
                inner_query.execute_with_vector_rerank(arrow_datasets, vs),
            )?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result)
    }

    fn __repr__(&self) -> String {
        format!("CypherQuery(\"{}\")", self.inner.query_text())
    }
}

// Helper functions to convert between Python and JSON values
fn python_to_json(value: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    python_to_json_value(value, 0)
}

/// Match serde_json's nesting bound and reject cycles before exhausting the Rust stack.
fn python_to_json_value(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<JsonValue> {
    if depth >= 128 {
        return Err(PyValueError::new_err(
            "Parameter nesting exceeds 128 levels (possibly a cyclic container)",
        ));
    }
    if value.is_none() {
        Ok(JsonValue::Null)
    } else if let Ok(b) = value.extract::<bool>() {
        Ok(JsonValue::Bool(b))
    } else if value.is_instance_of::<PyInt>() {
        let i = value.extract::<i64>().map_err(|_| {
            PyValueError::new_err("Integer parameters must fit signed 64-bit values")
        })?;
        Ok(JsonValue::Number(i.into()))
    } else if let Ok(i) = value.extract::<i64>() {
        Ok(JsonValue::Number(i.into()))
    } else if let Ok(f) = value.extract::<f64>() {
        Ok(JsonValue::Number(
            serde_json::Number::from_f64(f)
                .ok_or_else(|| PyValueError::new_err("Invalid float value"))?,
        ))
    } else if let Ok(s) = value.extract::<String>() {
        Ok(JsonValue::String(s))
    } else if let Ok(values) = value.cast::<PyList>() {
        let values = values
            .iter()
            .map(|v| python_to_json_value(&v, depth + 1))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(JsonValue::Array(values))
    } else if let Ok(values) = value.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in values.iter() {
            let key = key
                .extract::<String>()
                .map_err(|_| PyValueError::new_err("Map parameter keys must be strings"))?;
            map.insert(key, python_to_json_value(&value, depth + 1)?);
        }
        Ok(JsonValue::Object(map))
    } else if value.hasattr("isoformat")? {
        // datetime and date: ISO 8601 text, which DataFusion casts when comparing with
        // timestamp and date columns.
        Ok(JsonValue::String(
            value.call_method0("isoformat")?.extract::<String>()?,
        ))
    } else {
        Err(PyValueError::new_err("Unsupported parameter type"))
    }
}

fn json_to_python(py: Python, value: &JsonValue) -> PyResult<Py<PyAny>> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => {
            use pyo3::types::PyBool;
            Ok(PyBool::new(py, *b).to_owned().into())
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.unbind().into())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.unbind().into())
            } else {
                Ok(n.to_string().into_pyobject(py)?.unbind().into())
            }
        }
        JsonValue::String(s) => Ok(s.as_str().into_pyobject(py)?.unbind().into()),
        JsonValue::Array(values) => {
            let values = values
                .iter()
                .map(|v| json_to_python(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, values)?.into_any().unbind())
        }
        JsonValue::Object(values) => {
            let map = PyDict::new(py);
            for (key, value) in values {
                map.set_item(key, json_to_python(py, value)?)?;
            }
            Ok(map.into_any().unbind())
        }
    }
}

// Helper functions for Arrow conversion

/// Convert a single Python dataset value to a RecordBatch
fn python_dataset_to_batch(value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // Lance datasets land here too: the one-shot `CypherQuery` paths materialize
    // their inputs, unlike `CypherEngine`.
    let batch = if value.hasattr("to_table")? {
        let table = value.call_method0("to_table")?;
        python_any_to_record_batch(&table)?
    } else {
        python_any_to_record_batch(value)?
    };
    normalize_record_batch(batch)
}

fn python_datasets_to_batches(
    datasets: &Bound<'_, PyDict>,
) -> PyResult<HashMap<String, RecordBatch>> {
    python_datasets_to_batches_impl(datasets, None)
}

fn python_datasets_to_batches_with_override(
    datasets: &Bound<'_, PyDict>,
    override_label: &str,
    override_batch: &RecordBatch,
) -> PyResult<HashMap<String, RecordBatch>> {
    python_datasets_to_batches_impl(datasets, Some((override_label, override_batch)))
}

fn python_datasets_to_batches_impl(
    datasets: &Bound<'_, PyDict>,
    override_entry: Option<(&str, &RecordBatch)>,
) -> PyResult<HashMap<String, RecordBatch>> {
    let mut arrow_datasets = HashMap::new();
    for (key, value) in datasets.iter() {
        let table_name: String = key.extract()?;

        // Check if this table should use the override batch
        if let Some((override_label, override_batch)) = override_entry {
            if table_name == override_label {
                arrow_datasets.insert(table_name, override_batch.clone());
                continue;
            }
        }

        let batch = python_dataset_to_batch(&value)?;
        arrow_datasets.insert(table_name, batch);
    }
    Ok(arrow_datasets)
}

fn try_execute_with_lance_index(
    py: Python,
    query: &RustCypherQuery,
    datasets: &Bound<'_, PyDict>,
    vector_search: &VectorSearch,
) -> PyResult<Option<RecordBatch>> {
    // Only use vector-first path for simple queries without filters.
    // Queries with WITH/WHERE clauses need the standard rerank path to ensure correct semantics.
    let ast = query.ast();
    if ast.with_clause.is_some()
        || ast.where_clause.is_some()
        || ast.post_with_where_clause.is_some()
    {
        return Ok(None);
    }

    let query_vector = match vector_search.inner.get_query_vector() {
        Some(vec) => vec.to_vec(),
        None => {
            return Err(PyValueError::new_err(
                "VectorSearch.query_vector is required when use_lance_index is enabled",
            ))
        }
    };

    let (alias, column) = split_vector_column(vector_search.inner.column());
    let label = resolve_vector_label(query, alias.as_deref())?;
    let label = match label {
        Some(label) => label,
        None => return Ok(None),
    };

    let dataset_value = match datasets.get_item(&label)? {
        Some(value) => value,
        None => return Ok(None),
    };

    if !is_lance_dataset(&dataset_value)? {
        return Ok(None);
    }

    let metric_str = match vector_search.inner.get_metric() {
        RustDistanceMetric::L2 => "l2",
        RustDistanceMetric::Cosine => "cosine",
        RustDistanceMetric::Dot => "dot",
    };

    // Build the `nearest` dict for Lance's to_table() ANN query.
    // Setting use_index=true tells Lance to use the ANN index if available,
    // otherwise it falls back to flat (brute-force) search.
    let nearest = PyDict::new(py);
    nearest.set_item("column", column)?;
    nearest.set_item("k", vector_search.inner.get_top_k())?;
    nearest.set_item("q", query_vector)?;
    nearest.set_item("metric", metric_str)?;
    nearest.set_item("use_index", true)?;

    let kwargs = PyDict::new(py);
    kwargs.set_item("nearest", nearest)?;

    let table = dataset_value.call_method("to_table", (), Some(&kwargs))?;
    let batch = python_any_to_record_batch(&table)?;
    let batch = normalize_record_batch(batch)?;

    let arrow_datasets = python_datasets_to_batches_with_override(datasets, &label, &batch)?;

    let inner_query = query.clone();
    let result = RT
        .block_on(Some(py), inner_query.execute(arrow_datasets, None))?
        .map_err(graph_error_to_pyerr)?;

    Ok(Some(result))
}

fn split_vector_column(column: &str) -> (Option<String>, &str) {
    let mut parts = column.splitn(2, '.');
    let first = parts.next().unwrap_or(column);
    if let Some(rest) = parts.next() {
        (Some(first.to_string()), rest)
    } else {
        (None, column)
    }
}

fn resolve_vector_label(query: &RustCypherQuery, alias: Option<&str>) -> PyResult<Option<String>> {
    let alias_map = alias_map_from_query(query);
    if let Some(alias) = alias {
        return Ok(alias_map.get(alias).cloned());
    }
    if alias_map.len() == 1 {
        return Ok(alias_map.values().next().cloned());
    }
    Ok(None)
}

fn alias_map_from_query(query: &RustCypherQuery) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let ast = query.ast();
    for clause in ast
        .reading_clauses
        .iter()
        .chain(ast.post_with_reading_clauses.iter())
        .chain(
            ast.further_with_stages
                .iter()
                .flat_map(|stage| stage.reading_clauses.iter()),
        )
    {
        if let ReadingClause::Match(match_clause) = clause {
            for pattern in &match_clause.patterns {
                collect_aliases_from_pattern(pattern, &mut map);
            }
        }
    }
    map
}

fn collect_aliases_from_pattern(pattern: &GraphPattern, map: &mut HashMap<String, String>) {
    match pattern {
        GraphPattern::Node(node) => {
            if let (Some(var), Some(label)) = (node.variable.as_ref(), node.labels.first()) {
                map.entry(var.clone()).or_insert_with(|| label.clone());
            }
        }
        GraphPattern::Path(path) => {
            if let (Some(var), Some(label)) = (
                path.start_node.variable.as_ref(),
                path.start_node.labels.first(),
            ) {
                map.entry(var.clone()).or_insert_with(|| label.clone());
            }
            for segment in &path.segments {
                if let (Some(var), Some(label)) = (
                    segment.end_node.variable.as_ref(),
                    segment.end_node.labels.first(),
                ) {
                    map.entry(var.clone()).or_insert_with(|| label.clone());
                }
            }
        }
    }
}

fn normalize_record_batch(batch: RecordBatch) -> PyResult<RecordBatch> {
    if batch.schema().metadata().is_empty() {
        return Ok(batch);
    }

    // DataFusion expects stable, metadata-free schemas across optimization passes.
    // Rebuild the schema without the PyArrow-specific metadata to avoid mismatches.
    let columns = batch.columns().to_vec();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to strip metadata from Arrow batch: {}", e))
    })
}

/// True for `lance.LanceDataset` and its subclasses.
fn is_lance_dataset(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    // pylance is a hard dependency, so a failed import only happens in a broken install.
    match value.py().import("lance") {
        Ok(lance) => value.is_instance(&lance.getattr("LanceDataset")?),
        Err(_) => Ok(false),
    }
}

fn python_any_to_record_batch(value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    use arrow::pyarrow::FromPyArrow;

    if let Ok(batch) = RecordBatch::from_pyarrow_bound(value) {
        return Ok(batch);
    }

    let mut reader = ArrowArrayStreamReader::from_pyarrow_bound(value)?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next()
        .transpose()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to read Arrow batch: {}", e)))?
    {
        batches.push(batch);
    }

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    concat_batches(&schema, &batches)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to concatenate batches: {}", e)))
}

fn record_batch_to_python_table(
    py: Python,
    batch: &arrow_array::RecordBatch,
) -> PyResult<Py<PyAny>> {
    use arrow::pyarrow::ToPyArrow;
    use pyo3::types::PyList;

    // Convert RecordBatch -> PyArrow.RecordBatch
    let py_rb = batch.to_pyarrow(py)?;

    // Build pyarrow.Table from batches
    let pa = py.import("pyarrow")?;
    let table_cls = pa.getattr("Table")?;
    let batches = PyList::new(py, [py_rb])?;
    let table = table_cls.call_method1("from_batches", (batches,))?;
    Ok(table.unbind())
}

/// One input dataset as registered with a [`CypherEngine`].
#[derive(Clone)]
enum EngineSource {
    /// On-disk Lance dataset, read lazily through `LanceTableProvider`.
    ///
    /// `dataset` is pinned to one version; only `refresh()` replaces it. `uri` and
    /// `storage_options` are kept so refresh can reopen the same location with the
    /// same credentials.
    Lance {
        uri: String,
        storage_options: HashMap<String, String>,
        dataset: Arc<Dataset>,
    },
    /// In-memory Arrow data supplied by the caller. Treated as immutable: replacing
    /// it requires a new engine.
    Arrow(RecordBatch),
}

/// Everything a query needs, built once and shared read-only by concurrent queries.
///
/// `refresh()` builds a complete replacement and swaps it in; queries already running
/// keep the `Arc` they started with, so they finish on their original snapshot.
struct EngineState {
    sources: HashMap<String, EngineSource>,
    plans: Mutex<VecDeque<CachedQuery>>,
    catalog: Arc<dyn lance_graph::GraphSourceCatalog>,
    context: SessionContext,
    /// CSR adjacencies by relationship type; empty unless the engine uses CSR.
    csr: HashMap<String, CsrEntry>,
    /// Node id -> row indexes by node label, for the fused target lookup; same lifecycle.
    node_index: HashMap<String, NodeIndexEntry>,
    /// Join-key distinct counts by dataset name, reported to DataFusion's join planner.
    key_stats: HashMap<String, KeyStatsEntry>,
}

/// A small LRU of unoptimized plans. Values are bound after lookup, before optimization.
struct CachedQuery {
    text: String,
    parameter_types: Vec<(String, u8)>,
    query: RustCypherQuery,
}

// Containers change expression shape; keep them on the uncached path.
fn parameter_types(parameters: &HashMap<String, JsonValue>) -> Option<Vec<(String, u8)>> {
    let mut types = parameters
        .iter()
        .map(|(name, value)| {
            let kind = match value {
                JsonValue::Null => 0,
                JsonValue::Bool(_) => 1,
                JsonValue::String(_) => 2,
                JsonValue::Number(value) if value.is_i64() => 3,
                JsonValue::Number(value) if value.is_f64() => 4,
                _ => return None,
            };
            Some((name.clone(), kind))
        })
        .collect::<Option<Vec<_>>>()?;
    types.sort_unstable();
    Some(types)
}

/// Distinct counts for one dataset's node-id and edge-endpoint columns.
#[derive(Clone)]
struct KeyStatsEntry {
    /// Dataset the counts describe; reused while it is the same snapshot.
    source: EngineSource,
    /// Actual column name -> distinct non-null values.
    distinct: HashMap<String, usize>,
    /// For a relationship dataset: mean fan-out by direction (outgoing, incoming).
    fan_out: Option<(String, f64, f64)>,
}

/// Id -> row index for one node table, plus the source it came from.
#[derive(Clone)]
struct NodeIndexEntry {
    source: EngineSource,
    rows: Arc<NodeRows>,
    build_ms: f64,
}

/// Both-direction adjacencies for one relationship type, plus the source they came from.
#[derive(Clone)]
struct CsrEntry {
    /// Dataset the adjacencies reflect; reused while it is the same snapshot.
    source: EngineSource,
    outgoing: Arc<Adjacency>,
    incoming: Arc<Adjacency>,
    /// Wall time of the last build or delta application.
    build_ms: f64,
    /// How the last refresh produced this entry: build, rebuild, delta or reused.
    update: &'static str,
}

/// Rebuild instead of applying a delta once pending changes (appended edges +
/// tombstones, accumulated since the last full build) would exceed this fraction
/// of the base. Lookups pay for the delta on every query, and a rebuild folds it in.
///
/// This threshold also bounds refresh cost: each delta refresh copies the whole
/// accumulated delta (see the design note on `lance_graph::csr_expand::Adjacency`).
/// Raising it makes refreshes slower as well as lookups.
// shortcut: fixed threshold, chosen from python/benchmarks/csr_delta_accumulated.py; tune per workload if deltas grow large.
const CSR_REBUILD_FRACTION: f64 = 0.25;

/// Apply the edge changes between `old`'s snapshot and `source` as a delta.
///
/// `Ok(None)` means rebuild: not both Lance snapshots, the diff is not meaningful
/// or failed (e.g. old files already cleaned up), or the delta would be too large.
fn try_apply_csr_delta(
    py: Python,
    old: &CsrEntry,
    source: &EngineSource,
    src: &str,
    dst: &str,
) -> PyResult<Option<CsrEntry>> {
    let (
        EngineSource::Lance {
            dataset: old_ds, ..
        },
        EngineSource::Lance {
            dataset: new_ds, ..
        },
    ) = (&old.source, source)
    else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let Ok(Some(changes)) = RT.block_on(Some(py), edge_changes(old_ds, new_ds, src, dst))? else {
        return Ok(None);
    };
    let elapsed = || started.elapsed().as_secs_f64() * 1e3;
    if changes.is_empty() {
        return Ok(Some(CsrEntry {
            source: source.clone(),
            build_ms: elapsed(),
            update: "reused",
            ..old.clone()
        }));
    }
    let base = old.outgoing.base_edges() as f64;
    let pending = old.outgoing.delta_edges() + old.outgoing.tombstones() + changes.len() as u64;
    if pending as f64 > CSR_REBUILD_FRACTION * base {
        return Ok(None);
    }
    let flip = |edges: &[(i64, i64)]| edges.iter().map(|&(a, b)| (b, a)).collect::<Vec<_>>();
    let outgoing = old.outgoing.with_changes(&changes.added, &changes.removed);
    let incoming = old
        .incoming
        .with_changes(&flip(&changes.added), &flip(&changes.removed));
    Ok(Some(CsrEntry {
        source: source.clone(),
        outgoing: Arc::new(outgoing),
        incoming: Arc::new(incoming),
        build_ms: elapsed(),
        update: "delta",
    }))
}

impl EngineSource {
    /// Pinned Lance version, or `None` for PyArrow input.
    fn lance_version(&self) -> Option<u64> {
        match self {
            Self::Lance { dataset, .. } => Some(dataset.manifest().version),
            Self::Arrow(_) => None,
        }
    }

    /// True when both refer to the same immutable data (same pinned Lance handle or
    /// same Arrow batch), so anything derived from one is valid for the other.
    fn same_data(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Lance { dataset: a, .. }, Self::Lance { dataset: b, .. }) => Arc::ptr_eq(a, b),
            (Self::Arrow(a), Self::Arrow(b)) => {
                a.num_columns() == b.num_columns()
                    && a.columns()
                        .iter()
                        .zip(b.columns())
                        .all(|(x, y)| x.to_data().ptr_eq(&y.to_data()))
            }
            _ => false,
        }
    }
}

impl EngineState {
    /// Mean fan-out of every relationship type with key statistics, both directions.
    fn fan_out(&self) -> lance_graph::logical_plan::FanOut {
        self.key_stats
            .values()
            .filter_map(|entry| entry.fan_out.as_ref())
            .flat_map(|(rel_type, outgoing, incoming)| {
                [
                    ((rel_type.clone(), true), *outgoing),
                    ((rel_type.clone(), false), *incoming),
                ]
            })
            .collect()
    }

    fn adjacency_catalog(&self) -> AdjacencyCatalog {
        let mut catalog = AdjacencyCatalog::default();
        for (rel_type, entry) in &self.csr {
            catalog.insert(rel_type, true, entry.outgoing.clone());
            catalog.insert(rel_type, false, entry.incoming.clone());
        }
        for (label, entry) in &self.node_index {
            catalog.insert_nodes(label, entry.rows.clone());
        }
        catalog
    }
}

/// Open a Lance dataset in Rust. `version = None` means the latest version.
fn open_lance_dataset(
    py: Python,
    uri: &str,
    version: Option<u64>,
    storage_options: &HashMap<String, String>,
) -> PyResult<Dataset> {
    let mut builder = DatasetBuilder::from_uri(uri).with_storage_options(storage_options.clone());
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    RT.block_on(Some(py), builder.load())?.map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to open Lance dataset {}: {}", uri, e))
    })
}

/// Same version number and manifest commit time means the same snapshot.
///
/// The timestamp distinguishes a table deleted and recreated at the same location,
/// which restarts version numbering at 1.
fn same_lance_snapshot(a: &Dataset, b: &Dataset) -> bool {
    a.manifest().version == b.manifest().version
        && a.manifest().timestamp_nanos == b.manifest().timestamp_nanos
}

/// Convert one Python input value into an engine source without reading its rows.
fn python_value_to_source(
    py: Python,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<EngineSource> {
    if !is_lance_dataset(value)? {
        return Ok(EngineSource::Arrow(python_dataset_to_batch(value)?));
    }
    // Datasets opened through a namespace client get credentials vended at runtime;
    // reopening by URI would lose them, so refuse rather than read with the wrong identity.
    for attr in ["_namespace_client", "_base_store_params"] {
        if value.getattr(attr).map(|v| !v.is_none()).unwrap_or(false) {
            return Err(PyValueError::new_err(format!(
                "Dataset '{}' was opened with {}; CypherEngine can only reopen Lance datasets by URI \
                 and storage options",
                name, attr
            )));
        }
    }
    let uri: String = value.getattr("uri")?.extract()?;
    let version: u64 = value.getattr("version")?.extract()?;
    let storage_options: HashMap<String, String> = value
        .getattr("initial_storage_options")?
        .extract::<Option<HashMap<String, String>>>()?
        .unwrap_or_default();
    // Open the exact version the caller passed so the engine never silently jumps to latest.
    let dataset = open_lance_dataset(py, &uri, Some(version), &storage_options)?;
    Ok(EngineSource::Lance {
        uri,
        storage_options,
        dataset: Arc::new(dataset),
    })
}

/// Check that every configured id/endpoint column exists in its dataset.
///
/// Labels and columns compare case-insensitively, matching the planner. Mappings
/// without a dataset are left for query time, as before, so unused labels stay legal.
fn validate_sources(
    config: &RustGraphConfig,
    schemas: &HashMap<String, arrow_schema::SchemaRef>,
) -> PyResult<()> {
    let schema_for = |label: &str| {
        schemas
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(label))
            .map(|(_, schema)| schema)
    };
    let check = |label: &str, column: &str| -> PyResult<()> {
        if let Some(schema) = schema_for(label) {
            if !schema
                .fields()
                .iter()
                .any(|f| f.name().eq_ignore_ascii_case(column))
            {
                return Err(PyValueError::new_err(format!(
                    "Dataset for '{}' has no column '{}' required by the graph configuration",
                    label, column
                )));
            }
        }
        Ok(())
    };
    for (_, mapping) in config.iter_node_mappings() {
        check(&mapping.label, &mapping.id_field)?;
    }
    for (_, mapping) in config.iter_relationship_mappings() {
        check(&mapping.relationship_type, &mapping.source_id_field)?;
        check(&mapping.relationship_type, &mapping.target_id_field)?;
    }
    Ok(())
}

/// Validate sources and register them in a fresh catalog and session context.
fn build_engine_state(
    py: Python,
    config: &RustGraphConfig,
    sources: HashMap<String, EngineSource>,
    use_csr: bool,
    previous: Option<&EngineState>,
) -> PyResult<EngineState> {
    // The CSR planner only changes plans that contain CSR nodes, so install it always.
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(CsrQueryPlanner))
            // Move target-only filters into CSR nodes first, so pruning can then drop
            // the filter columns from the take.
            .with_optimizer_rule(Arc::new(PushCsrTargetFilters))
            .with_optimizer_rule(Arc::new(PruneCsrTargetColumns))
            .build(),
    );
    let mut schemas = HashMap::new();
    let mut providers: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();

    for (name, source) in &sources {
        let provider: Arc<dyn TableProvider> = match source {
            // No _rowid/_rowaddr columns, so Lance and Arrow inputs expose the same schema.
            EngineSource::Lance { dataset, .. } => {
                Arc::new(LanceTableProvider::new(dataset.clone(), false, false))
            }
            EngineSource::Arrow(batch) => Arc::new(
                MemTable::try_new(batch.schema(), memtable_partitions(batch)).map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "Failed to create MemTable for {}: {}",
                        name, e
                    ))
                })?,
            ),
        };
        schemas.insert(name.clone(), provider.schema());
        // Registered unwrapped first: the CSR and key-statistics builds below scan it.
        ctx.register_table(name.to_lowercase(), provider.clone())
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to register table {}: {}", name, e))
            })?;
        providers.insert(name.clone(), provider);
    }

    validate_sources(config, &schemas)?;
    let (csr, node_index) = if use_csr {
        (
            build_csr_entries(py, config, &sources, &schemas, &ctx, previous)?,
            build_node_index(py, config, &sources, previous)?,
        )
    } else {
        (HashMap::new(), HashMap::new())
    };
    let key_stats = build_key_statistics(py, config, &sources, &schemas, &ctx, &csr, previous)?;

    let mut catalog = InMemoryCatalog::new();
    for (name, provider) in providers {
        // Re-register with join-key statistics, so plans see them; see `key_statistics`.
        let provider: Arc<dyn TableProvider> = match key_stats.get(&name) {
            Some(entry) => Arc::new(KeyStatisticsProvider::new(provider, entry.distinct.clone())),
            None => provider,
        };
        let table = name.to_lowercase();
        ctx.deregister_table(table.as_str())
            .and_then(|_| ctx.register_table(table.as_str(), provider.clone()))
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to register table {}: {}", name, e))
            })?;

        // Every dataset is registered as both a node and a relationship source; the
        // GraphConfig and query pattern decide which interpretation the planner uses.
        let table_source = Arc::new(DefaultTableSource::new(provider));
        catalog = catalog
            .with_node_source(&name, table_source.clone())
            .with_relationship_source(&name, table_source);
    }
    Ok(EngineState {
        sources,
        plans: Mutex::default(),
        catalog: Arc::new(catalog),
        context: ctx,
        csr,
        node_index,
        key_stats,
    })
}

/// Build (or reuse) an id -> row index for every node label with unique integer ids.
///
/// Reused while the node dataset is the same snapshot; edge changes never touch it.
/// Labels whose ids are not integers or not unique get no entry, so their targets
/// keep the hash join.
fn build_node_index(
    py: Python,
    config: &RustGraphConfig,
    sources: &HashMap<String, EngineSource>,
    previous: Option<&EngineState>,
) -> PyResult<HashMap<String, NodeIndexEntry>> {
    let mut entries = HashMap::new();
    for (_, mapping) in config.iter_node_mappings() {
        let label = &mapping.label;
        let Some(source) = sources
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(label))
            .map(|(_, source)| source)
        else {
            continue;
        };
        if let Some(old) = previous.and_then(|p| p.node_index.get(label)) {
            if old.source.same_data(source) {
                entries.insert(label.clone(), old.clone());
                continue;
            }
        }
        let started = std::time::Instant::now();
        let rows = match source {
            EngineSource::Lance { dataset, .. } => RT.block_on(
                Some(py),
                NodeRows::from_lance(dataset.clone(), &mapping.id_field),
            )?,
            EngineSource::Arrow(batch) => NodeRows::from_batch(batch.clone(), &mapping.id_field),
        }
        .map_err(graph_error_to_pyerr)?;
        if let Some(rows) = rows {
            entries.insert(
                label.clone(),
                NodeIndexEntry {
                    source: source.clone(),
                    rows: Arc::new(rows),
                    build_ms: started.elapsed().as_secs_f64() * 1e3,
                },
            );
        }
    }
    Ok(entries)
}

/// Rows per in-memory batch, DataFusion's default `batch_size`.
const MEMTABLE_BATCH_ROWS: usize = 8192;

/// Split one in-memory table into zero-copy slices spread over one partition per core.
///
/// A `MemTable` holding a single large batch is one partition with one batch, and
/// `RoundRobinBatch` repartitioning moves whole batches, so every operator that does
/// not hash-repartition (such as `CsrExpandExec`) would run on one thread over all of
/// it. Slicing shares the input's buffers; nothing is copied.
fn memtable_partitions(batch: &RecordBatch) -> Vec<Vec<RecordBatch>> {
    let partitions = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut out = vec![Vec::new(); partitions];
    let rows = batch.num_rows();
    if rows == 0 {
        return vec![vec![batch.clone()]];
    }
    for (i, offset) in (0..rows).step_by(MEMTABLE_BATCH_ROWS).enumerate() {
        let len = MEMTABLE_BATCH_ROWS.min(rows - offset);
        out[i % partitions].push(batch.slice(offset, len));
    }
    out.retain(|p| !p.is_empty());
    out
}

/// Distinct counts for every dataset's node-id and edge-endpoint columns.
///
/// DataFusion 54 sizes hash joins from these (see `lance_graph::key_statistics`).
/// Node ids count as unique (the row count). Edge endpoints use the CSR's distinct
/// source and target counts when the CSR is built, and otherwise one scan of the two
/// endpoint columns; only integer endpoints are counted. Reused while a dataset is
/// the same snapshot, so an unchanged `refresh()` costs nothing.
// shortcut: the fallback scan reads every edge endpoint at construction and on each changed refresh (about one CSR build's scan); switch to an approximate counter (HyperLogLog) or Lance column statistics if that shows up in startup time.
fn build_key_statistics(
    py: Python,
    config: &RustGraphConfig,
    sources: &HashMap<String, EngineSource>,
    schemas: &HashMap<String, arrow_schema::SchemaRef>,
    ctx: &SessionContext,
    csr: &HashMap<String, CsrEntry>,
    previous: Option<&EngineState>,
) -> PyResult<HashMap<String, KeyStatsEntry>> {
    let mut entries = HashMap::new();
    for (name, source) in sources {
        if let Some(old) = previous.and_then(|p| p.key_stats.get(name)) {
            if old.source.same_data(source) {
                entries.insert(name.clone(), old.clone());
                continue;
            }
        }
        let column = |wanted: &str| {
            schemas[name]
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(wanted))
                .cloned()
        };
        let rows = || -> PyResult<usize> {
            Ok(match source {
                EngineSource::Arrow(batch) => batch.num_rows(),
                EngineSource::Lance { dataset, .. } => RT
                    .block_on(Some(py), dataset.count_rows(None))?
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to count rows of {}: {}", name, e))
                    })?,
            })
        };
        let mut distinct = HashMap::new();
        let mut to_count = Vec::new();
        // Relationship type and endpoint columns, for the fan-out once counts are known.
        let mut endpoints = None;
        for (_, mapping) in config.iter_node_mappings() {
            if !mapping.label.eq_ignore_ascii_case(name) {
                continue;
            }
            if let Some(id) = column(&mapping.id_field) {
                distinct.insert(id.name().clone(), rows()?);
            }
        }
        for (_, mapping) in config.iter_relationship_mappings() {
            if !mapping.relationship_type.eq_ignore_ascii_case(name) {
                continue;
            }
            let (Some(src), Some(dst)) = (
                column(&mapping.source_id_field),
                column(&mapping.target_id_field),
            ) else {
                continue;
            };
            endpoints = Some((
                mapping.relationship_type.to_lowercase(),
                src.name().clone(),
                dst.name().clone(),
            ));
            match csr.get(&mapping.relationship_type) {
                // Built from this same snapshot in this refresh, so its counts are current.
                Some(entry) => {
                    distinct.insert(src.name().clone(), entry.outgoing.num_sources());
                    distinct.insert(dst.name().clone(), entry.incoming.num_sources());
                }
                None => to_count.extend(
                    [src, dst]
                        .into_iter()
                        .filter(|f| is_supported_id_type(f.data_type()))
                        .map(|f| f.name().clone()),
                ),
            }
        }
        to_count.retain(|c| !distinct.contains_key(c));
        if !to_count.is_empty() {
            let table = name.to_lowercase();
            let counts = RT
                .block_on(Some(py), async {
                    let df = ctx.table(table.as_str()).await?;
                    let names: Vec<&str> = to_count.iter().map(String::as_str).collect();
                    let mut stream = df.select_columns(&names)?.execute_stream().await?;
                    let mut seen: Vec<std::collections::HashSet<i64>> =
                        vec![Default::default(); names.len()];
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        for (i, values) in seen.iter_mut().enumerate() {
                            let column = arrow::compute::cast(
                                batch.column(i),
                                &arrow_schema::DataType::Int64,
                            )?;
                            let column = column
                                .as_any()
                                .downcast_ref::<arrow_array::Int64Array>()
                                .expect("cast to Int64");
                            values.extend(column.iter().flatten());
                        }
                    }
                    Ok::<_, datafusion::error::DataFusionError>(
                        seen.into_iter().map(|s| s.len()).collect::<Vec<_>>(),
                    )
                })?
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to count join keys of {}: {}", name, e))
                })?;
            distinct.extend(to_count.into_iter().zip(counts));
        }
        let fan_out = match endpoints {
            Some((rel_type, src, dst)) if distinct.contains_key(&src) => {
                let edges = rows()? as f64;
                let per = |c: &str| edges / distinct.get(c).copied().unwrap_or(1).max(1) as f64;
                Some((rel_type, per(&src), per(&dst)))
            }
            _ => None,
        };
        if !distinct.is_empty() {
            entries.insert(
                name.clone(),
                KeyStatsEntry {
                    source: source.clone(),
                    distinct,
                    fan_out,
                },
            );
        }
    }
    Ok(entries)
}

/// Build (or reuse) CSR adjacencies for every relationship type with integer endpoints.
///
/// An entry from `previous` is reused when its relationship dataset is the same
/// snapshot; node-table changes never invalidate it, since the CSR stores raw
/// target ids. Relationship types with non-integer endpoints get no entry and
/// queries over them use the join path.
fn build_csr_entries(
    py: Python,
    config: &RustGraphConfig,
    sources: &HashMap<String, EngineSource>,
    schemas: &HashMap<String, arrow_schema::SchemaRef>,
    ctx: &SessionContext,
    previous: Option<&EngineState>,
) -> PyResult<HashMap<String, CsrEntry>> {
    let mut entries = HashMap::new();
    for (_, mapping) in config.iter_relationship_mappings() {
        let rel_type = &mapping.relationship_type;
        let Some((name, source)) = sources
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(rel_type))
        else {
            continue;
        };
        if let Some(old) = previous.and_then(|p| p.csr.get(rel_type)) {
            if old.source.same_data(source) {
                entries.insert(rel_type.clone(), old.clone());
                continue;
            }
        }
        let field = |column: &str| {
            schemas[name]
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(column))
                .cloned()
        };
        let (Some(src), Some(dst)) = (
            field(&mapping.source_id_field),
            field(&mapping.target_id_field),
        ) else {
            continue;
        };
        if !is_supported_id_type(src.data_type()) || !is_supported_id_type(dst.data_type()) {
            continue;
        }

        let previous_entry = previous.and_then(|p| p.csr.get(rel_type));
        if let Some(old) = previous_entry {
            if let Some(entry) = try_apply_csr_delta(py, old, source, src.name(), dst.name())? {
                entries.insert(rel_type.clone(), entry);
                continue;
            }
        }

        let started = std::time::Instant::now();
        let table = name.to_lowercase();
        let (outgoing, incoming) = RT.block_on(Some(py), async {
            // Two projected scans of the endpoint columns, one per direction.
            let scan = || async {
                ctx.table(table.as_str())
                    .await?
                    .select_columns(&[src.name().as_str(), dst.name().as_str()])?
                    .execute_stream()
                    .await
            };
            let scan_err = |e: datafusion::error::DataFusionError| {
                PyRuntimeError::new_err(format!("Failed to scan edges for CSR: {}", e))
            };
            let outgoing =
                Adjacency::build(scan().await.map_err(scan_err)?, src.name(), dst.name())
                    .await
                    .map_err(graph_error_to_pyerr)?;
            let incoming =
                Adjacency::build(scan().await.map_err(scan_err)?, dst.name(), src.name())
                    .await
                    .map_err(graph_error_to_pyerr)?;
            Ok::<_, PyErr>((outgoing, incoming))
        })??;
        entries.insert(
            rel_type.clone(),
            CsrEntry {
                source: source.clone(),
                outgoing: Arc::new(outgoing),
                incoming: Arc::new(incoming),
                build_ms: started.elapsed().as_secs_f64() * 1e3,
                update: if previous_entry.is_some() {
                    "rebuild"
                } else {
                    "build"
                },
            },
        );
    }
    Ok(entries)
}

/// Blocking adapter from a DataFusion stream to Arrow's `RecordBatchReader`.
///
/// PyArrow calls `next()` from its C stream interface with the GIL released, so
/// blocking on the runtime here does not stall other Python threads. Ctrl-C is not
/// checked between batches; dropping the reader cancels the remaining work.
struct BlockingBatchReader {
    schema: arrow_schema::SchemaRef,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
}

impl Iterator for BlockingBatchReader {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        RT.runtime
            .block_on(self.stream.next())
            .map(|r| r.map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for BlockingBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

/// A timed reader owns its stream independently of calls into Arrow's C interface.
struct TimedBatchState {
    stream: Option<datafusion::physical_plan::SendableRecordBatchStream>,
    expired: bool,
}

struct DeadlineBatchReader {
    schema: arrow_schema::SchemaRef,
    state: Arc<tokio::sync::Mutex<TimedBatchState>>,
    deadline: tokio::time::Instant,
    watchdog: tokio::task::JoinHandle<()>,
    finished: bool,
}

fn stream_timeout_error() -> arrow_schema::ArrowError {
    arrow_schema::ArrowError::ExternalError(Box::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        QUERY_TIMEOUT_MESSAGE,
    )))
}

impl Iterator for DeadlineBatchReader {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let result = RT.runtime.block_on(async {
            let mut state = self.state.lock().await;
            if state.expired || tokio::time::Instant::now() >= self.deadline {
                state.stream.take();
                state.expired = true;
                return Some(Err(stream_timeout_error()));
            }
            let stream = state.stream.as_mut()?;
            let result = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(self.deadline) => None,
                result = stream.next() => Some(result),
            };
            match result {
                None => {
                    state.stream.take();
                    state.expired = true;
                    Some(Err(stream_timeout_error()))
                }
                Some(result) => {
                    if tokio::time::Instant::now() >= self.deadline {
                        state.stream.take();
                        state.expired = true;
                        return Some(Err(stream_timeout_error()));
                    }
                    if result.is_none() {
                        state.stream.take();
                    }
                    result.map(|r| {
                        r.map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e)))
                    })
                }
            }
        });
        if result.as_ref().is_none_or(|r| r.is_err()) {
            self.finished = true;
            self.watchdog.abort();
        }
        result
    }
}

impl RecordBatchReader for DeadlineBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

impl Drop for DeadlineBatchReader {
    fn drop(&mut self) {
        self.watchdog.abort();
        if let Ok(mut state) = self.state.try_lock() {
            state.stream.take();
        }
    }
}

fn reader_with_deadline(
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    deadline: QueryDeadline,
) -> Box<dyn RecordBatchReader + Send> {
    let schema = stream.schema();
    let Some(deadline) = deadline.instant() else {
        return Box::new(BlockingBatchReader { schema, stream });
    };
    let state = Arc::new(tokio::sync::Mutex::new(TimedBatchState {
        stream: Some(stream),
        expired: false,
    }));
    let watched = state.clone();
    let watchdog = RT.runtime.spawn(async move {
        tokio::time::sleep_until(deadline).await;
        let mut state = watched.lock().await;
        if state.stream.take().is_some() {
            state.expired = true;
        }
    });
    Box::new(DeadlineBatchReader {
        schema,
        state,
        deadline,
        watchdog,
        finished: false,
    })
}

/// Cypher query engine with cached catalog for efficient multi-query execution
///
/// The engine registers each dataset once and reuses the catalog and DataFusion
/// context across queries.
///
/// Lance datasets are read lazily from storage, pinned to the version passed in:
/// writes made later (by this or another process) are not visible until
/// ``refresh()``. Each dataset is pinned separately; versions across tables are not
/// a transactional multi-table snapshot. PyArrow tables are held in memory and
/// never change; supply a new engine to replace them.
///
/// Examples
/// --------
/// >>> from lance_csr import CypherEngine, GraphConfig
/// >>> import pyarrow as pa
/// >>>
/// >>> # Setup
/// >>> config = GraphConfig.builder() \\
/// ...     .node_table("Person", "id") \\
/// ...     .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id") \\
/// ...     .build()
/// >>>
/// >>> datasets = {
/// ...     "Person": person_table,
/// ...     "KNOWS": knows_table
/// ... }
/// >>>
/// >>> # Create engine once
/// >>> engine = CypherEngine(config, datasets)
/// >>>
/// >>> # Execute multiple queries efficiently
/// >>> result1 = engine.execute("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
/// >>> result2 = engine.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name")
/// >>> result3 = engine.execute("MATCH (p:Person) RETURN count(*)")
#[pyclass(name = "CypherEngine", module = "lance_csr.graph")]
pub struct CypherEngine {
    config: RustGraphConfig,
    state: RwLock<Arc<EngineState>>,
    use_csr: bool,
}

impl CypherEngine {
    /// The current snapshot. Cloning the `Arc` lets a query outlive a concurrent refresh.
    fn snapshot(&self) -> Arc<EngineState> {
        self.state
            .read()
            .expect("engine state lock poisoned")
            .clone()
    }

    /// Publish only if no other refresh replaced the snapshot used to build this state.
    fn try_publish_refresh(&self, expected: &Arc<EngineState>, replacement: EngineState) -> bool {
        let mut current = self.state.write().expect("engine state lock poisoned");
        if !Arc::ptr_eq(&current, expected) {
            return false;
        }
        *current = Arc::new(replacement);
        true
    }

    fn parse(
        &self,
        py: Python,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
        state: &EngineState,
        use_cache: bool,
    ) -> PyResult<RustCypherQuery> {
        let mut parameters = HashMap::new();
        for (key, value) in params.into_iter().flat_map(|p| p.iter()) {
            parameters.insert(
                key.extract::<String>()?.to_lowercase(),
                python_to_json(&value)?,
            );
        }
        py.detach(|| self.plan_query(query, parameters, state, use_cache))
            .map_err(graph_error_to_pyerr)
    }

    fn plan_query(
        &self,
        text: &str,
        parameters: HashMap<String, JsonValue>,
        state: &EngineState,
        use_cache: bool,
    ) -> Result<RustCypherQuery, RustGraphError> {
        let key = use_cache.then(|| parameter_types(&parameters)).flatten();
        if let Some(key) = &key {
            let mut plans = state.plans.lock().unwrap();
            if let Some(index) = plans
                .iter()
                .position(|entry| entry.text == text && entry.parameter_types == *key)
            {
                let entry = plans.remove(index).unwrap();
                let query = entry.query.clone().with_parameters(parameters);
                plans.push_back(entry);
                return Ok(query);
            }
        }
        let query = RustCypherQuery::new(text)?
            .with_parameters(parameters)
            .with_config(self.config.clone())
            .with_fan_out(state.fan_out());
        let mut query = if self.use_csr {
            query.with_adjacency(state.adjacency_catalog())
        } else {
            query
        };
        if let Some(parameter_types) = key {
            // Some operators require literal values during planning. If a template
            // cannot be built, ordinary planning preserves their existing behavior.
            if query
                .prepare_cached_plan(state.catalog.clone())
                .unwrap_or(false)
            {
                let mut plans = state.plans.lock().unwrap();
                plans
                    .retain(|entry| entry.text != text || entry.parameter_types != parameter_types);
                if plans.len() == 128 {
                    plans.pop_front();
                }
                plans.push_back(CachedQuery {
                    text: text.to_string(),
                    parameter_types,
                    query: query.clone(),
                });
            }
        }
        Ok(query)
    }
}

#[pymethods]
impl CypherEngine {
    /// Create a new CypherEngine with cached catalog
    ///
    /// Parameters
    /// ----------
    /// config : GraphConfig
    ///     The graph configuration defining node labels and relationships
    /// datasets : dict
    ///     Dictionary mapping table names to Lance datasets or PyArrow tables.
    ///     Lance datasets are not read here; PyArrow inputs are held in memory.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no datasets are given, a configured id/endpoint column is missing, or a
    ///     Lance dataset cannot be reopened by URI (for example, namespace-vended
    ///     credentials)
    /// RuntimeError
    ///     If a dataset cannot be opened or registered
    #[new]
    #[pyo3(signature = (config, datasets, use_csr = false))]
    fn new(
        py: Python,
        config: &GraphConfig,
        datasets: &Bound<'_, PyDict>,
        use_csr: bool,
    ) -> PyResult<Self> {
        if datasets.is_empty() {
            return Err(PyValueError::new_err("No input datasets provided"));
        }
        let mut sources = HashMap::new();
        for (key, value) in datasets.iter() {
            let name: String = key.extract()?;
            let source = python_value_to_source(py, &name, &value)?;
            sources.insert(name, source);
        }
        let state = build_engine_state(py, &config.inner, sources, use_csr, None)?;
        Ok(Self {
            config: config.inner.clone(),
            state: RwLock::new(Arc::new(state)),
            use_csr,
        })
    }

    /// Execute a Cypher query and return the full result as a ``pyarrow.Table``.
    ///
    /// The whole result is held in memory; use ``execute_batches`` for large outputs.
    /// ``timeout`` is an optional nonnegative number of seconds for the whole query;
    /// expiry raises ``TimeoutError``. Cancellation is cooperative.
    ///
    /// Parameters
    /// ----------
    /// query : str
    ///     The Cypher query
    /// params : dict, optional
    ///     Values for ``$name`` parameters in the query: numbers, strings, booleans,
    ///     ``None``, lists, string-keyed maps, and ``datetime``/``date`` values (compared as timestamps and dates).
    ///     ``execute_batches`` and ``explain`` take the same argument.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If the query is invalid
    /// RuntimeError
    ///     If query execution fails
    #[pyo3(signature = (query, params = None, *, timeout=None))]
    fn execute(
        &self,
        py: Python,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        let deadline = QueryDeadline::new(timeout)?;
        deadline.check()?;
        let state = self.snapshot();
        let cypher_query = self.parse(py, query, params, &state, true)?;
        let result_batch = RT
            .block_on_until(
                Some(py),
                deadline,
                cypher_query
                    .execute_with_catalog_and_context(state.catalog.clone(), state.context.clone()),
            )?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result_batch)
    }

    /// Execute a Cypher query and stream results as a ``pyarrow.RecordBatchReader``.
    ///
    /// Batches are computed as the reader is consumed. Scans, filters and projections
    /// stream with bounded memory; joins, sorts and aggregations may still buffer.
    /// The reader keeps its snapshot alive, so a concurrent ``refresh()`` does not
    /// affect it. Planning errors raise immediately; execution errors raise while reading.
    /// ``timeout`` keeps one deadline including pauses. Initial expiry raises
    /// ``TimeoutError``; expired reads raise ArrowInvalid: Query timeout exceeded.
    #[pyo3(signature = (query, params = None, *, timeout=None))]
    fn execute_batches(
        &self,
        py: Python,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        use arrow::pyarrow::IntoPyArrow;

        let deadline = QueryDeadline::new(timeout)?;
        deadline.check()?;
        let state = self.snapshot();
        let cypher_query = self.parse(py, query, params, &state, true)?;
        let stream = RT
            .block_on_until(
                Some(py),
                deadline,
                cypher_query.execute_stream_with_catalog_and_context(
                    state.catalog.clone(),
                    state.context.clone(),
                ),
            )?
            .map_err(graph_error_to_pyerr)?;
        deadline.check()?;
        let reader = reader_with_deadline(stream, deadline);
        Ok(reader.into_pyarrow(py)?.unbind())
    }

    /// Return the query plan, including the physical plan with pushed-down scans.
    #[pyo3(signature = (query, params = None))]
    fn explain(
        &self,
        py: Python,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let state = self.snapshot();
        let cypher_query = self.parse(py, query, params, &state, false)?;
        RT.block_on(
            Some(py),
            cypher_query
                .explain_with_catalog_and_context(state.catalog.clone(), state.context.clone()),
        )?
        .map_err(graph_error_to_pyerr)
    }

    /// Move every Lance dataset to its latest version.
    ///
    /// Datasets whose version and commit time are unchanged keep their open handle.
    /// The replacement is validated in full before it is swapped in; on error nothing
    /// changes and the previous snapshot stays usable. Queries already running finish
    /// on the snapshot they started with. PyArrow inputs are unchanged.
    /// If another refresh publishes first, retry from its snapshot instead of replacing
    /// it with a candidate built from an older state.
    fn refresh(&self, py: Python) -> PyResult<()> {
        loop {
            let current = self.snapshot();
            let mut sources = current.sources.clone();
            for source in sources.values_mut() {
                if let EngineSource::Lance {
                    uri,
                    storage_options,
                    dataset,
                } = source
                {
                    let latest = open_lance_dataset(py, uri, None, storage_options)?;
                    if !same_lance_snapshot(&latest, dataset) {
                        *dataset = Arc::new(latest);
                    }
                }
            }
            let state =
                build_engine_state(py, &self.config, sources, self.use_csr, Some(&current))?;
            if self.try_publish_refresh(&current, state) {
                return Ok(());
            }
        }
    }

    /// Node id -> row index statistics per node label (experimental, ``use_csr=True``).
    ///
    /// Same fields as ``csr_stats`` with ``rows`` instead of ``edges``. A label is
    /// missing when its ids are not unique integers; its targets then use a join.
    fn node_index_stats(&self, py: Python) -> PyResult<Py<PyAny>> {
        let stats = PyDict::new(py);
        for (label, entry) in &self.snapshot().node_index {
            let item = PyDict::new(py);
            item.set_item("rows", entry.rows.num_rows())?;
            item.set_item("version", entry.source.lance_version())?;
            item.set_item("build_ms", entry.build_ms)?;
            stats.set_item(label, item)?;
        }
        Ok(stats.into_any().unbind())
    }

    /// CSR adjacency statistics per relationship type (experimental, ``use_csr=True``).
    ///
    /// - ``edges``: live indexed edges (null endpoints are skipped).
    /// - ``delta_edges`` / ``tombstones``: appended and removed edges held outside
    ///   the base CSR since its last full build.
    /// - ``update``: how the last refresh produced it: ``build`` (first),
    ///   ``rebuild`` (full), ``delta`` (changes applied) or ``reused`` (no edge change).
    /// - ``version``: relationship dataset version it reflects (``None`` for PyArrow).
    /// - ``build_ms``: wall time of that build or delta, both directions.
    ///
    /// An identical dict after ``refresh()`` means the snapshot itself was unchanged.
    fn csr_stats(&self, py: Python) -> PyResult<Py<PyAny>> {
        let stats = PyDict::new(py);
        for (rel_type, entry) in &self.snapshot().csr {
            let item = PyDict::new(py);
            item.set_item("edges", entry.outgoing.num_edges())?;
            item.set_item("delta_edges", entry.outgoing.delta_edges())?;
            item.set_item("tombstones", entry.outgoing.tombstones())?;
            item.set_item("update", entry.update)?;
            item.set_item("version", entry.source.lance_version())?;
            item.set_item("build_ms", entry.build_ms)?;
            stats.set_item(rel_type, item)?;
        }
        Ok(stats.into_any().unbind())
    }

    /// Pinned version of each Lance dataset, keyed by dataset name.
    fn dataset_versions(&self) -> HashMap<String, u64> {
        self.snapshot()
            .sources
            .iter()
            .filter_map(|(name, source)| match source {
                EngineSource::Lance { dataset, .. } => {
                    Some((name.clone(), dataset.manifest().version))
                }
                EngineSource::Arrow(_) => None,
            })
            .collect()
    }

    /// Get the graph configuration
    fn config(&self) -> GraphConfig {
        GraphConfig {
            inner: self.config.clone(),
        }
    }

    /// Execute Cypher query with vector reranking
    ///
    /// Convenience method combining graph traversal and vector similarity ranking.
    /// See CypherQuery.execute_with_vector_rerank for detailed documentation.
    ///
    /// Parameters
    /// ----------
    /// query : str
    ///     Cypher query string
    /// vector_search : VectorSearch
    ///     Vector search configuration
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Results sorted by vector similarity
    fn execute_with_vector_rerank(
        &self,
        py: Python,
        query: &str,
        vector_search: &VectorSearch,
    ) -> PyResult<Py<PyAny>> {
        let state = self.snapshot();
        let cypher_query = self.parse(py, query, None, &state, false)?;
        let vs = vector_search.inner.clone();

        // Execute query to get candidates, then apply vector reranking
        let result_batch = RT
            .block_on(Some(py), async move {
                let candidates = cypher_query
                    .execute_with_catalog_and_context(state.catalog.clone(), state.context.clone())
                    .await?;
                vs.search(&candidates).await
            })?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result_batch)
    }

    fn __repr__(&self) -> String {
        format!(
            "CypherEngine(nodes={}, relationships={})",
            self.config.iter_node_mappings().len(),
            self.config.iter_relationship_mappings().len()
        )
    }
}

/// Execute raw SQL queries against in-memory datasets
///
/// This class allows executing standard SQL directly against Arrow tables,
/// without requiring a GraphConfig or Cypher parsing. DataFusion handles
/// SQL parsing and execution.
///
/// Examples
/// --------
/// >>> import pyarrow as pa
/// >>> from lance_csr import SqlQuery
/// >>>
/// >>> person = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "age": [28, 34]})
/// >>> query = SqlQuery("SELECT name, age FROM person WHERE age > 30")
/// >>> result = query.execute({"person": person})
/// >>> print(result.to_pandas())
#[pyclass(name = "SqlQuery", module = "lance_csr.graph")]
pub struct SqlQuery {
    inner: RustSqlQuery,
}

#[pymethods]
impl SqlQuery {
    /// Create a new SQL query
    ///
    /// Parameters
    /// ----------
    /// sql : str
    ///     The SQL query string
    ///
    /// Returns
    /// -------
    /// SqlQuery
    ///     A new SQL query instance
    #[new]
    fn new(sql: &str) -> Self {
        Self {
            inner: RustSqlQuery::new(sql),
        }
    }

    /// Get the SQL query text
    fn sql(&self) -> &str {
        self.inner.sql()
    }

    /// Execute query against in-memory datasets
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to PyArrow tables or Lance datasets
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Query results as Arrow table
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If query execution fails
    fn execute(&self, py: Python, datasets: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let arrow_datasets = python_datasets_to_batches(datasets)?;
        let inner = self.inner.clone();

        let result_batch = RT
            .block_on(Some(py), inner.execute(arrow_datasets))?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result_batch)
    }

    /// Return the execution plan as a string
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to PyArrow tables or Lance datasets
    ///
    /// Returns
    /// -------
    /// str
    ///     The DataFusion logical and physical execution plan
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If planning fails
    fn explain(&self, py: Python, datasets: &Bound<'_, PyDict>) -> PyResult<String> {
        let arrow_datasets = python_datasets_to_batches(datasets)?;
        let inner = self.inner.clone();

        let plan = RT
            .block_on(Some(py), inner.explain(arrow_datasets))?
            .map_err(graph_error_to_pyerr)?;

        Ok(plan)
    }

    fn __repr__(&self) -> String {
        format!("SqlQuery(\"{}\")", self.inner.sql())
    }
}

/// Cached SQL execution engine for running multiple queries against the same datasets
///
/// This class registers datasets once during initialization and reuses the
/// DataFusion SessionContext for subsequent queries, avoiding repeated setup.
///
/// Examples
/// --------
/// >>> from lance_csr import SqlEngine
/// >>> import pyarrow as pa
/// >>>
/// >>> datasets = {
/// ...     "person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "age": [28, 34]}),
/// ...     "knows": pa.table({"src": [1], "dst": [2]}),
/// ... }
/// >>>
/// >>> engine = SqlEngine(datasets)
/// >>> result1 = engine.execute("SELECT * FROM person WHERE age > 30")
/// >>> result2 = engine.execute("SELECT p.name FROM person p JOIN knows k ON p.id = k.src")
#[pyclass(name = "SqlEngine", module = "lance_csr.graph")]
pub struct SqlEngine {
    context: Arc<SessionContext>,
}

#[pymethods]
impl SqlEngine {
    /// Create a new SqlEngine with cached datasets
    ///
    /// Parameters
    /// ----------
    /// datasets : dict
    ///     Dictionary mapping table names to PyArrow tables or Lance datasets.
    ///     Table names are lowercased for consistency.
    ///
    /// Returns
    /// -------
    /// SqlEngine
    ///     A new engine instance ready to execute queries
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no datasets are provided
    /// RuntimeError
    ///     If table registration fails
    #[new]
    fn new(datasets: &Bound<'_, PyDict>) -> PyResult<Self> {
        let arrow_datasets = python_datasets_to_batches(datasets)?;

        if arrow_datasets.is_empty() {
            return Err(PyValueError::new_err("No input datasets provided"));
        }

        let ctx = SessionContext::new();

        for (name, batch) in &arrow_datasets {
            let mem_table = Arc::new(
                MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "Failed to create MemTable for {}: {}",
                        name, e
                    ))
                })?,
            );

            let normalized_name = name.to_lowercase();
            ctx.register_table(&normalized_name, mem_table)
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to register table {}: {}", name, e))
                })?;
        }

        Ok(Self {
            context: Arc::new(ctx),
        })
    }

    /// Execute a SQL query using the cached datasets
    ///
    /// Parameters
    /// ----------
    /// sql : str
    ///     The SQL query string to execute
    ///
    /// Returns
    /// -------
    /// pyarrow.Table
    ///     Query results as Arrow table
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If query execution fails
    fn execute(&self, py: Python, sql: &str) -> PyResult<Py<PyAny>> {
        let query = RustSqlQuery::new(sql);
        let context = self.context.as_ref().clone();

        let result_batch = RT
            .block_on(Some(py), query.execute_with_context(context))?
            .map_err(graph_error_to_pyerr)?;

        record_batch_to_python_table(py, &result_batch)
    }

    fn __repr__(&self) -> String {
        "SqlEngine(...)".to_string()
    }
}

/// Register graph functionality with the Python module
pub fn register_graph_module(py: Python, parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    let graph_module = PyModule::new(py, "graph")?;

    graph_module.add_class::<ExecutionStrategy>()?;
    graph_module.add_class::<SqlDialect>()?;
    graph_module.add_class::<DistanceMetric>()?;
    graph_module.add_class::<GraphConfig>()?;
    graph_module.add_class::<GraphConfigBuilder>()?;
    graph_module.add_class::<CypherQuery>()?;
    graph_module.add_class::<CypherEngine>()?;
    graph_module.add_class::<SqlQuery>()?;
    graph_module.add_class::<SqlEngine>()?;
    graph_module.add_class::<VectorSearch>()?;
    graph_module.add_class::<PyDirNamespace>()?;

    parent_module.add_submodule(&graph_module)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state(id: i64) -> EngineState {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(arrow_array::Int64Array::from(vec![id])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let provider =
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap());
        let context = SessionContext::new();
        context.register_table("person", provider.clone()).unwrap();
        EngineState {
            sources: HashMap::from([("Person".to_string(), EngineSource::Arrow(batch))]),
            plans: Mutex::default(),
            catalog: Arc::new(
                InMemoryCatalog::new()
                    .with_node_source("Person", Arc::new(DefaultTableSource::new(provider))),
            ),
            context,
            csr: HashMap::new(),
            node_index: HashMap::new(),
            key_stats: HashMap::new(),
        }
    }

    #[test]
    fn test_plan_cache_reuses_bounds_and_refreshes() {
        let engine = CypherEngine {
            config: RustGraphConfig::builder()
                .node_table("Person", "id")
                .build()
                .unwrap(),
            state: RwLock::new(Arc::new(test_state(1))),
            use_csr: false,
        };
        let snapshot = engine.snapshot();
        let text = "MATCH (p:Person) WHERE p.id = $value RETURN p.id";
        for value in [1, 2, 3] {
            engine
                .plan_query(
                    text,
                    HashMap::from([("value".into(), value.into())]),
                    &snapshot,
                    true,
                )
                .unwrap();
            assert_eq!(snapshot.plans.lock().unwrap().len(), 1);
            // The retained template still has its first parameters: later calls
            // really used lookup and rebinding rather than replacing the entry.
            assert_eq!(
                snapshot.plans.lock().unwrap()[0].query.parameters()["value"],
                JsonValue::from(1)
            );
        }
        for value in [JsonValue::Null, JsonValue::from(1.5)] {
            engine
                .plan_query(
                    text,
                    HashMap::from([("value".into(), value)]),
                    &snapshot,
                    true,
                )
                .unwrap();
        }
        assert_eq!(snapshot.plans.lock().unwrap().len(), 3);
        engine
            .plan_query(
                "MATCH (p:Person) WHERE p.id IN $ids RETURN p.id",
                HashMap::from([("ids".into(), serde_json::json!([1, 2]))]),
                &snapshot,
                true,
            )
            .unwrap();
        assert_eq!(snapshot.plans.lock().unwrap().len(), 3);
        for value in 0..130 {
            engine
                .plan_query(
                    &format!("MATCH (p:Person) RETURN p.id AS id LIMIT {value}"),
                    HashMap::new(),
                    &snapshot,
                    true,
                )
                .unwrap();
        }
        assert_eq!(snapshot.plans.lock().unwrap().len(), 128);
        assert!(snapshot
            .plans
            .lock()
            .unwrap()
            .iter()
            .all(|entry| entry.text != text));
        assert!(engine.try_publish_refresh(&snapshot, test_state(2)));
        assert!(engine.snapshot().plans.lock().unwrap().is_empty());
        assert_eq!(snapshot.plans.lock().unwrap().len(), 128);
    }

    #[test]
    fn test_refresh_rejects_stale_snapshot() {
        use arrow_array::Int64Array;
        use std::sync::mpsc;
        use std::time::Duration;

        let engine = CypherEngine {
            config: RustGraphConfig::builder()
                .node_table("Person", "id")
                .build()
                .unwrap(),
            state: RwLock::new(Arc::new(test_state(1))),
            use_csr: false,
        };
        let reader_snapshot = engine.snapshot();
        let (started_tx, started_rx) = mpsc::channel();
        let (published_tx, published_rx) = mpsc::channel();
        std::thread::scope(|threads| {
            let engine = &engine;
            let older = threads.spawn(move || {
                let starting_snapshot = engine.snapshot();
                started_tx.send(()).unwrap();
                published_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                engine.try_publish_refresh(&starting_snapshot, test_state(2))
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(engine.try_publish_refresh(&engine.snapshot(), test_state(3)));
            published_tx.send(()).unwrap();
            assert!(!older.join().unwrap());
        });

        for (snapshot, expected) in [(reader_snapshot, 1), (engine.snapshot(), 3)] {
            let query = RustCypherQuery::new("MATCH (p:Person) RETURN p.id")
                .unwrap()
                .with_config(engine.config.clone());
            let result = RT
                .runtime
                .block_on(query.execute_with_catalog_and_context(
                    snapshot.catalog.clone(),
                    snapshot.context.clone(),
                ))
                .unwrap();
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }

    fn retained_stream() -> (
        datafusion::physical_plan::SendableRecordBatchStream,
        std::sync::Weak<arrow_array::Int64Array>,
    ) {
        let values = Arc::new(arrow_array::Int64Array::from(vec![1_i64]));
        let retained = Arc::downgrade(&values);
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok(batch)]),
        );
        (Box::pin(stream), retained)
    }

    #[test]
    fn timed_reader_releases_unconsumed_stream_at_deadline() {
        let (stream, retained) = retained_stream();
        let mut reader = reader_with_deadline(stream, QueryDeadline::new(Some(0.02)).unwrap());
        let stop = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while retained.upgrade().is_some() && std::time::Instant::now() < stop {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(
            retained.upgrade().is_none(),
            "idle deadline must drop the owned stream"
        );
        assert!(reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains(QUERY_TIMEOUT_MESSAGE));
        assert!(reader.next().is_none());
    }

    #[test]
    fn closing_timed_reader_releases_stream_before_deadline() {
        let (stream, retained) = retained_stream();
        let reader = reader_with_deadline(stream, QueryDeadline::new(Some(10.0)).unwrap());
        drop(reader);
        assert!(retained.upgrade().is_none());
    }

    #[test]
    fn test_split_vector_column_with_alias() {
        let (alias, column) = split_vector_column("d.embedding");
        assert_eq!(alias, Some("d".to_string()));
        assert_eq!(column, "embedding");
    }

    #[test]
    fn test_split_vector_column_without_alias() {
        let (alias, column) = split_vector_column("embedding");
        assert_eq!(alias, None);
        assert_eq!(column, "embedding");
    }

    #[test]
    fn test_split_vector_column_with_multiple_dots() {
        // Should only split on the first dot
        let (alias, column) = split_vector_column("d.nested.embedding");
        assert_eq!(alias, Some("d".to_string()));
        assert_eq!(column, "nested.embedding");
    }

    #[test]
    fn test_split_vector_column_empty_string() {
        let (alias, column) = split_vector_column("");
        assert_eq!(alias, None);
        assert_eq!(column, "");
    }

    #[test]
    fn test_alias_map_from_simple_node_query() {
        let query = RustCypherQuery::new("MATCH (d:Document) RETURN d.name").unwrap();
        let map = alias_map_from_query(&query);
        assert_eq!(map.get("d"), Some(&"Document".to_string()));
    }

    #[test]
    fn test_alias_map_from_multiple_nodes() {
        let query =
            RustCypherQuery::new("MATCH (p:Person), (d:Document) RETURN p.name, d.title").unwrap();
        let map = alias_map_from_query(&query);
        assert_eq!(map.get("p"), Some(&"Person".to_string()));
        assert_eq!(map.get("d"), Some(&"Document".to_string()));
    }

    #[test]
    fn test_alias_map_from_path_query() {
        let query =
            RustCypherQuery::new("MATCH (p:Person)-[:KNOWS]->(f:Friend) RETURN p.name, f.name")
                .unwrap();
        let map = alias_map_from_query(&query);
        assert_eq!(map.get("p"), Some(&"Person".to_string()));
        assert_eq!(map.get("f"), Some(&"Friend".to_string()));
    }

    #[test]
    fn test_resolve_vector_label_with_alias() {
        let query = RustCypherQuery::new("MATCH (d:Document) RETURN d.name").unwrap();
        let result = resolve_vector_label(&query, Some("d")).unwrap();
        assert_eq!(result, Some("Document".to_string()));
    }

    #[test]
    fn test_resolve_vector_label_without_alias_single_node() {
        let query = RustCypherQuery::new("MATCH (d:Document) RETURN d.name").unwrap();
        // When no alias is provided and there's only one node, should return that label
        let result = resolve_vector_label(&query, None).unwrap();
        assert_eq!(result, Some("Document".to_string()));
    }

    #[test]
    fn test_resolve_vector_label_without_alias_multiple_nodes() {
        let query =
            RustCypherQuery::new("MATCH (p:Person), (d:Document) RETURN p.name, d.title").unwrap();
        // When no alias is provided and there are multiple nodes, should return None
        let result = resolve_vector_label(&query, None).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_vector_label_unknown_alias() {
        let query = RustCypherQuery::new("MATCH (d:Document) RETURN d.name").unwrap();
        // When alias doesn't exist in the query, should return None
        let result = resolve_vector_label(&query, Some("x")).unwrap();
        assert_eq!(result, None);
    }
}
