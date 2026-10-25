// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! High-level Cypher query interface for Lance datasets

use crate::ast::CypherQuery as CypherAST;
use crate::ast::ReadingClause;
use crate::config::GraphConfig;
use crate::error::{GraphError, Result};
use crate::logical_plan::LogicalPlanner;
use crate::parser::parse_cypher_query;
use crate::spark_dialect::build_spark_dialect;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_sql::unparser::dialect::{
    CustomDialect, DefaultDialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};
use datafusion_sql::unparser::Unparser;
use lance_graph_catalog::DirNamespace;
use lance_namespace::models::DescribeTableRequest;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// SQL dialect to use when generating SQL from Cypher queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlDialect {
    /// Generic SQL (DataFusion default dialect)
    #[default]
    Default,
    /// Spark SQL dialect (backtick quoting, STRING type, EXTRACT, etc.)
    Spark,
    /// PostgreSQL dialect
    PostgreSql,
    /// MySQL dialect
    MySql,
    /// SQLite dialect
    Sqlite,
}

/// Wrapper to hold the concrete dialect type and provide an `Unparser` reference.
pub enum DialectUnparser {
    Default(DefaultDialect),
    Spark(Box<CustomDialect>),
    PostgreSql(PostgreSqlDialect),
    MySql(MySqlDialect),
    Sqlite(SqliteDialect),
}

impl DialectUnparser {
    pub fn as_unparser(&self) -> Unparser<'_> {
        match self {
            DialectUnparser::Default(d) => Unparser::new(d),
            DialectUnparser::Spark(d) => Unparser::new(d.as_ref()),
            DialectUnparser::PostgreSql(d) => Unparser::new(d),
            DialectUnparser::MySql(d) => Unparser::new(d),
            DialectUnparser::Sqlite(d) => Unparser::new(d),
        }
    }
}

impl SqlDialect {
    /// Create a `DialectUnparser` configured for this dialect.
    pub fn unparser(&self) -> DialectUnparser {
        match self {
            SqlDialect::Default => DialectUnparser::Default(DefaultDialect {}),
            SqlDialect::Spark => DialectUnparser::Spark(Box::new(build_spark_dialect())),
            SqlDialect::PostgreSql => DialectUnparser::PostgreSql(PostgreSqlDialect {}),
            SqlDialect::MySql => DialectUnparser::MySql(MySqlDialect {}),
            SqlDialect::Sqlite => DialectUnparser::Sqlite(SqliteDialect {}),
        }
    }
}

/// Normalize an Arrow schema to have lowercase field names.
///
/// This ensures that column names in the dataset match the normalized
/// qualified column names used internally (e.g., "fullName" becomes "fullname").
pub(crate) fn normalize_schema(schema: SchemaRef) -> Result<SchemaRef> {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            Arc::new(Field::new(
                f.name().to_lowercase(),
                f.data_type().clone(),
                f.is_nullable(),
            ))
        })
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

/// Normalize a RecordBatch to have lowercase column names.
///
/// This creates a new RecordBatch with a normalized schema while
/// preserving all the data arrays.
pub(crate) fn normalize_record_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let normalized_schema = normalize_schema(batch.schema())?;
    RecordBatch::try_new(normalized_schema, batch.columns().to_vec()).map_err(|e| {
        GraphError::PlanError {
            message: format!("Failed to normalize record batch schema: {}", e),
            location: snafu::Location::new(file!(), line!(), column!()),
        }
    })
}

/// Execution strategy for Cypher queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionStrategy {
    /// Use DataFusion query planner (default, full feature support)
    #[default]
    DataFusion,
    /// Use Lance native executor (not yet implemented)
    LanceNative,
}

/// A Cypher query that can be executed against Lance datasets
#[derive(Debug, Clone)]
pub struct CypherQuery {
    /// The original Cypher query string
    query_text: String,
    /// Parsed AST representation
    ast: CypherAST,
    /// Graph configuration for mapping
    config: Option<GraphConfig>,
    /// Query parameters
    parameters: HashMap<String, serde_json::Value>,
    /// Optional CSR adjacencies; when set, supported single-hop expansions use them
    adjacency: Option<crate::csr_expand::AdjacencyCatalog>,
    /// Optional mean fan-out per relationship type and direction; see [`FanOut`]
    fan_out: Option<crate::logical_plan::FanOut>,
    /// An unoptimized template belonging to the caller's catalog snapshot.
    cached_plan: Option<Arc<CachedPlan>>,
}
struct CachedPlan {
    graph: crate::logical_plan::LogicalOperator,
    datafusion: datafusion::logical_expr::LogicalPlan,
    catalog: Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
    parameter_types: HashMap<String, arrow_schema::DataType>,
}

impl std::fmt::Debug for CachedPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPlan")
            .field("parameter_types", &self.parameter_types)
            .finish_non_exhaustive()
    }
}

impl CypherQuery {
    /// Create a new Cypher query from a query string
    pub fn new(query: &str) -> Result<Self> {
        let ast = parse_cypher_query(query)?;

        Ok(Self {
            query_text: query.to_string(),
            ast,
            config: None,
            parameters: HashMap::new(),
            adjacency: None,
            fan_out: None,
            cached_plan: None,
        })
    }

    /// Set the graph configuration for this query
    pub fn with_config(mut self, config: GraphConfig) -> Self {
        self.cached_plan = None;
        self.config = Some(config);
        self
    }

    /// Plan supported single-hop expansions with CSR lookups instead of joins.
    ///
    /// The `SessionContext` used for execution must install
    /// [`crate::csr_expand::CsrQueryPlanner`]. Each adjacency must be built from the
    /// same relationship data the context serves, or results will be wrong.
    pub fn with_adjacency(mut self, adjacency: crate::csr_expand::AdjacencyCatalog) -> Self {
        self.cached_plan = None;
        self.adjacency = Some(adjacency);
        self
    }

    /// Mean fan-out per relationship type and direction, so a pattern that continues
    /// earlier rows can start from its cheaper bound end (see [`crate::logical_plan::FanOut`]).
    pub fn with_fan_out(mut self, fan_out: crate::logical_plan::FanOut) -> Self {
        self.cached_plan = None;
        self.fan_out = Some(fan_out);
        self
    }

    /// Add a parameter to the query
    pub fn with_parameter<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<serde_json::Value>,
    {
        self.parameters
            .insert(key.into().to_lowercase(), value.into());
        self
    }

    /// Add multiple parameters to the query
    pub fn with_parameters(mut self, params: HashMap<String, serde_json::Value>) -> Self {
        for (k, v) in params {
            self.parameters.insert(k.to_lowercase(), v);
        }
        self
    }

    /// Get the original query text
    pub fn query_text(&self) -> &str {
        &self.query_text
    }

    /// Get the parsed AST
    pub fn ast(&self) -> &CypherAST {
        &self.ast
    }

    /// Get the graph configuration
    pub fn config(&self) -> Option<&GraphConfig> {
        self.config.as_ref()
    }

    /// Get query parameters
    pub fn parameters(&self) -> &HashMap<String, serde_json::Value> {
        &self.parameters
    }

    /// Build a snapshot-local scalar-parameter template for the Python engine's cache.
    /// Binding must precede optimization so pushed CSR filters never retain old values.
    #[doc(hidden)]
    pub fn prepare_cached_plan(
        &mut self,
        catalog: Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
    ) -> Result<bool> {
        let parameter_counts = self
            .ast
            .limit
            .iter()
            .chain(self.ast.skip.iter())
            .chain(
                self.ast
                    .with_clause
                    .iter()
                    .filter_map(|clause| clause.limit.as_ref()),
            )
            .chain(
                self.ast
                    .further_with_stages
                    .iter()
                    .filter_map(|stage| stage.with_clause.limit.as_ref()),
            )
            .any(|count| matches!(count, crate::ast::RowCount::Parameter(_)));
        if parameter_counts || self.scalar_parameters().is_none() {
            return Ok(false);
        }
        let (graph, datafusion) = self.build_logical_plans(catalog.clone(), true)?;
        self.cached_plan = Some(Arc::new(CachedPlan {
            graph,
            datafusion,
            catalog,
            parameter_types: self
                .scalar_parameters()
                .unwrap()
                .into_iter()
                .map(|(name, value)| (name, value.data_type()))
                .collect(),
        }));
        Ok(true)
    }

    fn scalar_parameters(&self) -> Option<HashMap<String, datafusion::common::ScalarValue>> {
        use datafusion::common::ScalarValue;
        self.parameters
            .iter()
            .map(|(name, value)| {
                let value = match value {
                    serde_json::Value::Null => ScalarValue::Null,
                    serde_json::Value::Bool(value) => ScalarValue::Boolean(Some(*value)),
                    serde_json::Value::String(value) => ScalarValue::Utf8(Some(value.clone())),
                    serde_json::Value::Number(value) if value.is_i64() => {
                        ScalarValue::Int64(value.as_i64())
                    }
                    serde_json::Value::Number(value) if value.is_f64() => {
                        ScalarValue::Float64(value.as_f64())
                    }
                    _ => return None,
                };
                Some((name.clone(), value))
            })
            .collect()
    }

    /// Get the required config, returning an error if not set
    fn require_config(&self) -> Result<&GraphConfig> {
        self.config.as_ref().ok_or_else(|| GraphError::ConfigError {
            message: "Graph configuration is required for query execution".to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
    }

    /// Execute the query against provided in-memory datasets
    ///
    /// This method uses the DataFusion planner by default for comprehensive query support
    /// including joins, aggregations, and complex patterns. You can optionally specify
    /// a different execution strategy.
    ///
    /// # Arguments
    /// * `datasets` - HashMap of table name to RecordBatch (nodes and relationships)
    /// * `strategy` - Optional execution strategy (defaults to DataFusion)
    ///
    /// # Returns
    /// A single RecordBatch containing the query results
    ///
    /// # Errors
    /// Returns error if query parsing, planning, or execution fails
    ///
    /// # Example
    /// ```ignore
    /// use std::collections::HashMap;
    /// use arrow::record_batch::RecordBatch;
    /// use lance_graph::query::CypherQuery;
    ///
    /// // Create in-memory datasets
    /// let mut datasets = HashMap::new();
    /// datasets.insert("Person".to_string(), person_batch);
    /// datasets.insert("KNOWS".to_string(), knows_batch);
    ///
    /// // Parse and execute query
    /// let query = CypherQuery::parse("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name")?
    ///     .with_config(config);
    /// // Use the default DataFusion strategy
    /// let result = query.execute(datasets, None).await?;
    /// ```
    pub async fn execute(
        &self,
        datasets: HashMap<String, arrow::record_batch::RecordBatch>,
        strategy: Option<ExecutionStrategy>,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let strategy = strategy.unwrap_or_default();
        match strategy {
            ExecutionStrategy::DataFusion => self.execute_datafusion(datasets).await,
            ExecutionStrategy::LanceNative => Err(GraphError::UnsupportedFeature {
                feature: "Lance native execution strategy is not yet implemented".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            }),
        }
    }

    /// Execute the query using a namespace-backed table resolver.
    ///
    /// The namespace is provided by value and will be shared internally as needed.
    pub async fn execute_with_namespace(
        &self,
        namespace: DirNamespace,
        strategy: Option<ExecutionStrategy>,
    ) -> Result<arrow::record_batch::RecordBatch> {
        self.execute_with_namespace_arc(std::sync::Arc::new(namespace), strategy)
            .await
    }

    /// Execute the query using a shared namespace instance.
    pub async fn execute_with_namespace_arc(
        &self,
        namespace: std::sync::Arc<DirNamespace>,
        strategy: Option<ExecutionStrategy>,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let namespace_trait: std::sync::Arc<dyn lance_namespace::LanceNamespace + Send + Sync> =
            namespace;
        self.execute_with_namespace_internal(namespace_trait, strategy)
            .await
    }

    async fn execute_with_namespace_internal(
        &self,
        namespace: std::sync::Arc<dyn lance_namespace::LanceNamespace + Send + Sync>,
        strategy: Option<ExecutionStrategy>,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let strategy = strategy.unwrap_or_default();
        match strategy {
            ExecutionStrategy::DataFusion => {
                let (catalog, ctx) = self
                    .build_catalog_and_context_from_namespace(namespace)
                    .await?;
                self.execute_with_catalog_and_context(std::sync::Arc::new(catalog), ctx)
                    .await
            }
            ExecutionStrategy::LanceNative => Err(GraphError::UnsupportedFeature {
                feature: "Lance native execution strategy is not yet implemented".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            }),
        }
    }

    /// Explain the query execution plan using in-memory datasets
    ///
    /// Returns a formatted string showing the query execution plan at different stages:
    /// - Graph Logical Plan (graph-specific operators)
    /// - DataFusion Logical Plan (optimized relational plan)
    /// - DataFusion Physical Plan (execution plan with optimizations)
    ///
    /// This is useful for understanding query performance, debugging, and optimization.
    ///
    /// # Arguments
    /// * `datasets` - HashMap of table name to RecordBatch (nodes and relationships)
    ///
    /// # Returns
    /// A formatted string containing the execution plan at multiple levels
    ///
    /// # Errors
    /// Returns error if planning fails
    ///
    /// # Example
    /// ```ignore
    /// use std::collections::HashMap;
    /// use arrow::record_batch::RecordBatch;
    /// use lance_graph::query::CypherQuery;
    ///
    /// // Create in-memory datasets
    /// let mut datasets = HashMap::new();
    /// datasets.insert("Person".to_string(), person_batch);
    /// datasets.insert("KNOWS".to_string(), knows_batch);
    ///
    /// let query = CypherQuery::parse("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")?
    ///     .with_config(config);
    ///
    /// let plan = query.explain(datasets).await?;
    /// println!("{}", plan);
    /// ```
    pub async fn explain(
        &self,
        datasets: HashMap<String, arrow::record_batch::RecordBatch>,
    ) -> Result<String> {
        use std::sync::Arc;

        // Build catalog and context from datasets
        let (catalog, ctx) = self
            .build_catalog_and_context_from_datasets(datasets)
            .await?;

        // Delegate to the internal explain method
        self.explain_internal(Arc::new(catalog), ctx).await
    }

    /// Convert the Cypher query to a SQL string in the specified dialect.
    ///
    /// This method generates a SQL string that corresponds to the DataFusion logical plan
    /// derived from the Cypher query, using the specified SQL dialect for unparsing.
    ///
    /// **WARNING**: This method is experimental and the generated SQL dialect may change.
    ///
    /// **Note**: All table names and column names in the generated SQL are lowercased
    /// (e.g., `Person` becomes `person`, `fullName` becomes `fullname`), consistent with
    /// the system's case-insensitive identifier behavior.
    ///
    /// # Arguments
    /// * `datasets` - HashMap of table name to RecordBatch (nodes and relationships)
    /// * `dialect` - The SQL dialect to use for generating the output SQL.
    ///   Defaults to `SqlDialect::Default` (generic DataFusion SQL).
    ///   Use `SqlDialect::Spark` for Spark SQL, `SqlDialect::PostgreSql`, etc.
    ///
    /// # Returns
    /// A SQL string representing the query in the specified dialect
    ///
    /// # Errors
    /// Returns `GraphError::UnsupportedFeature` for runtime-only Cypher functions
    /// without a faithful SQL translation or typed nulls outside the Default dialect.
    /// Constant expressions may be folded first.
    pub async fn to_sql(
        &self,
        datasets: HashMap<String, arrow::record_batch::RecordBatch>,
        dialect: Option<SqlDialect>,
    ) -> Result<String> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::common::{not_impl_err, DFSchema, ScalarValue};
        use datafusion::logical_expr::expr_rewriter::NamePreserver;
        use datafusion::logical_expr::registry::FunctionRegistry;
        use datafusion::logical_expr::{lit, utils::merge_schema, Expr, LogicalPlan};
        use std::sync::Arc;

        let dialect = dialect.unwrap_or_default();
        let _config = self.require_config()?;

        // Build catalog and context from datasets using the helper
        let (catalog, ctx) = self
            .build_catalog_and_context_from_datasets(datasets)
            .await?;

        // Generate Logical Plan
        let (_, df_plan) = self.create_logical_plans(Arc::new(catalog))?;

        // Optimize the plan using DataFusion's default optimizer rules
        // This helps simplify the plan (e.g., merging projections) to produce cleaner SQL
        let optimized_plan = ctx
            .state()
            .optimize(&df_plan)
            .map_err(|e| GraphError::PlanError {
                message: format!("Failed to optimize plan: {}", e),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;

        // An SQL function with the same name need not implement Cypher semantics.
        let optimized_plan = optimized_plan
            .transform_up_with_subqueries(|plan| {
                let schema = if let LogicalPlan::TableScan(scan) = &plan {
                    DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())?
                } else {
                    merge_schema(&plan.inputs())
                };
                let names = NamePreserver::new(&plan);
                plan.map_expressions(|expr| {
                    let name = names.save(&expr);
                    let rewritten = expr.transform_up(|expr| {
                        if let Expr::ScalarFunction(function) = &expr {
                            if let Some(native) = crate::datafusion_planner::sql_expr(function, &schema)? {
                                return Ok(Transformed::yes(native));
                            }
                            if !ctx.udf(function.name()).is_ok_and(|registered| registered == function.func) {
                                return not_impl_err!(
                                    "SQL export of Cypher function '{}' is unsupported; execute the Cypher query directly",
                                    function.name()
                                );
                            }
                        }
                        if let Expr::Literal(value, _) = &expr {
                            let data_type = value.data_type();
                            if value.is_null() && !data_type.is_null() {
                                let type_name = data_type.to_string();
                                if dialect != SqlDialect::Default
                                    || !type_name.parse::<arrow_schema::DataType>().is_ok_and(|parsed| parsed == data_type)
                                {
                                    return not_impl_err!("SQL export of typed null {data_type} requires a faithful Default-dialect Arrow cast");
                                }
                                // The unparser emits bare NULL for typed nulls, losing their Arrow type.
                                return Ok(Transformed::yes(datafusion::functions::core::arrow_cast().call(vec![
                                    lit(ScalarValue::Null), lit(type_name),
                                ])));
                            }
                        }
                        Ok(Transformed::no(expr))
                    })?;
                    Ok(rewritten.update_data(|expr| name.restore(expr)))
                })
            })
            .map_err(|error| GraphError::UnsupportedFeature {
                feature: error.to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .data;

        // Unparse to SQL using the specified dialect
        let dialect_unparser = dialect.unparser();
        let unparser = dialect_unparser.as_unparser();
        let sql_ast = unparser
            .plan_to_sql(&optimized_plan)
            .map_err(|e| GraphError::PlanError {
                message: format!("Failed to unparse plan to SQL: {}", e),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;

        Ok(sql_ast.to_string())
    }

    /// Execute query with a DataFusion SessionContext, automatically building the catalog
    ///
    /// This is a convenience method that builds the graph catalog by querying the
    /// SessionContext for table schemas. The GraphConfig determines which tables to
    /// look up (node labels and relationship types).
    ///
    /// This method is ideal for integrating with DataFusion's rich data source ecosystem
    /// (CSV, Parquet, Delta Lake, Iceberg, etc.) without manually building a catalog.
    ///
    /// # Arguments
    /// * `ctx` - DataFusion SessionContext with pre-registered tables
    ///
    /// # Returns
    /// Query results as an Arrow RecordBatch
    ///
    /// # Errors
    /// Returns error if:
    /// - GraphConfig is not set (use `.with_config()` first)
    /// - Required tables are not registered in the SessionContext
    /// - Query execution fails
    ///
    /// # Example
    /// ```ignore
    /// use datafusion::execution::context::SessionContext;
    /// use datafusion::prelude::CsvReadOptions;
    /// use lance_graph::{CypherQuery, GraphConfig};
    ///
    /// // Step 1: Create GraphConfig
    /// let config = GraphConfig::builder()
    ///     .node_table("Person", "person_id")
    ///     .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
    ///     .build()?;
    ///
    /// // Step 2: Register data sources in DataFusion
    /// let ctx = SessionContext::new();
    /// ctx.register_csv("Person", "data/persons.csv", CsvReadOptions::default()).await?;
    /// ctx.register_parquet("KNOWS", "s3://bucket/knows.parquet", Default::default()).await?;
    ///
    /// // Step 3: Execute query (catalog is built automatically)
    /// let query = CypherQuery::parse("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name")?
    ///     .with_config(config);
    /// let result = query.execute_with_context(ctx).await?;
    /// ```
    ///
    /// # Note
    /// The catalog is built by querying the SessionContext for schemas of tables
    /// mentioned in the GraphConfig. Table names must match between GraphConfig
    /// (node labels/relationship types) and SessionContext (registered table names).
    pub async fn execute_with_context(
        &self,
        ctx: datafusion::execution::context::SessionContext,
    ) -> Result<arrow::record_batch::RecordBatch> {
        use datafusion::datasource::DefaultTableSource;
        use lance_graph_catalog::InMemoryCatalog;
        use std::sync::Arc;

        let config = self.require_config()?;

        // Build catalog by querying SessionContext for table providers
        let mut catalog = InMemoryCatalog::new();

        // Register node sources
        for (label, _) in config.iter_node_mappings() {
            let table_provider =
                ctx.table_provider(label)
                    .await
                    .map_err(|e| GraphError::ConfigError {
                        message: format!(
                            "Node label '{}' not found in SessionContext: {}",
                            label, e
                        ),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;

            let table_source = Arc::new(DefaultTableSource::new(table_provider));
            catalog = catalog.with_node_source(label, table_source);
        }

        // Register relationship sources
        for (rel_type, _) in config.iter_relationship_mappings() {
            let table_provider =
                ctx.table_provider(rel_type)
                    .await
                    .map_err(|e| GraphError::ConfigError {
                        message: format!(
                            "Relationship type '{}' not found in SessionContext: {}",
                            rel_type, e
                        ),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;

            let table_source = Arc::new(DefaultTableSource::new(table_provider));
            catalog = catalog.with_relationship_source(rel_type, table_source);
        }

        // Execute using the built catalog
        self.execute_with_catalog_and_context(Arc::new(catalog), ctx)
            .await
    }

    /// Execute query with an explicit catalog and session context
    ///
    /// This is the most flexible API for advanced users who want to provide their own
    /// catalog implementation or have fine-grained control over both the catalog and
    /// session context.
    ///
    /// # Arguments
    /// * `catalog` - Graph catalog containing node and relationship schemas for planning
    /// * `ctx` - DataFusion SessionContext with registered data sources for execution
    ///
    /// # Returns
    /// Query results as an Arrow RecordBatch
    ///
    /// # Errors
    /// Returns error if query parsing, planning, or execution fails
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    /// use datafusion::execution::context::SessionContext;
    /// use lance_graph::InMemoryCatalog;
    /// use lance_graph::query::CypherQuery;
    ///
    /// // Create custom catalog
    /// let catalog = InMemoryCatalog::new()
    ///     .with_node_source("Person", custom_table_source);
    ///
    /// // Create SessionContext
    /// let ctx = SessionContext::new();
    /// ctx.register_table("Person", custom_table).unwrap();
    ///
    /// // Execute with explicit catalog and context
    /// let query = CypherQuery::parse("MATCH (p:Person) RETURN p.name")?
    ///     .with_config(config);
    /// let result = query.execute_with_catalog_and_context(Arc::new(catalog), ctx).await?;
    /// ```
    pub async fn execute_with_catalog_and_context(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        ctx: datafusion::execution::context::SessionContext,
    ) -> Result<arrow::record_batch::RecordBatch> {
        use arrow::compute::concat_batches;

        let df = self
            .dataframe_with_catalog_and_context(catalog, ctx)
            .await?;

        // Capture the final schema after type coercion, exactly as DataFrame::collect
        // creates its physical plan. UNION batches can differ in nullability, while
        // the unoptimized logical schema can still have a narrower data type.
        let task_context = Arc::new(df.task_ctx());
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| GraphError::ExecutionError {
                message: format!("Failed to plan query results: {e}"),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        let result_schema = physical.schema();
        let batches = datafusion::physical_plan::collect(physical, task_context)
            .await
            .map_err(|e| GraphError::ExecutionError {
                message: format!("Failed to collect query results: {e}"),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;

        if batches.is_empty() {
            // Return empty batch with the schema from the DataFrame
            // This preserves column structure even when there are no rows
            return Ok(arrow::record_batch::RecordBatch::new_empty(result_schema));
        }

        // UNION branches may emit batches with narrower nullability. The complete
        // result schema must govern concatenation regardless of which branch finishes first.
        concat_batches(&result_schema, &batches).map_err(|e| GraphError::ExecutionError {
            message: format!("Failed to concatenate result batches: {}", e),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
    }

    /// Execute query with an explicit catalog and session context, returning a stream.
    ///
    /// Unlike [`Self::execute_with_catalog_and_context`], results are not collected:
    /// batches are produced as the consumer polls, so memory is bounded by the plan's
    /// operators (scans and filters stream; joins, sorts and aggregates may buffer).
    pub async fn execute_stream_with_catalog_and_context(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        ctx: datafusion::execution::context::SessionContext,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let df = self
            .dataframe_with_catalog_and_context(catalog, ctx)
            .await?;
        df.execute_stream()
            .await
            .map_err(|e| GraphError::ExecutionError {
                message: format!("Failed to start query stream: {}", e),
                location: snafu::Location::new(file!(), line!(), column!()),
            })
    }

    /// Explain the plan with an explicit catalog and session context.
    pub async fn explain_with_catalog_and_context(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        ctx: datafusion::execution::context::SessionContext,
    ) -> Result<String> {
        self.explain_internal(catalog, ctx).await
    }

    /// Plan the query (phases 1-3) and hand it to DataFusion (phase 4) without executing.
    async fn dataframe_with_catalog_and_context(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        ctx: datafusion::execution::context::SessionContext,
    ) -> Result<datafusion::dataframe::DataFrame> {
        let (_logical_plan, df_logical_plan) = self.create_logical_plans(catalog)?;
        ctx.execute_logical_plan(df_logical_plan)
            .await
            .map_err(|e| GraphError::ExecutionError {
                message: format!("Failed to execute DataFusion plan: {}", e),
                location: snafu::Location::new(file!(), line!(), column!()),
            })
    }

    /// Execute using the DataFusion planner with in-memory datasets
    ///
    /// # Overview
    /// This convenience method creates both a catalog and session context from the provided
    /// in-memory RecordBatches. It's ideal for testing and small datasets that fit in memory.
    ///
    /// For production use with external data sources (CSV, Parquet, databases), use
    /// `execute_with_context` instead, which automatically builds the catalog
    /// from the SessionContext.
    ///
    /// # Arguments
    /// * `datasets` - HashMap of table name to RecordBatch (nodes and relationships)
    ///
    /// # Returns
    /// A single RecordBatch containing the query results
    async fn execute_datafusion(
        &self,
        datasets: HashMap<String, arrow::record_batch::RecordBatch>,
    ) -> Result<arrow::record_batch::RecordBatch> {
        use std::sync::Arc;

        // Build catalog and context from datasets
        let (catalog, ctx) = self
            .build_catalog_and_context_from_datasets(datasets)
            .await?;

        // Delegate to common execution logic
        self.execute_with_catalog_and_context(Arc::new(catalog), ctx)
            .await
    }

    /// Helper to build catalog and context from in-memory datasets
    async fn build_catalog_and_context_from_datasets(
        &self,
        datasets: HashMap<String, arrow::record_batch::RecordBatch>,
    ) -> Result<(
        lance_graph_catalog::InMemoryCatalog,
        datafusion::execution::context::SessionContext,
    )> {
        use datafusion::datasource::{DefaultTableSource, MemTable};
        use datafusion::execution::context::SessionContext;
        use lance_graph_catalog::InMemoryCatalog;
        use std::sync::Arc;

        if datasets.is_empty() {
            return Err(GraphError::ConfigError {
                message: "No input datasets provided".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        // Create session context and catalog
        let ctx = SessionContext::new();
        let mut catalog = InMemoryCatalog::new();

        // Register all datasets as tables
        for (name, batch) in &datasets {
            // Normalize the schema to lowercase field names
            let normalized_batch = normalize_record_batch(batch)?;

            let mem_table = Arc::new(
                MemTable::try_new(
                    normalized_batch.schema(),
                    vec![vec![normalized_batch.clone()]],
                )
                .map_err(|e| GraphError::PlanError {
                    message: format!("Failed to create MemTable for {}: {}", name, e),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?,
            );

            // Normalize table name to lowercase
            let normalized_name = name.to_lowercase();

            // Register in session context for execution
            ctx.register_table(&normalized_name, mem_table.clone())
                .map_err(|e| GraphError::PlanError {
                    message: format!("Failed to register table {}: {}", name, e),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

            let table_source = Arc::new(DefaultTableSource::new(mem_table));

            // Register as both node and relationship source with original name
            // The config lookup is already case-insensitive, so we can use original name
            catalog = catalog
                .with_node_source(name, table_source.clone())
                .with_relationship_source(name, table_source);
        }

        Ok((catalog, ctx))
    }

    /// Helper to build catalog and context using a namespace resolver
    async fn build_catalog_and_context_from_namespace(
        &self,
        namespace: std::sync::Arc<dyn lance_namespace::LanceNamespace + Send + Sync>,
    ) -> Result<(
        lance_graph_catalog::InMemoryCatalog,
        datafusion::execution::context::SessionContext,
    )> {
        use datafusion::datasource::{DefaultTableSource, TableProvider};
        use datafusion::execution::context::SessionContext;
        use lance::datafusion::LanceTableProvider;
        use lance_graph_catalog::InMemoryCatalog;
        use std::sync::Arc;

        let config = self.require_config()?;

        let mut required_tables: HashSet<String> = HashSet::new();
        // Use original label/type names (not lowercase keys) for namespace resolution
        // The namespace needs the original casing to find files on disk
        required_tables.extend(
            config
                .iter_node_mappings()
                .map(|(_, mapping)| mapping.label.clone()),
        );
        required_tables.extend(
            config
                .iter_relationship_mappings()
                .map(|(_, mapping)| mapping.relationship_type.clone()),
        );

        if required_tables.is_empty() {
            return Err(GraphError::ConfigError {
                message:
                    "Graph configuration does not reference any node labels or relationship types"
                        .to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        let ctx = SessionContext::new();
        let mut catalog = InMemoryCatalog::new();
        let mut providers: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();

        for table_name in required_tables {
            let mut request = DescribeTableRequest::new();
            request.id = Some(vec![table_name.clone()]);

            let response =
                namespace
                    .describe_table(request)
                    .await
                    .map_err(|e| GraphError::ConfigError {
                        message: format!(
                            "Namespace failed to resolve table '{}': {}",
                            table_name, e
                        ),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;

            let location = response.location.ok_or_else(|| GraphError::ConfigError {
                message: format!(
                    "Namespace did not provide a location for table '{}'",
                    table_name
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;

            let dataset = lance::dataset::Dataset::open(&location)
                .await
                .map_err(|e| GraphError::ConfigError {
                    message: format!("Failed to open dataset for table '{}': {}", table_name, e),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

            let dataset = Arc::new(dataset);
            let provider: Arc<dyn TableProvider> =
                Arc::new(LanceTableProvider::new(dataset.clone(), true, true));

            // Register with lowercase table name for case-insensitive behavior
            let normalized_table_name = table_name.to_lowercase();
            ctx.register_table(&normalized_table_name, provider.clone())
                .map_err(|e| GraphError::PlanError {
                    message: format!(
                        "Failed to register table '{}' in SessionContext: {}",
                        table_name, e
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

            // Store provider with normalized (lowercase) key for consistent lookup
            providers.insert(normalized_table_name.clone(), provider);
        }

        for (label, _) in config.iter_node_mappings() {
            let provider = providers
                .get(label)
                .ok_or_else(|| GraphError::ConfigError {
                    message: format!(
                        "Namespace did not resolve dataset for node label '{}'",
                        label
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

            let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
            catalog = catalog.with_node_source(label, table_source);
        }

        for (rel_type, _) in config.iter_relationship_mappings() {
            let provider = providers
                .get(rel_type)
                .ok_or_else(|| GraphError::ConfigError {
                    message: format!(
                        "Namespace did not resolve dataset for relationship type '{}'",
                        rel_type
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

            let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
            catalog = catalog.with_relationship_source(rel_type, table_source);
        }

        Ok((catalog, ctx))
    }

    /// Internal helper to explain the query execution plan with explicit catalog and session context
    async fn explain_internal(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        ctx: datafusion::execution::context::SessionContext,
    ) -> Result<String> {
        // Create all plans (phases 1-4)
        let (logical_plan, df_logical_plan, physical_plan) =
            self.create_plans(catalog, &ctx).await?;

        // Format the explain output
        self.format_explain_output(&logical_plan, &df_logical_plan, physical_plan.as_ref())
    }

    /// Helper to create logical plans (graph logical, DataFusion logical)
    ///
    /// This performs phases 1-3 of query execution (semantic analysis, graph logical planning,
    /// DataFusion logical planning) without creating the physical plan.
    fn create_logical_plans(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
    ) -> Result<(
        crate::logical_plan::LogicalOperator,
        datafusion::logical_expr::LogicalPlan,
    )> {
        if let Some(template) = &self.cached_plan {
            if let Some(parameters) = self.scalar_parameters() {
                let same_types = parameters.len() == template.parameter_types.len()
                    && parameters.iter().all(|(name, value)| {
                        template.parameter_types.get(name) == Some(&value.data_type())
                    });
                if !Arc::ptr_eq(&catalog, &template.catalog) || !same_types {
                    return self.build_logical_plans(catalog, false);
                }
                let plan = template
                    .datafusion
                    .clone()
                    .with_param_values(parameters)
                    .map_err(|error| GraphError::PlanError {
                        message: format!("Failed to bind query parameters: {error}"),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;
                return Ok((template.graph.clone(), plan));
            }
        }
        self.build_logical_plans(catalog, false)
    }

    fn build_logical_plans(
        &self,
        catalog: Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        parameterized: bool,
    ) -> Result<(
        crate::logical_plan::LogicalOperator,
        datafusion::logical_expr::LogicalPlan,
    )> {
        use crate::datafusion_planner::{DataFusionPlanner, GraphPhysicalPlanner};
        use crate::semantic::SemanticAnalyzer;

        let config = self.require_config()?;

        // Phase 1: Semantic Analysis
        let mut analyzer = SemanticAnalyzer::new(config.clone()).with_catalog(catalog.clone());
        let semantic = analyzer.analyze(&self.ast, &self.parameters)?;
        if !semantic.errors.is_empty() {
            return Err(GraphError::PlanError {
                message: format!("Semantic analysis failed:\n{}", semantic.errors.join("\n")),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        // Phase 2: Graph Logical Plan
        let mut logical_planner = LogicalPlanner::new(config)
            .with_fan_out(self.fan_out.as_ref())
            .with_adjacency(self.adjacency.as_ref());
        let ast = if parameterized {
            &self.ast
        } else {
            &semantic.ast
        };
        let logical_plan = logical_planner.plan(ast)?;

        // Phase 3: DataFusion Logical Plan
        let mut df_planner = DataFusionPlanner::with_catalog(config.clone(), catalog);
        df_planner.adjacency = self.adjacency.clone();
        if parameterized {
            df_planner.parameter_types = self
                .scalar_parameters()
                .unwrap_or_default()
                .into_iter()
                .map(|(name, value)| (name, value.data_type()))
                .collect();
        }
        let df_logical_plan = df_planner.plan(&logical_plan)?;

        Ok((logical_plan, df_logical_plan))
    }

    /// Helper to create all plans (graph logical, DataFusion logical, physical)
    async fn create_plans(
        &self,
        catalog: std::sync::Arc<dyn lance_graph_catalog::GraphSourceCatalog>,
        ctx: &datafusion::execution::context::SessionContext,
    ) -> Result<(
        crate::logical_plan::LogicalOperator,
        datafusion::logical_expr::LogicalPlan,
        std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    )> {
        // Phases 1-3: Create logical plans
        let (logical_plan, df_logical_plan) = self.create_logical_plans(catalog)?;

        // Phase 4: DataFusion Physical Plan
        let df = ctx
            .execute_logical_plan(df_logical_plan.clone())
            .await
            .map_err(|e| GraphError::ExecutionError {
                message: format!("Failed to execute DataFusion plan: {}", e),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;

        let physical_plan =
            df.create_physical_plan()
                .await
                .map_err(|e| GraphError::ExecutionError {
                    message: format!("Failed to create physical plan: {}", e),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

        Ok((logical_plan, df_logical_plan, physical_plan))
    }

    /// Format explain output as a table
    fn format_explain_output(
        &self,
        logical_plan: &crate::logical_plan::LogicalOperator,
        df_logical_plan: &datafusion::logical_expr::LogicalPlan,
        physical_plan: &dyn datafusion::physical_plan::ExecutionPlan,
    ) -> Result<String> {
        // Format output with query first, then table
        let mut output = String::new();

        // Show Cypher query before the table
        output.push_str("Cypher Query:\n");
        output.push_str(&format!("  {}\n\n", self.query_text));

        // Build table rows (without the query)
        let mut rows = vec![];

        // Row 1: Graph Logical Plan
        let graph_plan_str = format!("{:#?}", logical_plan);
        rows.push(("graph_logical_plan", graph_plan_str));

        // Row 2: DataFusion Logical Plan
        let df_logical_str = format!("{}", df_logical_plan.display_indent());
        rows.push(("logical_plan", df_logical_str));

        // Row 3: DataFusion Physical Plan
        let df_physical_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan).indent(true)
        );
        rows.push(("physical_plan", df_physical_str));

        // Calculate column widths
        let plan_type_width = rows.iter().map(|(t, _)| t.len()).max().unwrap_or(10);
        let plan_width = rows
            .iter()
            .map(|(_, p)| p.lines().map(|l| l.len()).max().unwrap_or(0))
            .max()
            .unwrap_or(50);

        // Build table
        let separator = format!(
            "+{}+{}+",
            "-".repeat(plan_type_width + 2),
            "-".repeat(plan_width + 2)
        );

        output.push_str(&separator);
        output.push('\n');

        // Header
        output.push_str(&format!(
            "| {:<width$} | {:<plan_width$} |\n",
            "plan_type",
            "plan",
            width = plan_type_width,
            plan_width = plan_width
        ));
        output.push_str(&separator);
        output.push('\n');

        // Data rows
        for (plan_type, plan_content) in rows {
            let lines: Vec<&str> = plan_content.lines().collect();
            if lines.is_empty() {
                output.push_str(&format!(
                    "| {:<width$} | {:<plan_width$} |\n",
                    plan_type,
                    "",
                    width = plan_type_width,
                    plan_width = plan_width
                ));
            } else {
                // First line with plan_type
                output.push_str(&format!(
                    "| {:<width$} | {:<plan_width$} |\n",
                    plan_type,
                    lines[0],
                    width = plan_type_width,
                    plan_width = plan_width
                ));

                // Remaining lines with empty plan_type
                for line in &lines[1..] {
                    output.push_str(&format!(
                        "| {:<width$} | {:<plan_width$} |\n",
                        "",
                        line,
                        width = plan_type_width,
                        plan_width = plan_width
                    ));
                }
            }
        }

        output.push_str(&separator);
        output.push('\n');

        Ok(output)
    }

    /// Get all node labels referenced in this query
    pub fn referenced_node_labels(&self) -> Vec<String> {
        let mut labels = Vec::new();

        self.ast.for_each_query(&mut |ast| {
            for clause in &ast.reading_clauses {
                if let ReadingClause::Match(match_clause) = clause {
                    for pattern in &match_clause.patterns {
                        self.collect_node_labels_from_pattern(pattern, &mut labels);
                    }
                }
            }
        });
        labels.sort();
        labels.dedup();
        labels
    }

    /// Get all relationship types referenced in this query
    pub fn referenced_relationship_types(&self) -> Vec<String> {
        let mut types = Vec::new();

        self.ast.for_each_query(&mut |ast| {
            for clause in &ast.reading_clauses {
                if let ReadingClause::Match(match_clause) = clause {
                    for pattern in &match_clause.patterns {
                        self.collect_relationship_types_from_pattern(pattern, &mut types);
                    }
                }
            }
        });
        types.sort();
        types.dedup();
        types
    }

    /// Get all variables used in this query
    pub fn variables(&self) -> Vec<String> {
        let mut variables = Vec::new();

        self.ast.for_each_query(&mut |ast| {
            for clause in &ast.reading_clauses {
                match clause {
                    ReadingClause::Match(match_clause) => {
                        for pattern in &match_clause.patterns {
                            self.collect_variables_from_pattern(pattern, &mut variables);
                        }
                    }
                    ReadingClause::Unwind(unwind_clause) => {
                        variables.push(unwind_clause.alias.clone());
                    }
                }
            }
        });
        variables.sort();
        variables.dedup();
        variables
    }

    // Collection helper methods

    fn collect_node_labels_from_pattern(
        &self,
        pattern: &crate::ast::GraphPattern,
        labels: &mut Vec<String>,
    ) {
        match pattern {
            crate::ast::GraphPattern::Node(node) => {
                labels.extend(node.labels.clone());
            }
            crate::ast::GraphPattern::Path(path) => {
                labels.extend(path.start_node.labels.clone());
                for segment in &path.segments {
                    labels.extend(segment.end_node.labels.clone());
                }
            }
        }
    }

    fn collect_relationship_types_from_pattern(
        &self,
        pattern: &crate::ast::GraphPattern,
        types: &mut Vec<String>,
    ) {
        if let crate::ast::GraphPattern::Path(path) = pattern {
            for segment in &path.segments {
                types.extend(segment.relationship.types.clone());
            }
        }
    }

    fn collect_variables_from_pattern(
        &self,
        pattern: &crate::ast::GraphPattern,
        variables: &mut Vec<String>,
    ) {
        match pattern {
            crate::ast::GraphPattern::Node(node) => {
                if let Some(var) = &node.variable {
                    variables.push(var.clone());
                }
            }
            crate::ast::GraphPattern::Path(path) => {
                if let Some(var) = &path.start_node.variable {
                    variables.push(var.clone());
                }
                for segment in &path.segments {
                    if let Some(var) = &segment.relationship.variable {
                        variables.push(var.clone());
                    }
                    if let Some(var) = &segment.end_node.variable {
                        variables.push(var.clone());
                    }
                }
            }
        }
    }
}

impl CypherQuery {
    /// Execute Cypher query, then apply vector search reranking on results
    ///
    /// This is a convenience method for the common GraphRAG pattern:
    /// 1. Run Cypher query to get candidate entities via graph traversal
    /// 2. Rerank candidates by vector similarity
    ///
    /// # Arguments
    /// * `datasets` - HashMap of table name to RecordBatch (nodes and relationships)
    /// * `vector_search` - VectorSearch configuration for reranking
    ///
    /// # Returns
    /// A RecordBatch with the top-k results sorted by vector similarity
    ///
    /// # Example
    /// ```ignore
    /// use lance_graph::{CypherQuery, VectorSearch};
    /// use lance_graph::ast::DistanceMetric;
    ///
    /// let results = query
    ///     .execute_with_vector_rerank(
    ///         datasets,
    ///         VectorSearch::new("embedding")
    ///             .query_vector(query_vec)
    ///             .metric(DistanceMetric::Cosine)
    ///             .top_k(10)
    ///     )
    ///     .await?;
    /// ```
    pub async fn execute_with_vector_rerank(
        &self,
        datasets: HashMap<String, arrow::record_batch::RecordBatch>,
        vector_search: crate::lance_vector_search::VectorSearch,
    ) -> Result<arrow::record_batch::RecordBatch> {
        // Step 1: Execute Cypher query to get candidates
        let candidates = self.execute(datasets, None).await?;

        // Step 2: Apply vector search reranking
        vector_search.search(&candidates).await
    }
}

/// Builder for constructing Cypher queries programmatically
#[derive(Debug, Default)]
pub struct CypherQueryBuilder {
    match_clauses: Vec<crate::ast::MatchClause>,
    where_expression: Option<crate::ast::BooleanExpression>,
    return_items: Vec<crate::ast::ReturnItem>,
    order_by_items: Vec<crate::ast::OrderByItem>,
    limit: Option<u64>,
    distinct: bool,
    skip: Option<u64>,
    config: Option<GraphConfig>,
    parameters: HashMap<String, serde_json::Value>,
}

impl CypherQueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a MATCH clause for a node pattern
    pub fn match_node(mut self, variable: &str, label: &str) -> Self {
        let node = crate::ast::NodePattern {
            variable: Some(variable.to_string()),
            labels: vec![label.to_string()],
            properties: HashMap::new(),
        };

        let match_clause = crate::ast::MatchClause {
            patterns: vec![crate::ast::GraphPattern::Node(node)],
            optional: false,
            where_clause: None,
        };

        self.match_clauses.push(match_clause);
        self
    }

    /// Set the graph configuration
    pub fn with_config(mut self, config: GraphConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Add a RETURN item
    pub fn return_property(mut self, variable: &str, property: &str) -> Self {
        let prop_ref = crate::ast::PropertyRef::new(variable, property);
        let return_item = crate::ast::ReturnItem {
            expression: crate::ast::ValueExpression::Property(prop_ref),
            alias: None,
        };

        self.return_items.push(return_item);
        self
    }

    /// Set DISTINCT flag
    pub fn distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }

    /// Add a LIMIT clause
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Add a SKIP clause
    pub fn skip(mut self, skip: u64) -> Self {
        self.skip = Some(skip);
        self
    }

    /// Build the final CypherQuery
    pub fn build(self) -> Result<CypherQuery> {
        if self.match_clauses.is_empty() {
            return Err(GraphError::PlanError {
                message: "Query must have at least one MATCH clause".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        if self.return_items.is_empty() {
            return Err(GraphError::PlanError {
                message: "Query must have at least one RETURN item".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }

        let ast = crate::ast::CypherQuery {
            reading_clauses: self
                .match_clauses
                .into_iter()
                .map(crate::ast::ReadingClause::Match)
                .collect(),
            where_clause: self
                .where_expression
                .map(|expr| crate::ast::WhereClause { expression: expr }),
            with_clause: None, // WITH not supported via builder yet
            post_with_reading_clauses: vec![],
            post_with_where_clause: None,
            further_with_stages: vec![],
            union_branches: Vec::new(),
            return_clause: crate::ast::ReturnClause {
                distinct: self.distinct,
                items: self.return_items,
            },
            order_by: if self.order_by_items.is_empty() {
                None
            } else {
                Some(crate::ast::OrderByClause {
                    items: self.order_by_items,
                })
            },
            limit: self.limit.map(crate::ast::RowCount::Literal),
            skip: self.skip.map(crate::ast::RowCount::Literal),
        };

        // Generate query text from AST (simplified)
        let query_text = "MATCH ... RETURN ...".to_string(); // TODO: Implement AST->text conversion

        let query = CypherQuery {
            query_text,
            ast,
            config: self.config,
            parameters: self.parameters,
            adjacency: None,
            fan_out: None,
            cached_plan: None,
        };

        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GraphConfig;

    #[tokio::test]
    async fn union_distinct_all_and_schema_validation_execute() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 2]))],
        )
        .unwrap();
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        for (query, rows) in [
            ("MATCH (p:Person) RETURN p.id AS n UNION RETURN 2 AS n", 2),
            (
                "MATCH (p:Person) RETURN p.id AS n UNION ALL RETURN 2 AS n",
                4,
            ),
            (
                "RETURN 1 AS n UNION ALL RETURN 1 AS n UNION RETURN 2 AS n",
                2,
            ),
            ("RETURN NULL AS n UNION RETURN NULL AS n", 1),
        ] {
            let result = CypherQuery::new(query)
                .unwrap()
                .with_config(config.clone())
                .execute(HashMap::from([("Person".to_string(), batch.clone())]), None)
                .await
                .unwrap();
            assert_eq!(result.num_rows(), rows, "{query}");
        }
        for query in [
            "RETURN 1 AS x UNION RETURN 2 AS y",
            "RETURN 1 AS x UNION RETURN 'a' AS x",
        ] {
            assert!(CypherQuery::new(query)
                .unwrap()
                .with_config(config.clone())
                .execute(HashMap::from([("Person".to_string(), batch.clone())]), None)
                .await
                .is_err());
        }
    }

    #[test]
    fn test_parse_simple_cypher_query() {
        let query = CypherQuery::new("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(query.query_text(), "MATCH (n:Person) RETURN n.name");
        assert_eq!(query.referenced_node_labels(), vec!["Person"]);
        assert_eq!(query.variables(), vec!["n"]);
    }

    #[test]
    fn test_query_with_parameters() {
        let mut params = HashMap::new();
        params.insert("minAge".to_string(), serde_json::Value::Number(30.into()));
        params.insert("maxAge".to_string(), serde_json::Value::Number(50.into()));

        let query = CypherQuery::new(
            "MATCH (n:Person) WHERE n.age > $minAge AND n.age < $maxAge RETURN n.name",
        )
        .unwrap()
        .with_parameters(params);

        assert!(query.parameters().contains_key("minage"));
        assert!(query.parameters().contains_key("maxage"));
    }

    #[tokio::test]
    async fn cached_templates_rebind_scalars_and_preserve_schema() {
        use arrow_array::{ArrayRef, Int64Array, StringArray};
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        let datasets = HashMap::from([(
            "Person".into(),
            RecordBatch::try_from_iter([
                ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
                (
                    "name",
                    Arc::new(StringArray::from(vec!["Ada", "Bob", "Cam"])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )]);
        let cases = [
            (
                "MATCH (p:Person) WHERE p.id >= $value RETURN p.id ORDER BY p.id",
                vec![serde_json::json!(1), serde_json::json!(3)],
            ),
            (
                "MATCH (p:Person) RETURN p.id + $value AS adjusted ORDER BY adjusted",
                vec![serde_json::json!(1.5), serde_json::json!(8.5)],
            ),
            (
                "MATCH (p:Person) WHERE p.name = $value RETURN p.id",
                vec![serde_json::json!("Ada"), serde_json::json!("Cam")],
            ),
            (
                "MATCH (p:Person) RETURN $value AS value",
                vec![serde_json::json!(true), serde_json::json!(false)],
            ),
            (
                "MATCH (p:Person) RETURN $value AS value",
                vec![serde_json::Value::Null],
            ),
            (
                "MATCH (p:Person) RETURN $value",
                vec![serde_json::json!(1), serde_json::json!(2)],
            ),
            (
                "MATCH (p:Person) RETURN p.id + $value",
                vec![serde_json::json!(1), serde_json::json!(2)],
            ),
            (
                "MATCH (p:Person) RETURN p.id AS id, [x IN range(1, $value) | x + p.id] AS values ORDER BY id",
                vec![serde_json::json!(1), serde_json::json!(3)],
            ),
        ];
        for (text, values) in cases {
            let mut template = CypherQuery::new(text)
                .unwrap()
                .with_config(config.clone())
                .with_parameter("value", values[0].clone());
            let (catalog, context) = template
                .build_catalog_and_context_from_datasets(datasets.clone())
                .await
                .unwrap();
            let catalog: Arc<dyn lance_graph_catalog::GraphSourceCatalog> = Arc::new(catalog);
            assert!(
                template.prepare_cached_plan(catalog.clone()).unwrap(),
                "{text}"
            );
            for value in values {
                let cached = template.clone().with_parameter("value", value.clone());
                let fresh = CypherQuery::new(text)
                    .unwrap()
                    .with_config(config.clone())
                    .with_parameter("value", value);
                let actual = cached
                    .execute_with_catalog_and_context(catalog.clone(), context.clone())
                    .await
                    .unwrap();
                let expected = fresh
                    .execute_with_catalog_and_context(catalog.clone(), context.clone())
                    .await
                    .unwrap();
                assert_eq!(actual, expected, "{text}");
            }
        }
    }

    #[tokio::test]
    async fn cached_templates_respect_catalog_and_parameter_types() {
        use arrow_array::{ArrayRef, Int64Array};
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        let data = |id| {
            HashMap::from([(
                "Person".into(),
                RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(Int64Array::from(vec![id])) as ArrayRef,
                )])
                .unwrap(),
            )])
        };
        let mut query = CypherQuery::new("MATCH (p:Person) WHERE p.id = $value RETURN p.id")
            .unwrap()
            .with_config(config.clone())
            .with_parameter("value", 1);
        let (catalog, _) = query
            .build_catalog_and_context_from_datasets(data(1))
            .await
            .unwrap();
        assert!(query.prepare_cached_plan(Arc::new(catalog)).unwrap());
        // A different catalog must never serve rows from the prepared snapshot.
        assert_eq!(query.execute(data(2), None).await.unwrap().num_rows(), 0);
        assert!(query
            .clone()
            .with_parameter("value", "wrong")
            .execute(data(1), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("Type mismatch"));
        assert!(query.with_config(config).cached_plan.is_none());
        let mut containers = CypherQuery::new("MATCH (p:Person) WHERE p.id IN $ids RETURN p.id")
            .unwrap()
            .with_parameter("ids", serde_json::json!([1, 2]));
        assert!(!containers
            .prepare_cached_plan(Arc::new(crate::InMemoryCatalog::new()))
            .unwrap());
    }

    #[test]
    fn parameterized_row_counts_bypass_templates() {
        for text in [
            "MATCH (p:Person) RETURN p.id LIMIT $count",
            "MATCH (p:Person) RETURN p.id SKIP $count",
            "MATCH (p:Person) WITH p LIMIT $count RETURN p.id",
            "MATCH (p:Person) WITH p WITH p LIMIT $count RETURN p.id",
        ] {
            let mut query = CypherQuery::new(text).unwrap().with_parameter("count", 1);
            assert!(
                !query
                    .prepare_cached_plan(Arc::new(crate::InMemoryCatalog::new()))
                    .unwrap(),
                "{text}"
            );
            assert!(query.cached_plan.is_none());
        }
    }

    // Opt-in diagnostic: reports stage means for a fixed local scan, with no timing
    // assertions. Physical planning calls the query planner directly, avoiding the
    // extra optimize() performed by SessionState::create_physical_plan().
    #[tokio::test]
    #[ignore = "local stage timing; run with --ignored --nocapture"]
    async fn profile_query_planning_stages() {
        use crate::datafusion_planner::{DataFusionPlanner, GraphPhysicalPlanner};
        use crate::semantic::SemanticAnalyzer;
        use arrow_array::{ArrayRef, Int64Array};
        use std::time::Instant;
        let text = "MATCH (p:Person) WHERE p.id >= $minimum RETURN p.id AS id ORDER BY id";
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        let parameters = HashMap::from([("minimum".into(), serde_json::json!(10))]);
        let mut template = CypherQuery::new(text)
            .unwrap()
            .with_config(config.clone())
            .with_parameters(parameters.clone());
        let data = HashMap::from([(
            "Person".into(),
            RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from_iter_values(0..1000)) as ArrayRef,
            )])
            .unwrap(),
        )]);
        let (catalog, context) = template
            .build_catalog_and_context_from_datasets(data)
            .await
            .unwrap();
        let catalog: Arc<dyn lance_graph_catalog::GraphSourceCatalog> = Arc::new(catalog);
        template.prepare_cached_plan(catalog.clone()).unwrap();
        let state = context.state();
        let mut totals = [std::time::Duration::ZERO; 8];
        for _ in 0..100 {
            let start = Instant::now();
            let ast = parse_cypher_query(text).unwrap();
            totals[0] += start.elapsed();
            let start = Instant::now();
            let semantic = SemanticAnalyzer::new(config.clone())
                .with_catalog(catalog.clone())
                .analyze(&ast, &parameters)
                .unwrap();
            assert!(semantic.errors.is_empty());
            totals[1] += start.elapsed();
            let start = Instant::now();
            let graph = LogicalPlanner::new(&config).plan(&semantic.ast).unwrap();
            totals[2] += start.elapsed();
            let start = Instant::now();
            let logical = DataFusionPlanner::with_catalog(config.clone(), catalog.clone())
                .plan(&graph)
                .unwrap();
            totals[3] += start.elapsed();
            let start = Instant::now();
            let optimized = state.optimize(&logical).unwrap();
            totals[4] += start.elapsed();
            let start = Instant::now();
            let physical = state
                .query_planner()
                .create_physical_plan(&optimized, &state)
                .await
                .unwrap();
            totals[5] += start.elapsed();
            let start = Instant::now();
            let batches = datafusion::physical_plan::collect(physical, context.task_ctx())
                .await
                .unwrap();
            totals[6] += start.elapsed();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                990
            );
            let checksum: i64 = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                })
                .sum();
            assert_eq!(checksum, (10 + 999) * 990 / 2);
            let start = Instant::now();
            let _ = template.create_logical_plans(catalog.clone()).unwrap();
            totals[7] += start.elapsed();
        }
        for (name, duration) in [
            "parse",
            "semantic",
            "graph_logical",
            "datafusion_logical",
            "optimize",
            "physical",
            "execute",
            "cached_bind",
        ]
        .into_iter()
        .zip(totals)
        {
            println!(
                "{name}: {:.3} us mean (100 iterations, 1000 input rows, 990 result rows)",
                duration.as_secs_f64() * 1_000_000.0 / 100.0
            );
        }
    }

    #[test]
    fn test_query_builder() {
        let config = GraphConfig::builder()
            .node_table("Person", "person_id")
            .build()
            .unwrap();

        let query = CypherQueryBuilder::new()
            .with_config(config)
            .match_node("n", "Person")
            .return_property("n", "name")
            .limit(10)
            .build()
            .unwrap();

        assert_eq!(query.referenced_node_labels(), vec!["Person"]);
        assert_eq!(query.variables(), vec!["n"]);
    }

    #[test]
    fn test_relationship_query_parsing() {
        let query =
            CypherQuery::new("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name")
                .unwrap();
        assert_eq!(query.referenced_node_labels(), vec!["Person"]);
        assert_eq!(query.referenced_relationship_types(), vec!["KNOWS"]);
        assert_eq!(query.variables(), vec!["a", "b", "r"]);
    }

    #[tokio::test]
    async fn test_execute_basic_projection_and_filter() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // Build a simple batch: name (Utf8), age (Int64)
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David"])),
                Arc::new(Int64Array::from(vec![28, 34, 29, 42])),
            ],
        )
        .unwrap();

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        let q = CypherQuery::new("MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age")
            .unwrap()
            .with_config(cfg);

        let mut data = HashMap::new();
        data.insert("Person".to_string(), batch);

        let out = q.execute(data, None).await.unwrap();
        assert_eq!(out.num_rows(), 2);
        let names = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        // Expect Bob (34) and David (42)
        let result: Vec<(String, i64)> = (0..out.num_rows())
            .map(|i| (names.value(i).to_string(), ages.value(i)))
            .collect();
        assert!(result.contains(&("Bob".to_string(), 34)));
        assert!(result.contains(&("David".to_string(), 42)));
    }

    #[tokio::test]
    async fn test_execute_single_hop_path_join_projection() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // People table: id, name, age
        let person_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let people = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
                Arc::new(Int64Array::from(vec![28, 34, 29])),
            ],
        )
        .unwrap();

        // KNOWS relationship: src_person_id -> dst_person_id
        let rel_schema = Arc::new(Schema::new(vec![
            Field::new("src_person_id", DataType::Int64, false),
            Field::new("dst_person_id", DataType::Int64, false),
        ]));
        let knows = RecordBatch::try_new(
            rel_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])), // Alice -> Bob, Bob -> Carol
                Arc::new(Int64Array::from(vec![2, 3])),
            ],
        )
        .unwrap();

        // Config: Person(id) and KNOWS(src_person_id -> dst_person_id)
        let cfg = GraphConfig::builder()
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

        // Query: MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name
        let q = CypherQuery::new("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
            .unwrap()
            .with_config(cfg);

        let mut data = HashMap::new();
        // Register tables using labels / rel types as names
        data.insert("Person".to_string(), people);
        data.insert("KNOWS".to_string(), knows);

        let out = q.execute(data, None).await.unwrap();
        // Expect two rows: Bob, Carol (the targets)
        let names = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<String> = (0..out.num_rows())
            .map(|i| names.value(i).to_string())
            .collect();
        assert_eq!(got.len(), 2);
        assert!(got.contains(&"Bob".to_string()));
        assert!(got.contains(&"Carol".to_string()));
    }

    #[tokio::test]
    async fn test_execute_order_by_asc() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // name, age (int)
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Bob", "Alice", "David", "Carol"])),
                Arc::new(Int64Array::from(vec![34, 28, 42, 29])),
            ],
        )
        .unwrap();

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        // Order ascending by age
        let q = CypherQuery::new("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age ASC")
            .unwrap()
            .with_config(cfg);

        let mut data = HashMap::new();
        data.insert("Person".to_string(), batch);

        let out = q.execute(data, None).await.unwrap();
        let ages = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let collected: Vec<i64> = (0..out.num_rows()).map(|i| ages.value(i)).collect();
        assert_eq!(collected, vec![28, 29, 34, 42]);
    }

    #[tokio::test]
    async fn test_execute_order_by_desc_with_skip_limit() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Bob", "Alice", "David", "Carol"])),
                Arc::new(Int64Array::from(vec![34, 28, 42, 29])),
            ],
        )
        .unwrap();

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        // Desc by age, skip 1 (drop 42), take 2 -> [34, 29]
        let q =
            CypherQuery::new("MATCH (p:Person) RETURN p.age ORDER BY p.age DESC SKIP 1 LIMIT 2")
                .unwrap()
                .with_config(cfg);

        let mut data = HashMap::new();
        data.insert("Person".to_string(), batch);

        let out = q.execute(data, None).await.unwrap();
        assert_eq!(out.num_rows(), 2);
        let ages = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let collected: Vec<i64> = (0..out.num_rows()).map(|i| ages.value(i)).collect();
        assert_eq!(collected, vec![34, 29]);
    }

    #[tokio::test]
    async fn test_execute_skip_without_limit() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40]))],
        )
        .unwrap();

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        let q = CypherQuery::new("MATCH (p:Person) RETURN p.age ORDER BY p.age ASC SKIP 2")
            .unwrap()
            .with_config(cfg);

        let mut data = HashMap::new();
        data.insert("Person".to_string(), batch);

        let out = q.execute(data, None).await.unwrap();
        assert_eq!(out.num_rows(), 2);
        let ages = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let collected: Vec<i64> = (0..out.num_rows()).map(|i| ages.value(i)).collect();
        assert_eq!(collected, vec![30, 40]);
    }

    #[tokio::test]
    async fn test_execute_datafusion_simple_scan() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        // Test simple scan without filters
        let query = CypherQuery::new("MATCH (p:Person) RETURN p.name")
            .unwrap()
            .with_config(cfg);

        let mut datasets = HashMap::new();
        datasets.insert("Person".to_string(), batch);

        // Execute using DataFusion pipeline
        let result = query.execute_datafusion(datasets).await.unwrap();

        // Should return all rows
        assert_eq!(
            result.num_rows(),
            2,
            "Should return all 2 rows without filtering"
        );
        assert_eq!(result.num_columns(), 1, "Should return 1 column (name)");

        // Verify data
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_set: std::collections::HashSet<String> = (0..result.num_rows())
            .map(|i| names.value(i).to_string())
            .collect();
        let expected: std::collections::HashSet<String> =
            ["Alice", "Bob"].iter().map(|s| s.to_string()).collect();
        assert_eq!(name_set, expected, "Should return Alice and Bob");
    }

    #[tokio::test]
    async fn test_execute_with_context_simple_scan() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;
        use std::sync::Arc;

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
                Arc::new(Int64Array::from(vec![28, 34, 29])),
            ],
        )
        .unwrap();

        // Create SessionContext and register data source
        let mem_table =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap());
        let ctx = SessionContext::new();
        ctx.register_table("Person", mem_table).unwrap();

        // Create query
        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        let query = CypherQuery::new("MATCH (p:Person) RETURN p.name")
            .unwrap()
            .with_config(cfg);

        // Execute with context (catalog built automatically)
        let result = query.execute_with_context(ctx).await.unwrap();

        // Verify results
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 1);

        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Carol");
    }

    #[tokio::test]
    async fn test_execute_with_context_with_filter() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;
        use std::sync::Arc;

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David"])),
                Arc::new(Int64Array::from(vec![28, 34, 29, 42])),
            ],
        )
        .unwrap();

        // Create SessionContext
        let mem_table =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap());
        let ctx = SessionContext::new();
        ctx.register_table("Person", mem_table).unwrap();

        // Create query with filter
        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        let query = CypherQuery::new("MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age")
            .unwrap()
            .with_config(cfg);

        // Execute with context
        let result = query.execute_with_context(ctx).await.unwrap();

        // Verify: should return Bob (34) and David (42)
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let results: Vec<(String, i64)> = (0..result.num_rows())
            .map(|i| (names.value(i).to_string(), ages.value(i)))
            .collect();

        assert!(results.contains(&("Bob".to_string(), 34)));
        assert!(results.contains(&("David".to_string(), 42)));
    }

    #[tokio::test]
    async fn test_execute_with_context_relationship_traversal() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;
        use std::sync::Arc;

        // Create Person nodes
        let person_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let person_batch = RecordBatch::try_new(
            person_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            ],
        )
        .unwrap();

        // Create KNOWS relationships
        let knows_schema = Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
            Field::new("since", DataType::Int64, false),
        ]));
        let knows_batch = RecordBatch::try_new(
            knows_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(Int64Array::from(vec![2020, 2021])),
            ],
        )
        .unwrap();

        // Create SessionContext and register tables
        let person_table = Arc::new(
            MemTable::try_new(person_schema.clone(), vec![vec![person_batch.clone()]]).unwrap(),
        );
        let knows_table = Arc::new(
            MemTable::try_new(knows_schema.clone(), vec![vec![knows_batch.clone()]]).unwrap(),
        );

        let ctx = SessionContext::new();
        ctx.register_table("Person", person_table).unwrap();
        ctx.register_table("KNOWS", knows_table).unwrap();

        // Create query
        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
            .build()
            .unwrap();

        let query = CypherQuery::new("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
            .unwrap()
            .with_config(cfg);

        // Execute with context
        let result = query.execute_with_context(ctx).await.unwrap();

        // Verify: should return 2 relationships (Alice->Bob, Bob->Carol)
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let src_names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dst_names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let relationships: Vec<(String, String)> = (0..result.num_rows())
            .map(|i| {
                (
                    src_names.value(i).to_string(),
                    dst_names.value(i).to_string(),
                )
            })
            .collect();

        assert!(relationships.contains(&("Alice".to_string(), "Bob".to_string())));
        assert!(relationships.contains(&("Bob".to_string(), "Carol".to_string())));
    }

    #[tokio::test]
    async fn test_execute_with_context_order_by_limit() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionContext;
        use std::sync::Arc;

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David"])),
                Arc::new(Int64Array::from(vec![85, 92, 78, 95])),
            ],
        )
        .unwrap();

        // Create SessionContext
        let mem_table =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap());
        let ctx = SessionContext::new();
        ctx.register_table("Student", mem_table).unwrap();

        // Create query with ORDER BY and LIMIT
        let cfg = GraphConfig::builder()
            .node_table("Student", "id")
            .build()
            .unwrap();

        let query = CypherQuery::new(
            "MATCH (s:Student) RETURN s.name, s.score ORDER BY s.score DESC LIMIT 2",
        )
        .unwrap()
        .with_config(cfg);

        // Execute with context
        let result = query.execute_with_context(ctx).await.unwrap();

        // Verify: should return top 2 scores (David: 95, Bob: 92)
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // First row should be David (95)
        assert_eq!(names.value(0), "David");
        assert_eq!(scores.value(0), 95);

        // Second row should be Bob (92)
        assert_eq!(names.value(1), "Bob");
        assert_eq!(scores.value(1), 92);
    }

    #[tokio::test]
    async fn test_to_sql() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        use std::collections::HashMap;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());

        let mut datasets = HashMap::new();
        datasets.insert("Person".to_string(), batch);

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        let query = CypherQuery::new("MATCH (p:Person) RETURN p.name")
            .unwrap()
            .with_config(cfg);

        let sql = query.to_sql(datasets, None).await.unwrap();
        println!("Generated SQL: {}", sql);

        assert!(sql.contains("SELECT"));
        assert!(sql.to_lowercase().contains("from person"));
        // Note: DataFusion unparser might quote identifiers or use aliases
        // We check for "p.name" which is the expected output alias
        assert!(sql.contains("p.name"));
    }

    #[tokio::test]
    async fn test_to_sql_rejects_runtime_only_cypher_function() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(arrow_array::Int64Array::from(vec![1])) as arrow_array::ArrayRef,
            ),
            (
                "number",
                Arc::new(arrow_array::Float64Array::from(vec![-1.5])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let query = CypherQuery::new("MATCH (p:Person) RETURN round(p.number) AS value")
            .unwrap()
            .with_config(
                GraphConfig::builder()
                    .node_table("Person", "id")
                    .build()
                    .unwrap(),
            );
        let error = query
            .to_sql(HashMap::from([("Person".to_string(), batch)]), None)
            .await
            .unwrap_err();
        assert!(matches!(error, GraphError::UnsupportedFeature { .. }));
        assert!(error
            .to_string()
            .contains("SQL export of Cypher function 'round'"));
    }

    #[tokio::test]
    async fn test_to_sql_preserves_constant_null_types() {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(arrow_array::Int64Array::from(vec![1])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let datasets = HashMap::from([("Person".to_string(), batch)]);
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        for expression in [
            "round(null)",
            "abs(null)",
            "sqrt(null)",
            "floor(null)",
            "ceil(null)",
            "toUpper(null)",
            "null",
        ] {
            let query = CypherQuery::new(&format!("MATCH (p:Person) RETURN {expression} AS value"))
                .unwrap()
                .with_config(config.clone());
            let cypher = query.execute(datasets.clone(), None).await.unwrap();
            let sql_text = query.to_sql(datasets.clone(), None).await.unwrap();
            let sql = crate::sql_query::SqlQuery::new(&sql_text)
                .execute(datasets.clone())
                .await
                .unwrap();
            assert_eq!(
                sql.schema().field(0).data_type(),
                cypher.schema().field(0).data_type(),
                "{expression}: {sql_text}"
            );
            assert_eq!(sql.column(0).logical_null_count(), 1, "{expression}");
        }
    }

    async fn write_lance_dataset(path: &std::path::Path, batch: arrow_array::RecordBatch) {
        use arrow_array::{RecordBatch, RecordBatchIterator};
        use lance::dataset::{Dataset, WriteParams};

        let schema = batch.schema();
        let batches: Vec<std::result::Result<RecordBatch, arrow::error::ArrowError>> =
            vec![std::result::Result::Ok(batch)];
        let reader = RecordBatchIterator::new(batches.into_iter(), schema);

        Dataset::write(reader, path.to_str().unwrap(), None::<WriteParams>)
            .await
            .expect("write lance dataset");
    }

    fn build_people_batch() -> arrow_array::RecordBatch {
        use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("person_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![28, 34, 29, 42])) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns).expect("valid person batch")
    }

    fn build_friendship_batch() -> arrow_array::RecordBatch {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("person1_id", DataType::Int64, false),
            Field::new("person2_id", DataType::Int64, false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2, 3, 4, 4])) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns).expect("valid friendship batch")
    }

    #[tokio::test]
    async fn executes_against_directory_namespace() {
        use arrow_array::StringArray;
        use tempfile::tempdir;

        let tmp_dir = tempdir().unwrap();
        write_lance_dataset(&tmp_dir.path().join("Person.lance"), build_people_batch()).await;
        write_lance_dataset(
            &tmp_dir.path().join("FRIEND_OF.lance"),
            build_friendship_batch(),
        )
        .await;

        let config = GraphConfig::builder()
            .node_table("Person", "person_id")
            .rel_table("FRIEND_OF", "Person", "Person", "person1_id", "person2_id")
            .build()
            .expect("valid graph config");

        let query = CypherQuery::new("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
            .expect("query parses")
            .with_config(config);

        let namespace = DirNamespace::new(tmp_dir.path().to_string_lossy().into_owned());

        let result = query
            .execute_with_namespace(namespace.clone(), None)
            .await
            .expect("namespace execution succeeds");

        use arrow_array::Array;
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");

        let mut values: Vec<String> = (0..names.len())
            .map(|i| names.value(i).to_string())
            .collect();
        values.sort();
        assert_eq!(values, vec!["Bob".to_string(), "David".to_string()]);
    }

    #[tokio::test]
    async fn test_execute_fails_on_semantic_error() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        use std::collections::HashMap;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);
        let mut datasets = HashMap::new();
        datasets.insert("Person".to_string(), batch);

        let cfg = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();

        // Query referencing undefined variable 'x'
        let query = CypherQuery::new("MATCH (n:Person) RETURN x.name")
            .unwrap()
            .with_config(cfg);

        let result = query.execute(datasets, None).await;

        assert!(result.is_err());
        match result {
            Err(GraphError::PlanError { message, .. }) => {
                assert!(message.contains("Semantic analysis failed"));
                assert!(message.contains("Undefined variable: 'x'"));
            }
            _ => panic!(
                "Expected PlanError with semantic failure message, got {:?}",
                result
            ),
        }
    }
}
