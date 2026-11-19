//! One-shot queries: `CypherQuery` and `SqlQuery` read their tables for each call.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use lance_csr::{
    ast::{GraphPattern, ReadingClause},
    CypherQuery as RustCypherQuery, DirNamespace, ExecutionStrategy as RustExecutionStrategy,
    SqlDialect as RustSqlDialect, SqlQuery as RustSqlQuery,
};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

use crate::convert::{
    batch_to_ipc, batches, datasets, graph_config, graph_error, invalid, ipc_to_batch, params,
    vector_search, Dataset, DatasetInput, Deadline, GraphConfigSpec, VectorSearchSpec,
};

#[napi]
pub enum ExecutionStrategy {
    DataFusion,
    LanceNative,
}

#[napi]
pub enum SqlDialect {
    Default,
    Spark,
    PostgreSql,
    MySql,
    Sqlite,
}

#[napi(object)]
pub struct GraphConfigInfo {
    pub node_labels: Vec<String>,
    pub relationship_types: Vec<String>,
}

/// Validate a config, as `GraphConfigBuilder.build()` does in Python.
#[napi]
pub fn build_graph_config(spec: GraphConfigSpec) -> napi::Result<GraphConfigInfo> {
    let config = graph_config(&spec)?;
    Ok(GraphConfigInfo {
        node_labels: config
            .iter_node_mappings()
            .map(|(name, _)| name.clone())
            .collect(),
        relationship_types: config
            .iter_relationship_mappings()
            .map(|(name, _)| name.clone())
            .collect(),
    })
}

#[napi(object)]
pub struct ParsedCypher {
    pub variables: Vec<String>,
    pub node_labels: Vec<String>,
    pub relationship_types: Vec<String>,
}

/// Parse a query, raising on a syntax error, as the Python `CypherQuery` constructor does.
#[napi]
pub fn parse_cypher(query: String) -> napi::Result<ParsedCypher> {
    let query = RustCypherQuery::new(&query).map_err(graph_error)?;
    Ok(ParsedCypher {
        variables: query.variables(),
        node_labels: query.ast().get_node_labels(),
        relationship_types: query.ast().get_relationship_types(),
    })
}

/// Everything a `CypherQuery` holds, sent with each call.
#[napi(object)]
pub struct CypherQuerySpec {
    pub query: String,
    pub config: Option<GraphConfigSpec>,
    /// JSON text of the bound parameters.
    pub parameters: Option<String>,
}

fn cypher_query(spec: CypherQuerySpec) -> napi::Result<RustCypherQuery> {
    let mut query = RustCypherQuery::new(&spec.query).map_err(graph_error)?;
    if let Some(config) = &spec.config {
        query = query.with_config(graph_config(config)?);
    }
    Ok(query.with_parameters(params(spec.parameters)?))
}

fn strategy(strategy: Option<ExecutionStrategy>) -> Option<RustExecutionStrategy> {
    strategy.map(|s| match s {
        ExecutionStrategy::DataFusion => RustExecutionStrategy::DataFusion,
        ExecutionStrategy::LanceNative => RustExecutionStrategy::LanceNative,
    })
}

#[napi]
pub async fn cypher_query_execute(
    spec: CypherQuerySpec,
    datasets_in: HashMap<String, DatasetInput>,
    execution_strategy: Option<ExecutionStrategy>,
    timeout_ms: Option<f64>,
) -> napi::Result<Buffer> {
    let deadline = Deadline::new(timeout_ms)?;
    let query = cypher_query(spec)?;
    let tables = batches(datasets(datasets_in)?).await?;
    let result = deadline
        .run(query.execute(tables, strategy(execution_strategy)))
        .await?;
    batch_to_ipc(&result)
}

#[napi]
pub async fn cypher_query_execute_with_namespace(
    spec: CypherQuerySpec,
    base_uri: String,
    execution_strategy: Option<ExecutionStrategy>,
) -> napi::Result<Buffer> {
    let query = cypher_query(spec)?;
    let namespace = Arc::new(DirNamespace::new(base_uri));
    let result = query
        .execute_with_namespace_arc(namespace, strategy(execution_strategy))
        .await
        .map_err(graph_error)?;
    batch_to_ipc(&result)
}

#[napi]
pub async fn cypher_query_explain(
    spec: CypherQuerySpec,
    datasets_in: HashMap<String, DatasetInput>,
) -> napi::Result<String> {
    let query = cypher_query(spec)?;
    let tables = batches(datasets(datasets_in)?).await?;
    query.explain(tables).await.map_err(graph_error)
}

#[napi]
pub async fn cypher_query_to_sql(
    spec: CypherQuerySpec,
    datasets_in: HashMap<String, DatasetInput>,
    dialect: Option<SqlDialect>,
) -> napi::Result<String> {
    let query = cypher_query(spec)?;
    let tables = batches(datasets(datasets_in)?).await?;
    let dialect = dialect.map(|d| match d {
        SqlDialect::Default => RustSqlDialect::Default,
        SqlDialect::Spark => RustSqlDialect::Spark,
        SqlDialect::PostgreSql => RustSqlDialect::PostgreSql,
        SqlDialect::MySql => RustSqlDialect::MySql,
        SqlDialect::Sqlite => RustSqlDialect::Sqlite,
    });
    query.to_sql(tables, dialect).await.map_err(graph_error)
}

#[napi]
pub async fn cypher_query_execute_with_vector_rerank(
    spec: CypherQuerySpec,
    datasets_in: HashMap<String, DatasetInput>,
    search: VectorSearchSpec,
) -> napi::Result<Buffer> {
    let query = cypher_query(spec)?;
    let mut inputs = datasets(datasets_in)?;
    if search.use_lance_index.unwrap_or(false) {
        if let Some(label) = lance_index_label(&query, &search, &inputs)? {
            // Replace the label's table with the rows the Lance ANN search returns, then run
            // the query over them, as the Python bindings do with `to_table(nearest=...)`.
            let position = inputs.iter().position(|(name, _)| *name == label);
            let (_, dataset) = inputs.swap_remove(position.expect("label was found"));
            let nearest = lance_nearest(dataset, &search).await?;
            inputs.push((label, Dataset::Arrow(nearest)));
            let result = query
                .execute(batches(inputs).await?, None)
                .await
                .map_err(graph_error)?;
            return batch_to_ipc(&result);
        }
    }
    let tables = batches(inputs).await?;
    let result = query
        .execute_with_vector_rerank(tables, vector_search(&search))
        .await
        .map_err(graph_error)?;
    batch_to_ipc(&result)
}

/// The label whose Lance dataset can answer the search with its index, if the query is
/// simple enough (no `WITH` or `WHERE`) for that to give the same rows.
fn lance_index_label(
    query: &RustCypherQuery,
    search: &VectorSearchSpec,
    inputs: &[(String, Dataset)],
) -> napi::Result<Option<String>> {
    let ast = query.ast();
    if ast.with_clause.is_some()
        || ast.where_clause.is_some()
        || ast.post_with_where_clause.is_some()
    {
        return Ok(None);
    }
    if search.query_vector.is_none() {
        return Err(invalid(
            "VectorSearch.queryVector is required when useLanceIndex is enabled",
        ));
    }
    let (alias, _) = split_vector_column(&search.column);
    let aliases = alias_map(query);
    let label = match alias {
        Some(alias) => aliases.get(alias).cloned(),
        None if aliases.len() == 1 => aliases.values().next().cloned(),
        None => None,
    };
    Ok(label.filter(|label| {
        inputs
            .iter()
            .any(|(name, dataset)| name == label && matches!(dataset, Dataset::Lance { .. }))
    }))
}

async fn lance_nearest(dataset: Dataset, search: &VectorSearchSpec) -> napi::Result<RecordBatch> {
    let Dataset::Lance {
        uri,
        version,
        storage_options,
    } = dataset
    else {
        unreachable!("lance_index_label only returns Lance datasets");
    };
    let dataset = lance_csr::open_lance_dataset(&uri, version, &storage_options)
        .await
        .map_err(graph_error)?;
    let (_, column) = split_vector_column(&search.column);
    let spec = VectorSearchSpec {
        column: column.to_string(),
        query_vector: search.query_vector.clone(),
        metric: search.metric,
        top_k: search.top_k,
        include_distance: None,
        distance_column_name: None,
        use_lance_index: None,
    };
    let rows = vector_search(&spec)
        .search_lance(&dataset)
        .await
        .map_err(graph_error)?;
    Ok(rows)
}

fn split_vector_column(column: &str) -> (Option<&str>, &str) {
    match column.split_once('.') {
        Some((alias, rest)) => (Some(alias), rest),
        None => (None, column),
    }
}

fn alias_map(query: &RustCypherQuery) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let ast = query.ast();
    let clauses = ast
        .reading_clauses
        .iter()
        .chain(ast.post_with_reading_clauses.iter())
        .chain(
            ast.further_with_stages
                .iter()
                .flat_map(|stage| stage.reading_clauses.iter()),
        );
    for clause in clauses {
        let ReadingClause::Match(match_clause) = clause else {
            continue;
        };
        for pattern in &match_clause.patterns {
            let nodes: Vec<_> = match pattern {
                GraphPattern::Node(node) => vec![node],
                GraphPattern::Path(path) => std::iter::once(&path.start_node)
                    .chain(path.segments.iter().map(|segment| &segment.end_node))
                    .collect(),
            };
            for node in nodes {
                if let (Some(var), Some(label)) = (&node.variable, node.labels.first()) {
                    map.entry(var.clone()).or_insert_with(|| label.clone());
                }
            }
        }
    }
    map
}

/// `VectorSearch.search`: rank the rows of one table.
#[napi]
pub async fn vector_search_search(search: VectorSearchSpec, ipc: Buffer) -> napi::Result<Buffer> {
    let batch = ipc_to_batch(&ipc)?;
    let result = vector_search(&search)
        .search(&batch)
        .await
        .map_err(graph_error)?;
    batch_to_ipc(&result)
}

#[napi]
pub async fn sql_query_execute(
    sql: String,
    datasets_in: HashMap<String, DatasetInput>,
) -> napi::Result<Buffer> {
    let tables = batches(datasets(datasets_in)?).await?;
    let result = RustSqlQuery::new(&sql)
        .execute(tables)
        .await
        .map_err(graph_error)?;
    batch_to_ipc(&result)
}

#[napi]
pub async fn sql_query_explain(
    sql: String,
    datasets_in: HashMap<String, DatasetInput>,
) -> napi::Result<String> {
    let tables = batches(datasets(datasets_in)?).await?;
    RustSqlQuery::new(&sql)
        .explain(tables)
        .await
        .map_err(graph_error)
}
