//! Conversions between the values the TypeScript layer passes and the engine's types.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance_csr::{
    ast::DistanceMetric as RustDistanceMetric, engine::EngineSource,
    GraphConfig as RustGraphConfig, GraphError, VectorSearch as RustVectorSearch,
};
use napi::bindgen_prelude::Buffer;
use napi::{Error, Status};
use napi_derive::napi;
use serde_json::Value as JsonValue;
use tokio::time::Instant;

/// The TypeScript layer turns these prefixes into `LanceCsrError.code`.
pub(crate) fn coded(code: &str, message: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, format!("[{code}] {message}"))
}

pub(crate) fn invalid(message: impl std::fmt::Display) -> Error {
    coded("InvalidArgument", message)
}

pub(crate) fn runtime(message: impl std::fmt::Display) -> Error {
    coded("Runtime", message)
}

pub(crate) fn timed_out() -> Error {
    coded("Timeout", "Query timeout exceeded")
}

/// The same split as the Python bindings' `ValueError`, `NotImplementedError` and `RuntimeError`.
pub(crate) fn graph_error(err: GraphError) -> Error {
    let code = match &err {
        GraphError::ParseError { .. }
        | GraphError::ConfigError { .. }
        | GraphError::PlanError { .. }
        | GraphError::InvalidPattern { .. } => "InvalidArgument",
        GraphError::UnsupportedFeature { .. } => "NotImplemented",
        _ => "Runtime",
    };
    coded(code, err)
}

#[napi(object)]
pub struct NodeTableSpec {
    pub label: String,
    pub primary_key: String,
}

#[napi(object)]
pub struct RelTableSpec {
    pub rel_type: String,
    pub from_table: String,
    pub to_table: String,
    pub src: String,
    pub dst: String,
}

/// What `GraphConfigBuilder` recorded, replayed into the Rust builder.
#[napi(object)]
pub struct GraphConfigSpec {
    pub nodes: Vec<NodeTableSpec>,
    pub relationships: Vec<RelTableSpec>,
}

pub(crate) fn graph_config(spec: &GraphConfigSpec) -> napi::Result<RustGraphConfig> {
    let mut builder = RustGraphConfig::builder();
    for node in &spec.nodes {
        builder = builder.node_table(&node.label, &node.primary_key);
    }
    for rel in &spec.relationships {
        builder = builder.rel_table(
            &rel.rel_type,
            &rel.from_table,
            &rel.to_table,
            &rel.src,
            &rel.dst,
        );
    }
    builder.build().map_err(graph_error)
}

/// One input table: Arrow IPC stream bytes, or a Lance dataset to open by URI.
#[napi(object)]
pub struct DatasetInput {
    pub ipc: Option<Buffer>,
    pub uri: Option<String>,
    pub version: Option<i64>,
    pub storage_options: Option<HashMap<String, String>>,
}

/// A [`DatasetInput`] with its bytes decoded, ready to cross into async code.
pub(crate) enum Dataset {
    Arrow(RecordBatch),
    Lance {
        uri: String,
        version: Option<u64>,
        storage_options: HashMap<String, String>,
    },
}

pub(crate) fn datasets(
    inputs: HashMap<String, DatasetInput>,
) -> napi::Result<Vec<(String, Dataset)>> {
    inputs
        .into_iter()
        .map(|(name, input)| {
            let dataset = match (input.ipc, input.uri) {
                (Some(ipc), None) => Dataset::Arrow(ipc_to_batch(&ipc)?),
                (None, Some(uri)) => Dataset::Lance {
                    uri,
                    version: input
                        .version
                        .map(|v| {
                            u64::try_from(v).map_err(|_| invalid("version must be nonnegative"))
                        })
                        .transpose()?,
                    storage_options: input.storage_options.unwrap_or_default(),
                },
                _ => {
                    return Err(invalid(format!(
                        "Dataset '{name}' needs exactly one of ipc or uri"
                    )))
                }
            };
            Ok((name, dataset))
        })
        .collect()
}

impl Dataset {
    /// Lance datasets stay on storage, pinned to the version given (or the latest).
    pub(crate) async fn into_source(self) -> napi::Result<EngineSource> {
        match self {
            Self::Arrow(batch) => Ok(EngineSource::Arrow(batch)),
            Self::Lance {
                uri,
                version,
                storage_options,
            } => EngineSource::open_lance(uri, version, storage_options)
                .await
                .map_err(graph_error),
        }
    }

    /// The one-shot `CypherQuery` and `SqlQuery` paths read every table into memory.
    pub(crate) async fn into_batch(self) -> napi::Result<RecordBatch> {
        match self {
            Self::Arrow(batch) => Ok(batch),
            Self::Lance {
                uri,
                version,
                storage_options,
            } => {
                let dataset = lance_csr::open_lance_dataset(&uri, version, &storage_options)
                    .await
                    .map_err(graph_error)?;
                let batch = dataset.scan().try_into_batch().await.map_err(runtime)?;
                normalize(batch)
            }
        }
    }
}

pub(crate) async fn sources(
    datasets: Vec<(String, Dataset)>,
) -> napi::Result<HashMap<String, EngineSource>> {
    let mut sources = HashMap::new();
    for (name, dataset) in datasets {
        sources.insert(name, dataset.into_source().await?);
    }
    Ok(sources)
}

pub(crate) async fn batches(
    datasets: Vec<(String, Dataset)>,
) -> napi::Result<HashMap<String, RecordBatch>> {
    let mut batches = HashMap::new();
    for (name, dataset) in datasets {
        batches.insert(name, dataset.into_batch().await?);
    }
    Ok(batches)
}

fn concat(schema: &SchemaRef, batches: &[RecordBatch]) -> napi::Result<RecordBatch> {
    arrow_select::concat::concat_batches(schema, batches)
        .map_err(|e| runtime(format!("Failed to concatenate batches: {e}")))
}

pub(crate) fn ipc_to_batch(bytes: &[u8]) -> napi::Result<RecordBatch> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| invalid(format!("Failed to read Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(format!("Failed to read Arrow batch: {e}")))?;
    normalize(concat(&schema, &batches)?)
}

/// Strip schema metadata, which DataFusion expects to be absent across optimization passes
/// (as in the Python bindings), and decode dictionary columns. apache-arrow JS builds string
/// columns from plain arrays as dictionaries, where pyarrow builds plain strings, and the
/// engine's string functions don't accept dictionaries.
fn normalize(batch: RecordBatch) -> napi::Result<RecordBatch> {
    let has_dictionary = batch
        .schema()
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Dictionary(..)));
    if batch.schema().metadata().is_empty() && !has_dictionary {
        return Ok(batch);
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Dictionary(_, value) => {
                let decoded = arrow_cast::cast(column, value).map_err(|e| {
                    invalid(format!("Failed to decode column {}: {e}", field.name()))
                })?;
                fields.push(Field::new(
                    field.name(),
                    value.as_ref().clone(),
                    field.is_nullable(),
                ));
                columns.push(decoded);
            }
            _ => {
                fields.push(field.as_ref().clone());
                columns.push(column.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(runtime)
}

pub(crate) fn batch_to_ipc(batch: &RecordBatch) -> napi::Result<Buffer> {
    schema_and_batches_to_ipc(&batch.schema(), std::slice::from_ref(batch))
}

pub(crate) fn schema_and_batches_to_ipc(
    schema: &Schema,
    batches: &[RecordBatch],
) -> napi::Result<Buffer> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).map_err(runtime)?;
    for batch in batches {
        writer.write(batch).map_err(runtime)?;
    }
    writer.finish().map_err(runtime)?;
    drop(writer);
    Ok(bytes.into())
}

/// `$name` parameters arrive as JSON text: the TypeScript layer has already checked the values.
pub(crate) fn params(json: Option<String>) -> napi::Result<HashMap<String, JsonValue>> {
    match json {
        None => Ok(HashMap::new()),
        Some(text) => {
            serde_json::from_str(&text).map_err(|e| invalid(format!("Invalid parameters: {e}")))
        }
    }
}

/// One absolute deadline for planning, execution and every streamed read.
#[derive(Clone, Copy)]
pub(crate) struct Deadline(pub(crate) Option<Instant>);

impl Deadline {
    pub(crate) fn new(timeout_ms: Option<f64>) -> napi::Result<Self> {
        let Some(ms) = timeout_ms else {
            return Ok(Self(None));
        };
        let duration = Duration::try_from_secs_f64(ms / 1000.0)
            .map_err(|_| invalid("timeoutMs must be a finite nonnegative number"))?;
        let instant = Instant::now()
            .checked_add(duration)
            .ok_or_else(|| invalid("timeoutMs exceeds the supported deadline range"))?;
        Ok(Self(Some(instant)))
    }

    pub(crate) fn expired(self) -> bool {
        self.0.is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Run `future` under the deadline: the engine sees it while planning, and the
    /// future is dropped (cancelling its work) once it passes.
    pub(crate) async fn run<T>(
        self,
        future: impl std::future::Future<Output = lance_csr::Result<T>>,
    ) -> napi::Result<T> {
        if self.expired() {
            return Err(timed_out());
        }
        let future = lance_csr::query::with_execution_deadline(self.0, future);
        let result = match self.0 {
            None => future.await,
            Some(deadline) => tokio::time::timeout_at(deadline, future)
                .await
                .map_err(|_| timed_out())?,
        };
        if self.expired() {
            return Err(timed_out());
        }
        result.map_err(graph_error)
    }
}

#[napi]
#[derive(Clone, Copy)]
pub enum DistanceMetric {
    L2,
    Cosine,
    Dot,
}

/// What `VectorSearch` recorded, replayed into the Rust builder.
#[napi(object)]
pub struct VectorSearchSpec {
    pub column: String,
    pub query_vector: Option<Vec<f64>>,
    pub metric: Option<DistanceMetric>,
    pub top_k: Option<u32>,
    pub include_distance: Option<bool>,
    pub distance_column_name: Option<String>,
    pub use_lance_index: Option<bool>,
}

pub(crate) fn vector_search(spec: &VectorSearchSpec) -> RustVectorSearch {
    let mut search = RustVectorSearch::new(&spec.column);
    if let Some(vector) = &spec.query_vector {
        search = search.query_vector(vector.iter().map(|v| *v as f32).collect());
    }
    if let Some(metric) = spec.metric {
        search = search.metric(match metric {
            DistanceMetric::L2 => RustDistanceMetric::L2,
            DistanceMetric::Cosine => RustDistanceMetric::Cosine,
            DistanceMetric::Dot => RustDistanceMetric::Dot,
        });
    }
    if let Some(k) = spec.top_k {
        search = search.top_k(k as usize);
    }
    if let Some(include) = spec.include_distance {
        search = search.include_distance(include);
    }
    if let Some(name) = &spec.distance_column_name {
        search = search.distance_column_name(name);
    }
    search
}
