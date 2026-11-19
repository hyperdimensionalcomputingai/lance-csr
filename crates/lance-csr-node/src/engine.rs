//! `CypherEngine` and `SqlEngine`: tables registered once, many queries.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use lance_csr::engine::{CypherEngine as RustCypherEngine, EngineOptions, MAX_TARGET_PARTITIONS};
use lance_csr::SqlEngine as RustSqlEngine;
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use tokio::sync::Mutex;

use crate::convert::{
    batch_to_ipc, datasets, graph_config, graph_error, invalid, params, runtime,
    schema_and_batches_to_ipc, sources, timed_out, DatasetInput, Deadline, GraphConfigSpec,
    VectorSearchSpec,
};

/// Index builds are CPU-bound inside async functions, so they run on a blocking thread
/// instead of holding up a runtime worker that other queries need.
async fn blocking<T: Send + 'static>(
    future: impl Future<Output = lance_csr::Result<T>> + Send + 'static,
) -> napi::Result<T> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || handle.block_on(future))
        .await
        .map_err(runtime)?
        .map_err(graph_error)
}

#[napi(object)]
pub struct NodeIndexStats {
    pub rows: i64,
    pub version: Option<i64>,
    pub build_ms: f64,
}

#[napi(object)]
pub struct CsrStats {
    pub edges: i64,
    pub delta_edges: i64,
    pub tombstones: i64,
    pub unindexed_edges: i64,
    pub update: String,
    pub version: Option<i64>,
    pub base_version: Option<i64>,
    pub build_ms: f64,
}

#[napi]
pub struct CypherEngine {
    inner: Arc<RustCypherEngine>,
}

#[napi]
pub async fn open_cypher_engine(
    config: GraphConfigSpec,
    datasets_in: HashMap<String, DatasetInput>,
    use_csr: bool,
    target_partitions: Option<f64>,
    persist_csr: bool,
) -> napi::Result<CypherEngine> {
    if persist_csr && !use_csr {
        return Err(invalid("persistCsr requires useCsr: true"));
    }
    let target_partitions = target_partitions
        .map(|value| {
            // Out-of-range values that still fit a usize are rejected by the engine.
            if value.fract() != 0.0 || value < 1.0 || value > usize::MAX as f64 {
                return Err(invalid(format!(
                    "targetPartitions must be an integer between 1 and {MAX_TARGET_PARTITIONS}"
                )));
            }
            Ok(value as usize)
        })
        .transpose()?;
    let config = graph_config(&config)?;
    let sources = sources(datasets(datasets_in)?).await?;
    let options = EngineOptions {
        use_csr,
        target_partitions,
        persist_csr,
    };
    let inner = blocking(RustCypherEngine::with_options(config, sources, options)).await?;
    Ok(CypherEngine {
        inner: Arc::new(inner),
    })
}

#[napi]
impl CypherEngine {
    #[napi]
    pub async fn execute(
        &self,
        query: String,
        params_json: Option<String>,
        timeout_ms: Option<f64>,
    ) -> napi::Result<Buffer> {
        let deadline = Deadline::new(timeout_ms)?;
        let params = params(params_json)?;
        let result = deadline.run(self.inner.execute(&query, params)).await?;
        batch_to_ipc(&result)
    }

    #[napi]
    pub async fn execute_batches(
        &self,
        query: String,
        params_json: Option<String>,
        timeout_ms: Option<f64>,
    ) -> napi::Result<RecordBatchStream> {
        let deadline = Deadline::new(timeout_ms)?;
        let params = params(params_json)?;
        let stream = deadline
            .run(self.inner.execute_stream(&query, params))
            .await?;
        Ok(RecordBatchStream::new(stream, deadline))
    }

    #[napi]
    pub async fn explain(
        &self,
        query: String,
        params_json: Option<String>,
        analyze: bool,
    ) -> napi::Result<String> {
        let params = params(params_json)?;
        if analyze {
            self.inner.explain_analyze(&query, params).await
        } else {
            self.inner.explain(&query, params).await
        }
        .map_err(graph_error)
    }

    #[napi]
    pub async fn checkout_latest(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        blocking(async move { inner.checkout_latest().await }).await
    }

    #[napi]
    pub async fn optimize_csr(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        blocking(async move { inner.optimize_csr().await }).await
    }

    #[napi]
    pub async fn delete_stored_csr(&self) -> napi::Result<u32> {
        let deleted = self.inner.delete_stored_csr().await.map_err(graph_error)?;
        u32::try_from(deleted).map_err(runtime)
    }

    #[napi]
    pub fn node_index_stats(&self) -> HashMap<String, NodeIndexStats> {
        self.inner
            .node_index_stats()
            .into_iter()
            .map(|(label, entry)| {
                let stats = NodeIndexStats {
                    rows: entry.rows as i64,
                    version: entry.version.map(|v| v as i64),
                    build_ms: entry.build_ms,
                };
                (label, stats)
            })
            .collect()
    }

    #[napi]
    pub fn csr_stats(&self) -> HashMap<String, CsrStats> {
        self.inner
            .csr_stats()
            .into_iter()
            .map(|(rel_type, entry)| {
                let stats = CsrStats {
                    edges: entry.edges as i64,
                    delta_edges: entry.delta_edges as i64,
                    tombstones: entry.tombstones as i64,
                    unindexed_edges: entry.unindexed_edges as i64,
                    update: entry.update.to_string(),
                    version: entry.version.map(|v| v as i64),
                    base_version: entry.base_version.map(|v| v as i64),
                    build_ms: entry.build_ms,
                };
                (rel_type, stats)
            })
            .collect()
    }

    #[napi]
    pub fn dataset_versions(&self) -> HashMap<String, i64> {
        self.inner
            .dataset_versions()
            .into_iter()
            .map(|(name, version)| (name, version as i64))
            .collect()
    }

    #[napi]
    pub async fn execute_with_vector_rerank(
        &self,
        query: String,
        search: VectorSearchSpec,
    ) -> napi::Result<Buffer> {
        let search = crate::convert::vector_search(&search);
        let result = self
            .inner
            .execute_with_vector_rerank(&query, &search)
            .await
            .map_err(graph_error)?;
        batch_to_ipc(&result)
    }
}

/// What a stream holds between reads; `None` once it ends, fails, times out or is closed.
struct StreamState {
    stream: Option<SendableRecordBatchStream>,
    expired: bool,
}

/// A query result read one batch at a time, each batch as Arrow IPC bytes.
#[napi]
pub struct RecordBatchStream {
    schema: arrow_schema::SchemaRef,
    state: Arc<Mutex<StreamState>>,
    deadline: Deadline,
    watchdog: Option<tokio::task::JoinHandle<()>>,
}

impl RecordBatchStream {
    fn new(stream: SendableRecordBatchStream, deadline: Deadline) -> Self {
        let schema = stream.schema();
        let state = Arc::new(Mutex::new(StreamState {
            stream: Some(stream),
            expired: false,
        }));
        // Past the deadline, drop the stream even if nobody reads again, so a reader that
        // stops early doesn't keep the query's memory.
        let watchdog = deadline.0.map(|instant| {
            let watched = state.clone();
            tokio::spawn(async move {
                tokio::time::sleep_until(instant).await;
                let mut state = watched.lock().await;
                if state.stream.take().is_some() {
                    state.expired = true;
                }
            })
        });
        Self {
            schema,
            state,
            deadline,
            watchdog,
        }
    }
}

#[napi]
impl RecordBatchStream {
    /// The result schema, as an IPC stream with no batches.
    #[napi]
    pub fn schema(&self) -> napi::Result<Buffer> {
        schema_and_batches_to_ipc(&self.schema, &[])
    }

    /// The next batch, or `null` at the end.
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<Buffer>> {
        let mut state = self.state.lock().await;
        if state.expired || self.deadline.expired() {
            state.stream.take();
            state.expired = true;
            return Err(timed_out());
        }
        let Some(stream) = state.stream.as_mut() else {
            return Ok(None);
        };
        let next = match self.deadline.0 {
            None => stream.next().await,
            Some(instant) => match tokio::time::timeout_at(instant, stream.next()).await {
                Ok(next) => next,
                Err(_) => {
                    state.stream.take();
                    state.expired = true;
                    return Err(timed_out());
                }
            },
        };
        match next {
            None => {
                state.stream.take();
                Ok(None)
            }
            Some(Err(err)) => {
                state.stream.take();
                Err(graph_error(err.into()))
            }
            Some(Ok(batch)) => batch_to_ipc(&batch).map(Some),
        }
    }

    /// Stop the query and release what it holds. Reading afterwards returns `null`.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        self.state.lock().await.stream.take();
        if let Some(watchdog) = &self.watchdog {
            watchdog.abort();
        }
        Ok(())
    }
}

impl Drop for RecordBatchStream {
    // A stream garbage-collected unread releases its query now, not at its deadline.
    fn drop(&mut self) {
        if let Some(watchdog) = &self.watchdog {
            watchdog.abort();
        }
        if let Ok(mut state) = self.state.try_lock() {
            state.stream.take();
        }
    }
}

#[napi]
pub struct SqlEngine {
    inner: RustSqlEngine,
}

#[napi]
pub async fn open_sql_engine(
    datasets_in: HashMap<String, DatasetInput>,
    config: Option<GraphConfigSpec>,
    use_csr: bool,
) -> napi::Result<SqlEngine> {
    if use_csr && config.is_none() {
        return Err(invalid("useCsr requires a config"));
    }
    let config = config
        .as_ref()
        .map(graph_config)
        .transpose()?
        .unwrap_or_default();
    let sources = sources(datasets(datasets_in)?).await?;
    let options = EngineOptions {
        use_csr,
        ..Default::default()
    };
    let inner = blocking(RustSqlEngine::with_graph(config, sources, options)).await?;
    Ok(SqlEngine { inner })
}

#[napi]
impl SqlEngine {
    /// SQL over a `CypherEngine`'s snapshot, sharing its tables and indexes.
    #[napi(factory)]
    pub fn from_engine(engine: &CypherEngine) -> Self {
        Self {
            inner: RustSqlEngine::from_engine(engine.inner.clone()),
        }
    }

    #[napi]
    pub async fn execute(
        &self,
        sql: String,
        params_json: Option<String>,
        timeout_ms: Option<f64>,
    ) -> napi::Result<Buffer> {
        let deadline = Deadline::new(timeout_ms)?;
        let params = params(params_json)?;
        let result = deadline.run(self.inner.execute(&sql, params)).await?;
        batch_to_ipc(&result)
    }

    #[napi]
    pub async fn explain(
        &self,
        sql: String,
        params_json: Option<String>,
        analyze: bool,
    ) -> napi::Result<String> {
        let params = params(params_json)?;
        if analyze {
            self.inner.explain_analyze(&sql, params).await
        } else {
            self.inner.explain(&sql, params).await
        }
        .map_err(graph_error)
    }

    #[napi]
    pub async fn checkout_latest(&self) -> napi::Result<()> {
        self.inner.checkout_latest().await.map_err(graph_error)
    }
}
