//! Fixtures for the Node tests. Built into every binary, so the release builds run the
//! whole test suite, but left out of the loader and types: it isn't public API.

use arrow_array::RecordBatchIterator;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

use crate::convert::{ipc_to_batch, runtime};

/// Write Arrow IPC rows to a Lance dataset: `mode` is `create`, `append` or `overwrite`.
#[napi(skip_typescript, js_name = "_writeLanceDataset")]
pub async fn write_lance_dataset(
    uri: String,
    ipc: Buffer,
    mode: String,
    max_rows_per_file: Option<u32>,
) -> napi::Result<i64> {
    let batch = ipc_to_batch(&ipc)?;
    let mode = match mode.as_str() {
        "create" => WriteMode::Create,
        "append" => WriteMode::Append,
        "overwrite" => WriteMode::Overwrite,
        other => return Err(runtime(format!("unknown write mode {other}"))),
    };
    let schema = batch.schema();
    let reader = RecordBatchIterator::new([Ok(batch)], schema);
    let mut params = WriteParams {
        mode,
        ..Default::default()
    };
    if let Some(rows) = max_rows_per_file {
        params.max_rows_per_file = rows as usize;
        params.max_rows_per_group = params.max_rows_per_group.min(rows as usize);
    }
    let dataset = Dataset::write(reader, &uri, Some(params))
        .await
        .map_err(runtime)?;
    Ok(dataset.version().version as i64)
}
