// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Persisted CSR files, for engines built with `persist_csr`.
//!
//! The base CSR of a relationship dataset is stored inside the dataset's own
//! directory, one Arrow IPC file per dataset version: `_lance_csr/<version>.arrow`.
//! Lance's `cleanup_old_versions()` deletes only the files it recognises (data,
//! index, deletion and transaction files, temporary manifests), so it leaves this
//! folder alone.
//!
//! One record batch holds both directions, one row per edge in CSR position order:
//! `out_source`/`in_source` are run-end encoded source keys (the run ends are the
//! CSR offsets), next to `*_neighbor` and `*_edge_row`. The schema metadata records
//! the dataset version, the endpoint columns and fields, and the fragments the base
//! was built from, so [`edge_changes_since`] can bring an older file up to date.
//!
//! A file is used only if it parses, its arrays are well formed and its endpoint
//! fields match; anything else is ignored and the CSR is rebuilt. The arrays are
//! zstd-compressed (4.7 times smaller on SF1), so loading decompresses them straight
//! into the CSR's buffers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, RunArray, UInt64Array};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, UInt64Type};
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{read_footer_length, FileDecoder};
use arrow::ipc::root_as_footer;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::ipc::CompressionType;
use lance::dataset::Dataset;
use lance_table::format::Fragment;
use object_store::path::Path;

use crate::csr_expand::{endpoint_fields, Adjacency, EndpointFields};
use crate::csr_index::CsrIndex;
use crate::error::{GraphError, Result};

/// Folder at the dataset root that holds the files.
const DIR: &str = "_lance_csr";
/// Bumped whenever the layout changes; files of another format are ignored.
const FORMAT: &str = "1";

const KEY_FORMAT: &str = "lance_csr.format";
const KEY_VERSION: &str = "lance_csr.version";
const KEY_ENDPOINTS: &str = "lance_csr.endpoints";
const KEY_FIELDS: &str = "lance_csr.fields";
const KEY_FRAGMENTS: &str = "lance_csr.fragments";

/// A base CSR read from a file, with what is needed to bring it up to date.
pub(crate) struct StoredCsr {
    /// Dataset version the base was built from.
    pub version: u64,
    pub fragments: Vec<Fragment>,
    pub fields: EndpointFields,
    pub outgoing: Adjacency,
    pub incoming: Adjacency,
}

fn store_error(message: String) -> GraphError {
    GraphError::ExecutionError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

/// `<dataset base>/_lance_csr`. Lance exposes `<base>/data` but not the base itself.
fn dir(ds: &Dataset) -> Path {
    let data = ds.data_dir();
    let parts: Vec<_> = data.parts().collect();
    Path::from_iter(parts[..parts.len().saturating_sub(1)].iter().cloned()).join(DIR)
}

fn file_path(ds: &Dataset, version: u64) -> Path {
    dir(ds).join(format!("{version}.arrow"))
}

/// Versions of the files in `_lance_csr`, unsorted.
async fn stored_versions(ds: &Dataset) -> Vec<u64> {
    let Ok(store) = ds.object_store(None).await else {
        return Vec::new();
    };
    // A missing folder lists as empty or fails, depending on the store.
    store
        .read_dir(dir(ds))
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|name| name.strip_suffix(".arrow")?.parse().ok())
        .collect()
}

/// The newest stored base at or before `ds`'s version for columns `src -> dst`, or
/// `None` when there is none or it can't be used.
pub(crate) async fn load(ds: &Dataset, src: &str, dst: &str) -> Option<StoredCsr> {
    let current = ds.manifest().version;
    let version = stored_versions(ds)
        .await
        .into_iter()
        .filter(|&v| v <= current)
        .max()?;
    let store = ds.object_store(None).await.ok()?;
    let bytes = store.read_one_all(&file_path(ds, version)).await.ok()?;
    match decode(Buffer::from(bytes), src, dst) {
        Ok(stored) if stored.version == version => Some(stored),
        Ok(_) => None,
        Err(e) => {
            log::warn!("Ignoring stored CSR {version} of {}: {e}", ds.uri());
            None
        }
    }
}

/// Write the base CSR of `ds` at its pinned version, then delete older files.
///
/// `outgoing` and `incoming` must have no delta: they are the full build of `ds`.
pub(crate) async fn save(
    ds: &Dataset,
    src: &str,
    dst: &str,
    outgoing: &Adjacency,
    incoming: &Adjacency,
) -> Result<()> {
    let version = ds.manifest().version;
    let fields = endpoint_fields(ds, src, dst)
        .ok_or_else(|| store_error(format!("No endpoint columns {src}, {dst}")))?;
    let fragments: Vec<Fragment> = ds
        .get_fragments()
        .into_iter()
        .map(|f| f.metadata().clone())
        .collect();
    let bytes = encode(version, src, dst, &fields, &fragments, outgoing, incoming)?;

    let store = ds
        .object_store(None)
        .await
        .map_err(|e| store_error(format!("Lance: {e}")))?;
    let path = file_path(ds, version);
    // shortcut: the compressed file is held whole in memory before the upload (150 MiB for SF1); write it through a multipart upload if files get much larger.
    let payload = object_store::PutPayload::from(bytes);
    let put = store.put_if_absent(&path, payload.clone()).await;
    let put = match put {
        // An earlier file for this version wasn't usable (we only save after a build),
        // or another process just wrote the same content; replace it either way.
        Err(object_store::Error::AlreadyExists { .. }) => {
            store
                .delete(&path)
                .await
                .map_err(|e| store_error(format!("Lance: {e}")))?;
            store.put_if_absent(&path, payload).await
        }
        other => other,
    };
    put.map_err(|e| store_error(format!("Failed to write {path}: {e}")))?;

    for old in stored_versions(ds)
        .await
        .into_iter()
        .filter(|&v| v < version)
    {
        // Best effort: a file left behind is ignored once a newer one exists.
        let _ = store.delete(&file_path(ds, old)).await;
    }
    Ok(())
}

/// Delete every stored CSR file of `ds`, and the folder where the store allows it.
/// Returns how many files were deleted.
pub(crate) async fn delete_all(ds: &Dataset) -> Result<usize> {
    let store = ds
        .object_store(None)
        .await
        .map_err(|e| store_error(format!("Lance: {e}")))?;
    let mut deleted = 0;
    for version in stored_versions(ds).await {
        store
            .delete(&file_path(ds, version))
            .await
            .map_err(|e| store_error(format!("Failed to delete a stored CSR: {e}")))?;
        deleted += 1;
    }
    // Object stores have no folders; on local disk this drops the now empty one.
    let _ = store.remove_dir_all(dir(ds)).await;
    Ok(deleted)
}

fn source_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int64, false)),
            Arc::new(Field::new("values", DataType::Int64, true)),
        ),
        false,
    )
}

fn encode(
    version: u64,
    src: &str,
    dst: &str,
    fields: &EndpointFields,
    fragments: &[Fragment],
    outgoing: &Adjacency,
    incoming: &Adjacency,
) -> Result<Vec<u8>> {
    let metadata = HashMap::from([
        (KEY_FORMAT.to_string(), FORMAT.to_string()),
        (KEY_VERSION.to_string(), version.to_string()),
        (KEY_ENDPOINTS.to_string(), to_json(&[src, dst])?),
        (KEY_FIELDS.to_string(), to_json(fields)?),
        (KEY_FRAGMENTS.to_string(), to_json(fragments)?),
    ]);
    let mut schema_fields = Vec::new();
    let mut columns = Vec::new();
    for (prefix, adjacency) in [("out", outgoing), ("in", incoming)] {
        schema_fields.extend([
            source_field(&format!("{prefix}_source")),
            Field::new(format!("{prefix}_neighbor"), DataType::UInt64, false),
            Field::new(format!("{prefix}_edge_row"), DataType::UInt64, false),
        ]);
        columns.extend(encode_direction(adjacency)?);
    }
    let schema = Arc::new(Schema::new_with_metadata(schema_fields, metadata));
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| store_error(format!("Failed to encode CSR: {e}")))?;
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .map_err(|e| store_error(format!("Failed to encode CSR: {e}")))?;
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options)
        .and_then(|mut w| w.write(&batch).map(|_| w))
        .map_err(|e| store_error(format!("Failed to encode CSR: {e}")))?;
    writer
        .finish()
        .map_err(|e| store_error(format!("Failed to encode CSR: {e}")))?;
    drop(writer);
    Ok(bytes)
}

/// Source keys, neighbours and edge rows of one direction, one row per edge.
fn encode_direction(adjacency: &Adjacency) -> Result<[ArrayRef; 3]> {
    let (keys, csr) = adjacency.base_parts();
    let (offsets, neighbors, edge_rows) = csr.parts();
    // Every base source has at least one edge, so `offsets[1..]` are strictly
    // ascending: valid run ends, shared with the offsets rather than copied.
    let ends = ScalarBuffer::<i64>::new(offsets.inner().clone(), 1, keys.len());
    let sources =
        RunArray::<Int64Type>::try_new(&Int64Array::new(ends, None), &Int64Array::from(keys))
            .map_err(|e| store_error(format!("Failed to encode CSR sources: {e}")))?;
    Ok([
        Arc::new(sources),
        Arc::new(UInt64Array::new(neighbors.clone(), None)),
        Arc::new(UInt64Array::new(edge_rows.clone(), None)),
    ])
}

fn decode(bytes: Buffer, src: &str, dst: &str) -> Result<StoredCsr> {
    let bad = |what: &str| store_error(format!("malformed CSR file: {what}"));
    let ipc = |e: arrow::error::ArrowError| store_error(format!("malformed CSR file: {e}"));
    if bytes.len() < 10 {
        return Err(bad("too short"));
    }
    let trailer = bytes.len() - 10;
    let footer_len =
        read_footer_length(bytes[trailer..].try_into().expect("10 bytes")).map_err(ipc)?;
    let footer = root_as_footer(
        &bytes[trailer
            .checked_sub(footer_len)
            .ok_or_else(|| bad("footer"))?..trailer],
    )
    .map_err(|e| store_error(format!("malformed CSR file: {e}")))?;
    let schema = Arc::new(fb_to_schema(
        footer.schema().ok_or_else(|| bad("no schema"))?,
    ));
    let meta = |key: &str| schema.metadata().get(key).ok_or_else(|| bad(key));
    if meta(KEY_FORMAT)? != FORMAT {
        return Err(bad("unknown format"));
    }
    let endpoints: [String; 2] = from_json(meta(KEY_ENDPOINTS)?)?;
    if endpoints != [src, dst] {
        return Err(bad("built for other endpoint columns"));
    }
    let version = meta(KEY_VERSION)?.parse().map_err(|_| bad(KEY_VERSION))?;
    let fields = from_json(meta(KEY_FIELDS)?)?;
    let fragments = from_json(meta(KEY_FRAGMENTS)?)?;

    let blocks = footer.recordBatches().ok_or_else(|| bad("no batches"))?;
    if blocks.len() != 1 {
        return Err(bad("expected one batch"));
    }
    let block = blocks.get(0);
    let length = block.bodyLength() as usize + block.metaDataLength() as usize;
    if block.offset() as usize + length > bytes.len() {
        return Err(bad("batch out of range"));
    }
    let data = bytes.slice_with_length(block.offset() as usize, length);
    let batch = FileDecoder::new(schema, footer.version())
        .read_record_batch(block, &data)
        .map_err(ipc)?
        .ok_or_else(|| bad("empty batch"))?;
    Ok(StoredCsr {
        version,
        fragments,
        fields,
        outgoing: decode_direction(&batch, "out").ok_or_else(|| bad("outgoing arrays"))?,
        incoming: decode_direction(&batch, "in").ok_or_else(|| bad("incoming arrays"))?,
    })
}

fn decode_direction(batch: &RecordBatch, prefix: &str) -> Option<Adjacency> {
    let column = |name: &str| batch.column_by_name(&format!("{prefix}_{name}"));
    let sources = column("source")?.as_run_opt::<Int64Type>()?;
    let ends = sources.run_ends();
    let keys = sources.values().as_primitive_opt::<Int64Type>()?;
    if sources.offset() != 0 || keys.null_count() != 0 || keys.len() != ends.values().len() {
        return None;
    }
    let mut offsets = Vec::with_capacity(keys.len() + 1);
    offsets.push(0);
    offsets.extend(ends.values().iter().map(|&end| end as u64));
    let u64s = |name: &str| {
        let array = column(name)?.as_primitive_opt::<UInt64Type>()?;
        (array.null_count() == 0).then(|| array.values().clone())
    };
    let csr = CsrIndex::from_parts(offsets.into(), u64s("neighbor")?, u64s("edge_row")?)?;
    Adjacency::from_base(keys.values(), csr)
}

fn from_json<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_str(text).map_err(|e| store_error(format!("malformed CSR file: {e}")))
}

fn to_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| store_error(format!("Failed to encode CSR: {e}")))
}
