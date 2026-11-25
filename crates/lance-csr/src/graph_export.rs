//! Export a graph as dense CSR arrays for graph-algorithm libraries.
//!
//! Libraries such as icebug, igraph or scipy take a graph as compressed sparse rows
//! over dense node ids `0..n`: an `indptr` array of `n + 1` offsets and an `indices`
//! array of neighbour ids. The engine's own CSR (`csr_expand`) keys rows by the
//! original node keys through a hash map and only lists nodes with edges, so it
//! can't be handed over as is. This module builds the dense form from projected
//! scans of the key and endpoint columns:
//!
//! - node ids: each label gets a consecutive range, in label order, and within it
//!   the node table's rows in scan order (rows with a null key are skipped), so keys
//!   that collide across labels stay distinct;
//! - edges: endpoints are mapped to dense ids in parallel; an edge with a null
//!   endpoint is skipped, one whose key isn't in its node table, or whose weight is
//!   null in a weighted export, is dropped and counted;
//! - rows: each edge is packed as `row * n + column` into a `u64`, sorted in parallel
//!   and deduplicated, which sorts every neighbour list too; `indptr` is then a count
//!   per row. Directed graphs also get the in-edge CSR (PageRank reads in-edges), built
//!   by transposing the finished out-edge CSR rather than sorting a second copy of the
//!   edges; undirected graphs get one CSR holding both directions, a self-loop once.
//! - weights, only when asked for: each packed key is paired with its weight (16 bytes
//!   per edge instead of 8 while sorting), and duplicate edges are combined into one
//!   weight by a [`CombineEdges`] rule. Unweighted exports never allocate the pairs.
//!   [`EdgeWeights`] names a column per relationship type, or one shared column, and a
//!   default weight for types without one.

use std::collections::HashMap;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use futures::StreamExt;
use rayon::prelude::*;

use crate::config::GraphConfig;
use crate::csr_expand::is_supported_id_type;
use crate::engine::EngineSource;
use crate::error::{GraphError, Result};

type FastMap<K, V> = HashMap<K, V, RandomState>;

/// A graph as dense CSR arrays; see [`crate::CypherEngine::export_graph`].
#[derive(Debug, Clone)]
pub struct GraphExport {
    /// Number of nodes, `n`. Dense ids run from 0 to `n - 1`.
    pub num_nodes: u64,
    /// Whether the arrays describe a directed graph.
    pub directed: bool,
    /// The original key of each dense id.
    pub node_keys: Int64Array,
    /// Each node label's dense id range, `start..end`, in id order.
    pub label_ranges: Vec<(String, u64, u64)>,
    /// Out-edge offsets, `n + 1` entries. Undirected graphs list each edge both ways.
    pub out_indptr: UInt64Array,
    /// Out-edge targets, sorted within each row.
    pub out_indices: UInt64Array,
    /// In-edge offsets, `n + 1` entries; `None` for undirected graphs.
    pub in_indptr: Option<UInt64Array>,
    /// In-edge sources, sorted within each row; `None` for undirected graphs.
    pub in_indices: Option<UInt64Array>,
    /// The weight of each entry in `out_indices`; `None` unless a weight column was asked for.
    pub out_weights: Option<Float64Array>,
    /// The weight of each entry in `in_indices`; `None` for undirected or unweighted graphs.
    pub in_weights: Option<Float64Array>,
    /// Edges dropped because an endpoint key isn't in its node table, or, in a weighted
    /// export, because the weight is null.
    pub dropped_edges: u64,
}

/// How an edge listed more than once combines its weights into one. In an undirected
/// export, `a -> b` and `b -> a` are the same edge and combine too.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CombineEdges {
    /// Add the weights, as for message counts or transaction amounts.
    #[default]
    Sum,
    /// Keep the smallest, as for distances or costs.
    Min,
    /// Keep the largest.
    Max,
}

impl CombineEdges {
    fn apply(self, kept: f64, other: f64) -> f64 {
        match self {
            CombineEdges::Sum => kept + other,
            CombineEdges::Min => kept.min(other),
            CombineEdges::Max => kept.max(other),
        }
    }
}

impl std::str::FromStr for CombineEdges {
    type Err = GraphError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sum" => Ok(CombineEdges::Sum),
            "min" => Ok(CombineEdges::Min),
            "max" => Ok(CombineEdges::Max),
            _ => Err(config_error(format!(
                "Unknown combine_edges '{s}'; expected 'sum', 'min' or 'max'"
            ))),
        }
    }
}

/// Which column holds each relationship type's edge weights, and how repeated edges combine.
#[derive(Debug, Clone, Default)]
pub struct EdgeWeights {
    /// The weight column of every type not in `per_type`, case-insensitive. With `default`
    /// set, a type without this column gets the default instead, as long as some type has it.
    pub column: Option<String>,
    /// Weight columns by relationship type, both case-insensitive; a named column must exist.
    pub per_type: Vec<(String, String)>,
    /// The weight of every edge of a type that has no weight column. `None` makes such a type
    /// an error.
    pub default: Option<f64>,
    /// How an edge listed more than once combines its weights.
    pub combine: CombineEdges,
}

impl EdgeWeights {
    /// One column for every exported relationship type.
    pub fn column(name: impl Into<String>) -> Self {
        Self {
            column: Some(name.into()),
            ..Self::default()
        }
    }
}

/// Where a relationship type's weights come from.
enum RelWeight {
    Column(String),
    Constant(f64),
}

/// One relationship type to export: its dataset and the labels at both ends.
struct RelSource<'a> {
    name: String,
    source: &'a EngineSource,
    src: String,
    dst: String,
    weight: Option<RelWeight>,
    src_label: usize,
    dst_label: usize,
}

/// Build the export from `sources` (the engine's pinned snapshot).
///
/// `rel_types` `None` takes every relationship type whose dataset is registered and
/// whose endpoints are integers. `weights` says where each type's edge weights come from.
pub(crate) async fn export_graph(
    config: &GraphConfig,
    sources: &HashMap<String, EngineSource>,
    rel_types: Option<&[String]>,
    directed: bool,
    weights: Option<&EdgeWeights>,
) -> Result<GraphExport> {
    let find_source = |name: &str| {
        sources
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, source)| source)
    };
    let mappings = match rel_types {
        Some(types) => types
            .iter()
            .map(|t| {
                config
                    .get_relationship_mapping(t)
                    .ok_or_else(|| config_error(format!("Unknown relationship type '{t}'")))
            })
            .collect::<Result<Vec<_>>>()?,
        None => {
            let mut all: Vec<_> = config
                .iter_relationship_mappings()
                .map(|(_, m)| m)
                .filter(|m| {
                    find_source(&m.relationship_type).is_some_and(|s| {
                        let schema = s.schema();
                        [&m.source_id_field, &m.target_id_field].iter().all(|c| {
                            field_type(&schema, c).is_some_and(|t| is_supported_id_type(&t))
                        })
                    })
                })
                .collect();
            all.sort_by(|a, b| a.relationship_type.cmp(&b.relationship_type));
            all
        }
    };
    if mappings.is_empty() {
        return Err(config_error(
            "No relationship type to export: none has a dataset with integer endpoints".to_string(),
        ));
    }

    // Labels at either end, sorted so ids don't depend on the order types are given in.
    let label_of = |label: &Option<String>, rel_type: &str| -> Result<String> {
        match label {
            Some(label) => config
                .get_node_mapping(label)
                .map(|m| m.label.clone())
                .ok_or_else(|| config_error(format!("Unknown node label '{label}' in '{rel_type}'"))),
            None if config.iter_node_mappings().len() == 1 => {
                Ok(config.iter_node_mappings().next().expect("one mapping").1.label.clone())
            }
            None => Err(config_error(format!(
                "Relationship type '{rel_type}' has no source or target label; set them in the config"
            ))),
        }
    };
    let mut ends = Vec::with_capacity(mappings.len());
    for m in &mappings {
        let rel = &m.relationship_type;
        ends.push((
            label_of(&m.source_label, rel)?,
            label_of(&m.target_label, rel)?,
        ));
    }
    let mut labels: Vec<String> = ends
        .iter()
        .flat_map(|(s, d)| [s.clone(), d.clone()])
        .collect();
    labels.sort();
    labels.dedup();

    let mut rels = Vec::with_capacity(mappings.len());
    for (m, (src_label, dst_label)) in mappings.iter().zip(&ends) {
        let rel = &m.relationship_type;
        let source = find_source(rel)
            .ok_or_else(|| config_error(format!("No dataset registered for '{rel}'")))?;
        let schema = source.schema();
        let column = |name: &str| -> Result<String> {
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(name))
                .ok_or_else(|| config_error(format!("Column '{name}' not found in '{rel}'")))?;
            if !is_supported_id_type(field.data_type()) {
                return Err(config_error(format!(
                    "Column '{name}' of '{rel}' is {}, not an integer type the export supports",
                    field.data_type()
                )));
            }
            Ok(field.name().clone())
        };
        let weight = weights.map(|w| rel_weight(w, rel, &schema)).transpose()?;
        rels.push(RelSource {
            name: rel.clone(),
            source,
            src: column(&m.source_id_field)?,
            dst: column(&m.target_id_field)?,
            weight,
            src_label: labels.binary_search(src_label).expect("collected above"),
            dst_label: labels.binary_search(dst_label).expect("collected above"),
        });
    }

    if let Some(w) = weights {
        for (rel_type, _) in &w.per_type {
            if !rels.iter().any(|r| r.name.eq_ignore_ascii_case(rel_type)) {
                return Err(config_error(format!(
                    "Weight column given for '{rel_type}', which isn't an exported relationship type"
                )));
            }
        }
        // A shared column that no type has is a typo, not a reason to use the default.
        let any_column = rels
            .iter()
            .any(|r| matches!(r.weight, Some(RelWeight::Column(_))));
        if let (Some(name), false) = (&w.column, any_column) {
            return Err(config_error(format!(
                "Weight column '{name}' not found in any exported relationship type"
            )));
        }
    }

    // Node keys -> dense ids, one map per label.
    let mut node_keys: Vec<i64> = Vec::new();
    let mut label_ranges = Vec::with_capacity(labels.len());
    let mut maps: Vec<FastMap<i64, u64>> = Vec::with_capacity(labels.len());
    for label in &labels {
        let mapping = config.get_node_mapping(label).expect("resolved above");
        let source = find_source(label)
            .ok_or_else(|| config_error(format!("No dataset registered for '{label}'")))?;
        if !field_type(&source.schema(), &mapping.id_field)
            .is_some_and(|t| is_supported_id_type(&t))
        {
            return Err(config_error(format!(
                "Key column '{}' of '{label}' is missing or not an integer type the export supports",
                mapping.id_field
            )));
        }
        let start = node_keys.len() as u64;
        let mut map = FastMap::default();
        let mut stream = source.scan_columns(&[mapping.id_field.as_str()]).await?;
        while let Some(batch) = stream.next().await {
            let keys = int64(batch?.column(0), &mapping.id_field, label)?;
            for key in keys.iter().flatten() {
                let id = node_keys.len() as u64;
                if map.insert(key, id).is_some() {
                    return Err(exec_error(format!(
                        "Node key {key} appears twice in '{label}'; the export needs unique keys"
                    )));
                }
                node_keys.push(key);
            }
        }
        label_ranges.push((label.clone(), start, node_keys.len() as u64));
        maps.push(map);
    }
    let n = node_keys.len() as u64;
    if n > u64::from(u32::MAX) {
        // `row * n + column` must fit in a u64.
        return Err(exec_error(format!(
            "{n} nodes is more than the export supports"
        )));
    }

    // Row counts (Lance metadata) size the edge vectors.
    let mut rows = 0;
    for rel in &rels {
        rows += rel.source.num_rows(&rel.name).await?;
    }
    let combine = weights.map(|w| w.combine).unwrap_or_default();
    let mut edges = EdgeBuilder::new(n, directed, weights.is_some(), rows);
    for rel in &rels {
        let (src_map, dst_map) = (&maps[rel.src_label], &maps[rel.dst_label]);
        let mut columns = vec![rel.src.as_str(), rel.dst.as_str()];
        let (weight_column, constant) = match &rel.weight {
            Some(RelWeight::Column(name)) => (Some(name.as_str()), 0.0),
            Some(RelWeight::Constant(value)) => (None, *value),
            None => (None, 0.0),
        };
        columns.extend(weight_column);
        let mut stream = rel.source.scan_columns(&columns).await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let s = int64(batch.column(0), &rel.src, &rel.name)?;
            let d = int64(batch.column(1), &rel.dst, &rel.name)?;
            let w = match weight_column {
                Some(name) => Some(float64(batch.column(2), name, &rel.name)?),
                None => None,
            };
            edges.add(&s, &d, src_map, dst_map, w.as_ref(), constant);
        }
    }
    Ok(edges.finish(Int64Array::from(node_keys), label_ranges, combine))
}

/// The node label of each of a query's first two RETURN columns, when both are a node's key.
///
/// `a.id` resolves to `Person` when the MATCH binds `a` to the single label `Person` and `id`
/// is that label's key column. Anything else (an unlabeled or multi-label node, a computed
/// expression, a WITH stage or UNION that may rebind variables) gives `None`: the keys are
/// then one key space without labels.
pub(crate) fn returned_labels(
    config: &GraphConfig,
    query: &crate::ast::CypherQuery,
) -> Option<[String; 2]> {
    use crate::ast::{GraphPattern, NodePattern, ReadingClause, ValueExpression};
    if query.with_clause.is_some()
        || !query.further_with_stages.is_empty()
        || !query.union_branches.is_empty()
    {
        return None;
    }
    let mut labels: HashMap<String, Vec<String>> = HashMap::new();
    let mut note = |node: &NodePattern| {
        if let Some(variable) = &node.variable {
            labels
                .entry(variable.clone())
                .or_default()
                .extend(node.labels.iter().cloned());
        }
    };
    for clause in &query.reading_clauses {
        let ReadingClause::Match(m) = clause else {
            continue;
        };
        for pattern in &m.patterns {
            match pattern {
                GraphPattern::Node(node) => note(node),
                GraphPattern::Path(path) => {
                    note(&path.start_node);
                    path.segments.iter().for_each(|s| note(&s.end_node));
                }
            }
        }
    }
    let resolve = |i: usize| -> Option<String> {
        let ValueExpression::Property(p) = &query.return_clause.items.get(i)?.expression else {
            return None;
        };
        let mut names = labels.get(&p.variable)?.clone();
        names.sort_by_key(|l| l.to_lowercase());
        names.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
        let [label] = names.as_slice() else {
            return None;
        };
        let mapping = config.get_node_mapping(label)?;
        mapping
            .id_field
            .eq_ignore_ascii_case(&p.property)
            .then(|| mapping.label.clone())
    };
    Some([resolve(0)?, resolve(1)?])
}

/// Build an export from a query result whose first two columns are the source and target
/// keys. With `labels`, each column's keys are that label's nodes and ids are grouped per
/// label as in [`export_graph`]; without, all keys share one id space.
pub(crate) fn export_rows(
    rows: &arrow::array::RecordBatch,
    labels: Option<[String; 2]>,
    directed: bool,
    weights: Option<&EdgeWeights>,
) -> Result<GraphExport> {
    const TABLE: &str = "the query result";
    let schema = rows.schema();
    if schema.fields().len() < 2 {
        return Err(config_error(
            "The query must return the source and target keys as its first two columns".to_string(),
        ));
    }
    let mut ends = Vec::with_capacity(2);
    for field in &schema.fields()[..2] {
        if !is_supported_id_type(field.data_type()) {
            return Err(config_error(format!(
                "Column '{}' of {TABLE} is {}, not an integer key type the export supports",
                field.name(),
                field.data_type()
            )));
        }
        ends.push(int64(rows.column(ends.len()), field.name(), TABLE)?);
    }
    let (s, d) = (&ends[0], &ends[1]);
    let (weight, constant) = match weights {
        None => (None, 0.0),
        Some(w) if !w.per_type.is_empty() => {
            return Err(config_error(
                "A subgraph's weights come from a column of the query result: name one, not \
                 one per relationship type"
                    .to_string(),
            ))
        }
        Some(EdgeWeights {
            column: Some(name), ..
        }) => {
            let (i, field) = schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, f)| f.name().eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    config_error(format!("Weight column '{name}' not found in {TABLE}"))
                })?;
            if !field.data_type().is_numeric() {
                return Err(config_error(format!(
                    "Weight column '{name}' of {TABLE} is {}, not a numeric type",
                    field.data_type()
                )));
            }
            (Some(float64(rows.column(i), name, TABLE)?), 0.0)
        }
        Some(EdgeWeights {
            default: Some(value),
            ..
        }) => (None, *value),
        Some(_) => {
            return Err(config_error(
                "Edge weights need a weight column or a default weight".to_string(),
            ))
        }
    };

    // Each id space's keys: the endpoints present, sorted. One space per label (in label
    // name order, as in `export_graph`), or one for all keys.
    let valid = |i: &usize| s.is_valid(*i) && d.is_valid(*i);
    let column_keys = |column: &Int64Array| -> Vec<i64> {
        (0..rows.num_rows())
            .filter(valid)
            .map(|i| column.value(i))
            .collect()
    };
    let mut spaces: Vec<(Option<String>, Vec<i64>)> = match &labels {
        Some([a, b]) if a == b => {
            let mut keys = column_keys(s);
            keys.extend(column_keys(d));
            vec![(Some(a.clone()), keys)]
        }
        Some([a, b]) => {
            let mut spaces = vec![
                (Some(a.clone()), column_keys(s)),
                (Some(b.clone()), column_keys(d)),
            ];
            spaces.sort_by(|x, y| x.0.cmp(&y.0));
            spaces
        }
        None => {
            let mut keys = column_keys(s);
            keys.extend(column_keys(d));
            vec![(None, keys)]
        }
    };
    let mut node_keys = Vec::new();
    let mut label_ranges = Vec::new();
    let mut maps = Vec::with_capacity(spaces.len());
    for (label, keys) in &mut spaces {
        keys.par_sort_unstable();
        keys.dedup();
        let start = node_keys.len() as u64;
        let mut map = FastMap::with_capacity_and_hasher(keys.len(), RandomState::default());
        for &key in keys.iter() {
            map.insert(key, node_keys.len() as u64);
            node_keys.push(key);
        }
        if let Some(label) = label {
            label_ranges.push((label.clone(), start, node_keys.len() as u64));
        }
        maps.push(map);
    }
    drop(spaces);
    let n = node_keys.len() as u64;
    if n > u64::from(u32::MAX) {
        // `row * n + column` must fit in a u64.
        return Err(exec_error(format!(
            "{n} nodes is more than the export supports"
        )));
    }
    // The source column's map comes first unless the labels are ordered the other way.
    let (src_map, dst_map) = match (&labels, maps.as_slice()) {
        (Some([a, b]), [first, second]) if a > b => (second, first),
        (_, [first, second]) => (first, second),
        (_, [only]) => (only, only),
        _ => unreachable!("one or two id spaces"),
    };
    let mut builder = EdgeBuilder::new(n, directed, weights.is_some(), rows.num_rows());
    builder.add(s, d, src_map, dst_map, weight.as_ref(), constant);
    let combine = weights.map(|w| w.combine).unwrap_or_default();
    Ok(builder.finish(Int64Array::from(node_keys), label_ranges, combine))
}

/// Edges mapped to dense ids, collected for the CSR build.
///
/// Out-edges only: a directed graph's in-edges come from transposing the out CSR, and an
/// undirected graph's reverse edges go into the same vector. Exactly one of the packed-key
/// and the `(key, weight)` vectors is used, so an unweighted build allocates nothing for
/// weights.
struct EdgeBuilder {
    n: u64,
    directed: bool,
    weighted: bool,
    out: Vec<u64>,
    out_w: Vec<(u64, f64)>,
    dropped: u64,
}

impl EdgeBuilder {
    /// `rows` sizes the vectors up front: growing by doubling would peak at several times
    /// the final size while copying.
    fn new(n: u64, directed: bool, weighted: bool, rows: usize) -> Self {
        let rows = if directed { rows } else { 2 * rows };
        let (keys, pairs) = if weighted { (0, rows) } else { (rows, 0) };
        Self {
            n,
            directed,
            weighted,
            out: Vec::with_capacity(keys),
            out_w: Vec::with_capacity(pairs),
            dropped: 0,
        }
    }

    /// Add one batch of edges. A null endpoint skips the edge; an endpoint key missing from
    /// its map, or a null weight, drops it and counts it. Without a weight column, a weighted
    /// build gives every edge `constant`.
    fn add(
        &mut self,
        s: &Int64Array,
        d: &Int64Array,
        src_map: &FastMap<i64, u64>,
        dst_map: &FastMap<i64, u64>,
        w: Option<&Float64Array>,
        constant: f64,
    ) {
        let pairs: Vec<Option<(u64, u64, usize)>> = (0..s.len())
            .into_par_iter()
            .with_min_len(4096)
            .filter(|&i| s.is_valid(i) && d.is_valid(i))
            .map(|i| {
                // A null weight drops the edge, as a missing endpoint does: no weight
                // is a safe stand-in for an unknown one.
                if w.is_some_and(|w| w.is_null(i)) {
                    return None;
                }
                Some((*src_map.get(&s.value(i))?, *dst_map.get(&d.value(i))?, i))
            })
            .collect();
        let n = self.n;
        for pair in pairs {
            let Some((a, b, i)) = pair else {
                self.dropped += 1;
                continue;
            };
            // A self-loop's reverse is itself; adding it would count its weight twice.
            let reverse = !self.directed && a != b;
            if self.weighted {
                let w = w.map_or(constant, |w| w.value(i));
                self.out_w.push((a * n + b, w));
                if reverse {
                    self.out_w.push((b * n + a, w));
                }
            } else {
                self.out.push(a * n + b);
                if reverse {
                    self.out.push(b * n + a);
                }
            }
        }
    }

    /// Sort, deduplicate (combining weights by `combine`) and build the CSR arrays.
    fn finish(
        self,
        node_keys: Int64Array,
        label_ranges: Vec<(String, u64, u64)>,
        combine: CombineEdges,
    ) -> GraphExport {
        let (n, directed) = (self.n, self.directed);
        let (out_indptr, out_indices, out_weights) = if self.weighted {
            drop(self.out);
            let (p, i, w) = weighted_csr(n, self.out_w, combine);
            (p, i, Some(w))
        } else {
            drop(self.out_w);
            let (p, i) = csr(n, self.out);
            (p, i, None)
        };
        let (in_indptr, in_indices, in_weights) = if directed {
            let (p, i, w) = transpose(
                out_indptr.values(),
                out_indices.values(),
                out_weights.as_ref().map(|w| w.values().as_ref()),
            );
            (Some(p), Some(i), w)
        } else {
            (None, None, None)
        };
        GraphExport {
            num_nodes: n,
            directed,
            node_keys,
            label_ranges,
            out_indptr,
            out_indices,
            in_indptr,
            in_indices,
            out_weights,
            in_weights,
            dropped_edges: self.dropped,
        }
    }
}

/// CSR offsets, `n + 1` entries, from each entry's row in order.
fn offsets(n: u64, rows: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut indptr = vec![0u64; n as usize + 1];
    for row in rows {
        indptr[row as usize + 1] += 1;
    }
    for i in 1..indptr.len() {
        indptr[i] += indptr[i - 1];
    }
    indptr
}

/// Sort and deduplicate packed `row * n + column` keys into `(indptr, indices)`.
fn csr(n: u64, mut packed: Vec<u64>) -> (UInt64Array, UInt64Array) {
    packed.par_sort_unstable();
    packed.dedup();
    let indptr = offsets(n, packed.iter().map(|key| key / n));
    packed
        .par_iter_mut()
        .with_min_len(4096)
        .for_each(|key| *key %= n);
    (UInt64Array::from(indptr), UInt64Array::from(packed))
}

/// [`csr`] for `(key, weight)` pairs: duplicate edges are combined by `combine`.
fn weighted_csr(
    n: u64,
    mut pairs: Vec<(u64, f64)>,
    combine: CombineEdges,
) -> (UInt64Array, UInt64Array, Float64Array) {
    pairs.par_sort_unstable_by_key(|p| p.0);
    // `dedup_by` passes the later element first, the one kept second.
    pairs.dedup_by(|later, kept| {
        let same = later.0 == kept.0;
        if same {
            kept.1 = combine.apply(kept.1, later.1);
        }
        same
    });
    let indptr = offsets(n, pairs.iter().map(|(key, _)| key / n));
    let weights: Vec<f64> = pairs.par_iter().map(|p| p.1).collect();
    // Collected in place into the pairs' allocation, then shrunk to the final size (measured
    // in place on macOS: a fresh vector instead raised the peak by 8 bytes per entry).
    let mut indices: Vec<u64> = pairs.into_iter().map(|p| p.0 % n).collect();
    indices.shrink_to_fit();
    (
        UInt64Array::from(indptr),
        UInt64Array::from(indices),
        Float64Array::from(weights),
    )
}

/// The in-edge CSR of a directed graph from its out-edge CSR, by counting and scattering.
///
/// Sources are visited in order, so every in-edge row comes out sorted, and the out CSR has
/// no duplicates, so neither does the result. Each weight is the out-edge's combined weight.
fn transpose(
    out_indptr: &[u64],
    out_indices: &[u64],
    out_weights: Option<&[f64]>,
) -> (UInt64Array, UInt64Array, Option<Float64Array>) {
    let n = out_indptr.len() - 1;
    // `indptr[t]` is the next free slot of row `t` while scattering; afterwards it holds
    // the end of row `t`, which the shift below turns back into the start of row `t + 1`.
    let mut indptr = offsets(n as u64, out_indices.iter().copied());
    let mut indices = vec![0u64; out_indices.len()];
    let mut weights = out_weights.map(|_| vec![0f64; out_indices.len()]);
    for source in 0..n {
        for edge in out_indptr[source] as usize..out_indptr[source + 1] as usize {
            let target = out_indices[edge] as usize;
            let slot = indptr[target] as usize;
            indptr[target] += 1;
            indices[slot] = source as u64;
            if let (Some(weights), Some(out)) = (weights.as_mut(), out_weights) {
                weights[slot] = out[edge];
            }
        }
    }
    indptr.copy_within(0..n, 1);
    indptr[0] = 0;
    (
        UInt64Array::from(indptr),
        UInt64Array::from(indices),
        weights.map(Float64Array::from),
    )
}

/// Add `column` to the node table of each label in `export`, from `values` indexed by dense id.
///
/// Each label's slice of `values` is paired with its keys and merged into the label's Lance
/// dataset on the key column, a new dataset version. Returns the updated sources, keyed like
/// `sources`. Every label is checked before anything is written, but a failure while writing a
/// later label leaves the earlier ones written.
pub(crate) async fn write_node_column(
    config: &GraphConfig,
    sources: &HashMap<String, EngineSource>,
    export: &GraphExport,
    column: &str,
    values: &ArrayRef,
    overwrite: bool,
) -> Result<Vec<(String, EngineSource)>> {
    if values.len() as u64 != export.num_nodes || export.node_keys.len() as u64 != export.num_nodes
    {
        return Err(config_error(format!(
            "Expected {} values, one per node of the export, got {}",
            export.num_nodes,
            values.len()
        )));
    }

    if export.label_ranges.is_empty() {
        return Err(config_error(
            "This export has no node labels (its query's keys couldn't be matched to labels), \
             so there is no node table to write to; join its node_keys to your table instead"
                .to_string(),
        ));
    }

    // Check every label first: a Lance dataset, and a column name that isn't its key.
    let mut targets = Vec::with_capacity(export.label_ranges.len());
    for (label, start, end) in &export.label_ranges {
        let mapping = config
            .get_node_mapping(label)
            .ok_or_else(|| config_error(format!("Unknown node label '{label}'")))?;
        let (name, source) = sources
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(label))
            .ok_or_else(|| config_error(format!("No dataset registered for '{label}'")))?;
        let EngineSource::Lance {
            uri,
            storage_options,
            dataset,
        } = source
        else {
            return Err(config_error(format!(
                "'{label}' is in-memory Arrow data; only Lance datasets can be written to"
            )));
        };
        let schema = source.schema();
        let key = schema
            .fields()
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(&mapping.id_field))
            .ok_or_else(|| {
                config_error(format!(
                    "Key column '{}' not found in '{label}'",
                    mapping.id_field
                ))
            })?;
        if key.name().eq_ignore_ascii_case(column) {
            return Err(config_error(format!(
                "'{column}' is the key column of '{label}' and can't be written"
            )));
        }
        let existing = schema
            .fields()
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(column))
            .map(|f| f.name().clone());
        if existing.is_some() && !overwrite {
            return Err(config_error(format!(
                "Column '{column}' already exists in '{label}'; pass overwrite to replace it"
            )));
        }
        targets.push((
            name.clone(),
            uri.clone(),
            storage_options.clone(),
            (**dataset).clone(),
            key.clone(),
            existing,
            *start as usize..*end as usize,
        ));
    }

    let mut updated = Vec::with_capacity(targets.len());
    for (name, uri, storage_options, mut dataset, key, existing, range) in targets {
        let keys: ArrayRef = std::sync::Arc::new(export.node_keys.slice(range.start, range.len()));
        let keys = cast(&keys, key.data_type())
            .map_err(|e| exec_error(format!("Can't convert keys of '{name}': {e}")))?;
        let slice = values.slice(range.start, range.len());
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(key.name(), key.data_type().clone(), true),
            arrow::datatypes::Field::new(column, slice.data_type().clone(), true),
        ]));
        let batch = arrow::array::RecordBatch::try_new(schema.clone(), vec![keys, slice])
            .map_err(|e| exec_error(format!("Can't build the batch for '{name}': {e}")))?;
        let lance_error =
            |e: lance::Error| exec_error(format!("Writing '{column}' to '{name}': {e}"));
        if let Some(existing) = existing {
            dataset
                .drop_columns(&[existing.as_str()])
                .await
                .map_err(lance_error)?;
        }
        let reader = arrow::array::RecordBatchIterator::new([Ok(batch)], schema);
        dataset
            .merge(reader, key.name(), key.name())
            .await
            .map_err(lance_error)?;
        updated.push((
            name,
            EngineSource::Lance {
                uri,
                storage_options,
                dataset: std::sync::Arc::new(dataset),
            },
        ));
    }
    Ok(updated)
}

/// Where `rel`'s weights come from: its column in `per_type`, else the shared column, else
/// the default. A column named for this type must exist and be numeric.
fn rel_weight(
    weights: &EdgeWeights,
    rel: &str,
    schema: &arrow::datatypes::Schema,
) -> Result<RelWeight> {
    let named = weights
        .per_type
        .iter()
        .find(|(t, _)| t.eq_ignore_ascii_case(rel))
        .map(|(_, column)| column);
    let (name, required) = match (named, &weights.column) {
        (Some(name), _) => (Some(name), true),
        (None, Some(name)) => (Some(name), weights.default.is_none()),
        (None, None) => (None, false),
    };
    let field = name.and_then(|name| {
        schema
            .fields()
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(name))
    });
    match (field, name, weights.default) {
        (Some(field), _, _) if !field.data_type().is_numeric() => Err(config_error(format!(
            "Weight column '{}' of '{rel}' is {}, not a numeric type",
            field.name(),
            field.data_type()
        ))),
        (Some(field), _, _) => Ok(RelWeight::Column(field.name().clone())),
        (None, Some(name), _) if required => Err(config_error(format!(
            "Weight column '{name}' not found in '{rel}'"
        ))),
        (None, _, Some(value)) => Ok(RelWeight::Constant(value)),
        (None, _, None) => Err(config_error(format!(
            "No weight column for '{rel}'; name one for it or set a default weight"
        ))),
    }
}

fn field_type(schema: &arrow::datatypes::Schema, name: &str) -> Option<DataType> {
    schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(name))
        .map(|f| f.data_type().clone())
}

fn int64(array: &ArrayRef, column: &str, table: &str) -> Result<Int64Array> {
    let array = cast(array, &DataType::Int64).map_err(|e| {
        exec_error(format!(
            "Column '{column}' of '{table}' is not an integer: {e}"
        ))
    })?;
    Ok(array.as_primitive::<Int64Type>().clone())
}

fn float64(array: &ArrayRef, column: &str, table: &str) -> Result<Float64Array> {
    let array = cast(array, &DataType::Float64).map_err(|e| {
        exec_error(format!(
            "Column '{column}' of '{table}' is not numeric: {e}"
        ))
    })?;
    Ok(array.as_primitive::<Float64Type>().clone())
}

fn config_error(message: String) -> GraphError {
    GraphError::ConfigError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn exec_error(message: String) -> GraphError {
    GraphError::ExecutionError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{RecordBatch, RecordBatchIterator, StringArray};
    use arrow::datatypes::{Field, Schema};
    use lance::dataset::{Dataset, WriteMode, WriteParams};

    use super::*;
    use crate::CypherEngine;
    use crate::DatasetOptimizeStats;

    fn batch(columns: &[(&str, Vec<Option<i64>>)]) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, true))
            .collect::<Vec<_>>();
        let arrays = columns
            .iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values.clone())) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    /// Non-contiguous keys in shuffled order, an isolated node (40), a duplicate edge
    /// (10->20), a self-loop (30->30), an edge to a missing node (99) and one with a
    /// null endpoint.
    fn people() -> (RecordBatch, RecordBatch) {
        let nodes = batch(&[("id", [60, 10, 50, 30, 20, 40].map(Some).to_vec())]);
        let edges = batch(&[
            (
                "src",
                vec![
                    Some(10),
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(30),
                    Some(10),
                    Some(50),
                    Some(60),
                    None,
                ],
            ),
            (
                "dst",
                vec![
                    Some(20),
                    Some(20),
                    Some(30),
                    Some(10),
                    Some(30),
                    Some(99),
                    Some(60),
                    Some(50),
                    Some(20),
                ],
            ),
        ]);
        (nodes, edges)
    }

    fn person_config() -> GraphConfig {
        GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap()
    }

    async fn arrow_engine(config: GraphConfig, tables: Vec<(&str, RecordBatch)>) -> CypherEngine {
        let sources = tables
            .into_iter()
            .map(|(name, batch)| (name.to_string(), EngineSource::Arrow(batch)))
            .collect();
        CypherEngine::new(config, sources, false).await.unwrap()
    }

    fn values(array: &UInt64Array) -> Vec<u64> {
        array.values().to_vec()
    }

    #[tokio::test]
    async fn directed_export_matches_hand_built_csr() {
        let (nodes, edges) = people();
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let g = engine.export_graph(None, true, None).await.unwrap();
        // Dense ids follow row order: 60->0, 10->1, 50->2, 30->3, 20->4, 40->5.
        assert_eq!(g.node_keys.values().to_vec(), vec![60, 10, 50, 30, 20, 40]);
        assert_eq!(g.num_nodes, 6);
        assert_eq!(g.label_ranges, vec![("Person".to_string(), 0, 6)]);
        assert_eq!(g.dropped_edges, 1);
        assert_eq!(values(&g.out_indptr), vec![0, 1, 2, 3, 5, 6, 6]);
        assert_eq!(values(&g.out_indices), vec![2, 4, 0, 1, 3, 3]);
        assert_eq!(
            values(g.in_indptr.as_ref().unwrap()),
            vec![0, 1, 2, 3, 5, 6, 6]
        );
        assert_eq!(
            values(g.in_indices.as_ref().unwrap()),
            vec![2, 3, 0, 3, 4, 1]
        );
    }

    #[tokio::test]
    async fn undirected_export_is_symmetric_with_self_loops_once() {
        let (nodes, edges) = people();
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let g = engine
            .export_graph(Some(&["knows".to_string()]), false, None)
            .await
            .unwrap();
        assert!(!g.directed && g.in_indptr.is_none() && g.in_indices.is_none());
        assert_eq!(values(&g.out_indptr), vec![0, 1, 3, 4, 7, 9, 9]);
        assert_eq!(values(&g.out_indices), vec![2, 3, 4, 0, 1, 3, 4, 1, 3]);
    }

    /// `people()` with an edge weight column `w`.
    fn weighted_people(weights: Vec<Option<f64>>) -> (RecordBatch, RecordBatch) {
        let (nodes, edges) = people();
        let mut fields = edges.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("w", DataType::Float64, true)));
        let mut columns = edges.columns().to_vec();
        columns.push(Arc::new(arrow::array::Float64Array::from(weights)));
        let edges = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        (nodes, edges)
    }

    fn powers_of_two() -> Vec<Option<f64>> {
        (0..9).map(|i| Some(f64::from(1 << i))).collect()
    }

    fn weights(array: &Option<Float64Array>) -> Vec<f64> {
        array.as_ref().unwrap().values().to_vec()
    }

    #[tokio::test]
    async fn weights_follow_their_edges_and_duplicates_sum() {
        let (nodes, edges) = weighted_people(powers_of_two());
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;

        // Unweighted: no weight arrays, same CSR as without the column.
        let plain = engine.export_graph(None, true, None).await.unwrap();
        assert!(plain.out_weights.is_none() && plain.in_weights.is_none());

        let g = engine
            .export_graph(None, true, Some(&EdgeWeights::column("W")))
            .await
            .unwrap();
        assert_eq!(
            (&g.out_indptr, &g.out_indices),
            (&plain.out_indptr, &plain.out_indices)
        );
        assert_eq!(
            (&g.in_indptr, &g.in_indices),
            (&plain.in_indptr, &plain.in_indices)
        );
        // 10->20 is listed twice, with weights 1 and 2.
        assert_eq!(
            weights(&g.out_weights),
            vec![128.0, 3.0, 64.0, 8.0, 16.0, 4.0]
        );
        assert_eq!(
            weights(&g.in_weights),
            vec![64.0, 8.0, 128.0, 16.0, 4.0, 3.0]
        );
        assert_eq!(g.dropped_edges, 1);

        // Undirected: 50->60 and 60->50 become one edge, the self-loop's weight counts once.
        let u = engine
            .export_graph(None, false, Some(&EdgeWeights::column("w")))
            .await
            .unwrap();
        let plain = engine.export_graph(None, false, None).await.unwrap();
        assert_eq!(
            (&u.out_indptr, &u.out_indices),
            (&plain.out_indptr, &plain.out_indices)
        );
        assert_eq!(
            weights(&u.out_weights),
            vec![192.0, 8.0, 3.0, 192.0, 8.0, 16.0, 4.0, 3.0, 4.0]
        );
        assert!(u.in_weights.is_none());
    }

    #[tokio::test]
    async fn combine_edges_picks_min_or_max() {
        let (nodes, edges) = weighted_people(powers_of_two());
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        // The duplicate 10->20 has weights 1 and 2; undirected 50-60 has 64 and 128.
        for (combine, dup, pair) in [
            (CombineEdges::Min, 1.0, 64.0),
            (CombineEdges::Max, 2.0, 128.0),
        ] {
            let g = engine
                .export_graph(
                    None,
                    true,
                    Some(&EdgeWeights {
                        combine,
                        ..EdgeWeights::column("w")
                    }),
                )
                .await
                .unwrap();
            assert_eq!(weights(&g.out_weights)[1], dup, "{combine:?}");
            assert_eq!(weights(&g.in_weights)[5], dup, "{combine:?}");
            let u = engine
                .export_graph(
                    None,
                    false,
                    Some(&EdgeWeights {
                        combine,
                        ..EdgeWeights::column("w")
                    }),
                )
                .await
                .unwrap();
            assert_eq!(
                weights(&u.out_weights),
                vec![pair, 8.0, dup, pair, 8.0, 16.0, 4.0, dup, 4.0],
                "{combine:?}"
            );
        }
        assert_eq!("MAX".parse::<CombineEdges>().unwrap(), CombineEdges::Max);
        assert!("mean".parse::<CombineEdges>().is_err());
    }

    #[tokio::test]
    async fn null_weights_drop_and_count_their_edges() {
        // Null weights on one copy of the duplicate 10->20 and on 30->10.
        let mut w = powers_of_two();
        w[1] = None;
        w[3] = None;
        let (nodes, edges) = weighted_people(w);
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let g = engine
            .export_graph(None, true, Some(&EdgeWeights::column("w")))
            .await
            .unwrap();
        // 10->99 and the two null weights; the null endpoint isn't counted.
        assert_eq!(g.dropped_edges, 3);
        assert_eq!(values(&g.out_indptr), vec![0, 1, 2, 3, 4, 5, 5]);
        assert_eq!(values(&g.out_indices), vec![2, 4, 0, 3, 3]);
        assert_eq!(weights(&g.out_weights), vec![128.0, 1.0, 64.0, 16.0, 4.0]);
        // Without `weight`, the column is ignored and nothing extra is dropped.
        let plain = engine.export_graph(None, true, None).await.unwrap();
        assert_eq!(plain.dropped_edges, 1);
    }

    /// KNOWS has a weight column `w`; LIKES has none, only an unrelated `since`.
    async fn mixed_engine() -> CypherEngine {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .rel_table("LIKES", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let knows = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src", DataType::Int64, true),
                Field::new("dst", DataType::Int64, true),
                Field::new("w", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 7.0])),
            ],
        )
        .unwrap();
        let likes = batch(&[
            ("src", vec![Some(1), Some(3)]),
            ("dst", vec![Some(2), Some(1)]),
            ("since", vec![Some(2020), Some(2021)]),
        ]);
        let nodes = batch(&[("id", vec![Some(1), Some(2), Some(3)])]);
        arrow_engine(
            config,
            vec![("Person", nodes), ("KNOWS", knows), ("LIKES", likes)],
        )
        .await
    }

    #[tokio::test]
    async fn weights_per_type_and_default() {
        let engine = mixed_engine().await;
        // Out-edges by dense id: 1->2 (KNOWS 5, and LIKES), 2->3 (KNOWS 7), 3->1 (LIKES).
        let export = |w: EdgeWeights| {
            let engine = &engine;
            async move { engine.export_graph(None, true, Some(&w)).await }
        };
        let per_type = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(t, c)| (t.to_string(), c.to_string()))
                .collect()
        };

        // A shared column with a default: LIKES edges get the default.
        let g = export(EdgeWeights {
            default: Some(1.0),
            ..EdgeWeights::column("w")
        })
        .await
        .unwrap();
        assert_eq!(values(&g.out_indices), vec![1, 2, 0]);
        assert_eq!(weights(&g.out_weights), vec![6.0, 7.0, 1.0]);

        // A column per type, any name.
        let g = export(EdgeWeights {
            per_type: per_type(&[("knows", "w"), ("LIKES", "SINCE")]),
            ..EdgeWeights::default()
        })
        .await
        .unwrap();
        assert_eq!(weights(&g.out_weights), vec![2025.0, 7.0, 2021.0]);

        // Some types named, a default for the rest.
        let g = export(EdgeWeights {
            per_type: per_type(&[("KNOWS", "w")]),
            default: Some(2.0),
            ..EdgeWeights::default()
        })
        .await
        .unwrap();
        assert_eq!(weights(&g.out_weights), vec![7.0, 7.0, 2.0]);
        assert_eq!(weights(&g.in_weights), vec![2.0, 7.0, 7.0]);

        // Errors: a type without a weight and no default; a column named for a type that
        // lacks it, even with a default; a type that isn't exported; a shared column no
        // type has.
        for w in [
            EdgeWeights::column("w"),
            EdgeWeights {
                per_type: per_type(&[("KNOWS", "w")]),
                ..EdgeWeights::default()
            },
            EdgeWeights {
                per_type: per_type(&[("LIKES", "w")]),
                default: Some(1.0),
                ..EdgeWeights::default()
            },
            EdgeWeights {
                per_type: per_type(&[("FOLLOWS", "w")]),
                default: Some(1.0),
                ..EdgeWeights::default()
            },
            EdgeWeights {
                default: Some(1.0),
                ..EdgeWeights::column("cost")
            },
        ] {
            let err = export(w.clone()).await.unwrap_err();
            assert!(
                matches!(err, GraphError::ConfigError { .. }),
                "{w:?}: {err}"
            );
        }

        // Only LIKES: the default is every weight.
        let g = engine
            .export_graph(
                Some(&["LIKES".to_string()]),
                true,
                Some(&EdgeWeights {
                    per_type: per_type(&[("KNOWS", "w")]),
                    default: Some(3.0),
                    ..EdgeWeights::default()
                }),
            )
            .await;
        assert!(g.is_err(), "KNOWS isn't exported");
    }

    #[tokio::test]
    async fn subgraph_of_a_query_by_the_export_rules() {
        let (nodes, edges) = weighted_people(powers_of_two());
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let sub = |q: &str, directed: bool, w: Option<EdgeWeights>| {
            let engine = &engine;
            let q = q.to_string();
            async move {
                engine
                    .export_subgraph(&q, HashMap::new(), directed, w.as_ref())
                    .await
            }
        };
        let all = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id";

        // The query's join drops 10->99 and the null endpoint; the nodes are the keys present,
        // sorted: 10, 20, 30, 50, 60. The isolated 40 isn't in the subgraph.
        let g = sub(all, true, None).await.unwrap();
        assert_eq!(g.node_keys.values().to_vec(), vec![10, 20, 30, 50, 60]);
        assert_eq!(g.label_ranges, vec![("Person".to_string(), 0, 5)]);
        assert_eq!(values(&g.out_indptr), vec![0, 1, 2, 4, 5, 6]);
        assert_eq!(values(&g.out_indices), vec![1, 2, 0, 2, 4, 3]);
        assert_eq!(
            values(g.in_indices.as_ref().unwrap()),
            vec![2, 0, 1, 2, 4, 3]
        );
        assert_eq!(g.dropped_edges, 0);

        // A filter selects the subgraph.
        let g = sub(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id >= 30 RETURN a.id, b.id",
            false,
            None,
        )
        .await
        .unwrap();
        assert_eq!(g.node_keys.values().to_vec(), vec![10, 30, 50, 60]);
        assert_eq!(values(&g.out_indptr), vec![0, 1, 3, 4, 5]);
        assert_eq!(values(&g.out_indices), vec![1, 0, 1, 3, 2]);

        // A weight column of the result: the duplicate 10->20 sums 1 and 2.
        let g = sub(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.id, b.id, r.w AS cost",
            true,
            Some(EdgeWeights::column("COST")),
        )
        .await
        .unwrap();
        assert_eq!(
            weights(&g.out_weights),
            vec![3.0, 4.0, 8.0, 16.0, 64.0, 128.0]
        );
        // One weight per row counts the rows behind each edge.
        let g = sub(
            all,
            true,
            Some(EdgeWeights {
                default: Some(1.0),
                ..EdgeWeights::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(weights(&g.out_weights), vec![2.0, 1.0, 1.0, 1.0, 1.0, 1.0]);

        // Keys that aren't a labeled node's key: one id space, no labels.
        let g = sub(
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.id, b.id",
            true,
            None,
        )
        .await
        .unwrap();
        assert!(g.label_ranges.is_empty());
        assert_eq!(g.node_keys.values().to_vec(), vec![10, 20, 30, 50, 60]);

        // Errors: a non-integer key column, weights per relationship type, a missing column.
        for (q, w) in [
            (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN 'x' AS s, b.id",
                None,
            ),
            (
                all,
                Some(EdgeWeights {
                    per_type: vec![("KNOWS".to_string(), "w".to_string())],
                    ..EdgeWeights::default()
                }),
            ),
            (all, Some(EdgeWeights::column("cost"))),
            ("MATCH (a:Person) RETURN a.id", None),
        ] {
            let err = sub(q, true, w).await.unwrap_err();
            assert!(matches!(err, GraphError::ConfigError { .. }), "{q}: {err}");
        }
    }

    #[tokio::test]
    async fn subgraph_labels_keep_colliding_keys_apart() {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .node_table("City", "id")
            .rel_table("LIVES_IN", "Person", "City", "src", "dst")
            .build()
            .unwrap();
        let ids = || batch(&[("id", vec![Some(1), Some(2), Some(3)])]);
        let lives_in = batch(&[
            ("src", vec![Some(1), Some(2), Some(3)]),
            ("dst", vec![Some(2), Some(1), Some(2)]),
        ]);
        let engine = arrow_engine(
            config,
            vec![("Person", ids()), ("City", ids()), ("LIVES_IN", lives_in)],
        )
        .await;
        let g = engine
            .export_subgraph(
                "MATCH (p:Person)-[:LIVES_IN]->(c:City) WHERE p.id <= 2 RETURN p.id, c.id",
                HashMap::new(),
                true,
                None,
            )
            .await
            .unwrap();
        // City 1, 2 then Person 1, 2: equal keys, distinct nodes.
        assert_eq!(
            g.label_ranges,
            vec![("City".to_string(), 0, 2), ("Person".to_string(), 2, 4)]
        );
        assert_eq!(g.node_keys.values().to_vec(), vec![1, 2, 1, 2]);
        assert_eq!(values(&g.out_indptr), vec![0, 0, 0, 1, 2]);
        assert_eq!(values(&g.out_indices), vec![1, 0]);
    }

    #[tokio::test]
    async fn bad_weight_columns_are_errors() {
        let (nodes, edges) = weighted_people(powers_of_two());
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let err = engine
            .export_graph(None, true, Some(&EdgeWeights::column("cost")))
            .await
            .unwrap_err();
        assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");
        let names = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src", DataType::Int64, true),
                Field::new("dst", DataType::Int64, true),
                Field::new("w", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(StringArray::from(vec!["heavy"])),
            ],
        )
        .unwrap();
        let (nodes, _) = people();
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", names)]).await;
        let err = engine
            .export_graph(None, true, Some(&EdgeWeights::column("w")))
            .await
            .unwrap_err();
        assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");
    }

    #[test]
    fn transpose_matches_sorting_the_reversed_edges() {
        // Random edges with duplicates and self-loops; weights tie each edge to its pair.
        let n = 50u64;
        let mut state = 7u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % n
        };
        let edges: Vec<(u64, u64)> = (0..400).map(|_| (next(), next())).collect();
        let weight = |a: u64, b: u64| (a * 1000 + b) as f64;
        let pairs = edges
            .iter()
            .map(|&(a, b)| (a * n + b, weight(a, b)))
            .collect();
        let (p, i, w) = weighted_csr(n, pairs, CombineEdges::Max);
        let (tp, ti, tw) = transpose(p.values(), i.values(), Some(w.values()));
        let (rp, ri) = csr(n, edges.iter().map(|&(a, b)| b * n + a).collect());
        assert_eq!((&tp, &ti), (&rp, &ri));
        // In-row `t` lists sources `s` of edges s -> t.
        let tw = tw.unwrap();
        for t in 0..n as usize {
            for e in tp.value(t) as usize..tp.value(t + 1) as usize {
                assert_eq!(tw.value(e), weight(ti.value(e), t as u64));
            }
        }
        let (_, _, none) = transpose(p.values(), i.values(), None);
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn labels_get_separate_ranges_for_colliding_keys() {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .node_table("City", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .rel_table("LIVES_IN", "Person", "City", "src", "dst")
            .build()
            .unwrap();
        let ids = || batch(&[("id", vec![Some(1), Some(2)])]);
        let engine = arrow_engine(
            config,
            vec![
                ("Person", ids()),
                ("City", ids()),
                (
                    "KNOWS",
                    batch(&[("src", vec![Some(1)]), ("dst", vec![Some(2)])]),
                ),
                (
                    "LIVES_IN",
                    batch(&[
                        ("src", vec![Some(1), Some(2)]),
                        ("dst", vec![Some(2), Some(1)]),
                    ]),
                ),
            ],
        )
        .await;
        let g = engine.export_graph(None, true, None).await.unwrap();
        // City 0..2, Person 2..4.
        assert_eq!(
            g.label_ranges,
            vec![("City".to_string(), 0, 2), ("Person".to_string(), 2, 4)]
        );
        assert_eq!(g.node_keys.values().to_vec(), vec![1, 2, 1, 2]);
        assert_eq!(values(&g.out_indptr), vec![0, 0, 0, 2, 3]);
        assert_eq!(values(&g.out_indices), vec![1, 3, 0]);
    }

    #[tokio::test]
    async fn bad_inputs_are_errors() {
        let (_, edges) = people();
        let duplicate = batch(&[("id", vec![Some(10), Some(10)])]);
        let engine = arrow_engine(
            person_config(),
            vec![("Person", duplicate), ("KNOWS", edges.clone())],
        )
        .await;
        let err = engine.export_graph(None, true, None).await.unwrap_err();
        assert!(matches!(err, GraphError::ExecutionError { .. }), "{err}");

        let (nodes, _) = people();
        let engine = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let err = engine
            .export_graph(Some(&["LIKES".to_string()]), true, None)
            .await
            .unwrap_err();
        assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");

        let names = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let config = GraphConfig::builder()
            .node_table("Tag", "id")
            .rel_table("TAGGED", "Tag", "Tag", "src", "dst")
            .build()
            .unwrap();
        let engine = arrow_engine(
            config,
            vec![
                ("Tag", names.clone()),
                ("TAGGED", batch(&[("src", vec![]), ("dst", vec![])])),
            ],
        )
        .await;
        let err = engine.export_graph(None, true, None).await.unwrap_err();
        assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");
    }

    async fn write(uri: &str, batch: RecordBatch, mode: WriteMode) {
        let schema = batch.schema();
        let params = WriteParams {
            mode,
            ..Default::default()
        };
        Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema),
            uri,
            Some(params),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn lance_input_matches_arrow_and_follows_checkout_latest() {
        let dir = tempfile::tempdir().unwrap();
        let uri = |name: &str| {
            dir.path()
                .join(format!("{name}.lance"))
                .display()
                .to_string()
        };
        let (nodes, edges) = people();
        write(&uri("Person"), nodes.clone(), WriteMode::Create).await;
        write(&uri("KNOWS"), edges.clone(), WriteMode::Create).await;
        let mut sources = HashMap::new();
        for name in ["Person", "KNOWS"] {
            let source = EngineSource::open_lance(uri(name), None, HashMap::new())
                .await
                .unwrap();
            sources.insert(name.to_string(), source);
        }
        let lance = CypherEngine::new(person_config(), sources, true)
            .await
            .unwrap();
        let arrow = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let (a, b) = (
            lance.export_graph(None, true, None).await.unwrap(),
            arrow.export_graph(None, true, None).await.unwrap(),
        );
        assert_eq!(a.node_keys, b.node_keys);
        assert_eq!(
            (&a.out_indptr, &a.out_indices),
            (&b.out_indptr, &b.out_indices)
        );
        assert_eq!((&a.in_indptr, &a.in_indices), (&b.in_indptr, &b.in_indices));

        // 40 -> 60 is invisible until checkout_latest().
        write(
            &uri("KNOWS"),
            batch(&[("src", vec![Some(40)]), ("dst", vec![Some(60)])]),
            WriteMode::Append,
        )
        .await;
        assert_eq!(
            values(
                &lance
                    .export_graph(None, true, None)
                    .await
                    .unwrap()
                    .out_indptr
            )[6],
            6
        );
        lance.checkout_latest().await.unwrap();
        let g = lance.export_graph(None, true, None).await.unwrap();
        assert_eq!(values(&g.out_indptr), vec![0, 1, 2, 3, 5, 6, 7]);
        assert_eq!(values(&g.out_indices)[6], 0);
    }

    #[tokio::test]
    async fn write_node_column_merges_by_key_and_repins() {
        let dir = tempfile::tempdir().unwrap();
        let uri = |name: &str| {
            dir.path()
                .join(format!("{name}.lance"))
                .display()
                .to_string()
        };
        let (nodes, edges) = people();
        write(&uri("Person"), nodes.clone(), WriteMode::Create).await;
        write(&uri("KNOWS"), edges.clone(), WriteMode::Create).await;
        let mut sources = HashMap::new();
        for name in ["Person", "KNOWS"] {
            let source = EngineSource::open_lance(uri(name), None, HashMap::new())
                .await
                .unwrap();
            sources.insert(name.to_string(), source);
        }
        let engine = CypherEngine::new(person_config(), sources, true)
            .await
            .unwrap();
        let g = engine.export_graph(None, true, None).await.unwrap();
        let degree = |g: &GraphExport| -> ArrayRef {
            let p = g.out_indptr.values();
            Arc::new(Int64Array::from_iter_values(
                p.windows(2).map(|w| (w[1] - w[0]) as i64),
            ))
        };
        let query = "MATCH (p:Person) RETURN p.id AS id, p.deg AS deg ORDER BY id";
        let read = |b: RecordBatch| -> Vec<(i64, Option<i64>)> {
            let (id, deg) = (
                b.column(0).as_primitive::<Int64Type>(),
                b.column(1).as_primitive::<Int64Type>(),
            );
            (0..b.num_rows())
                .map(|i| (id.value(i), deg.is_valid(i).then(|| deg.value(i))))
                .collect()
        };

        let before = engine.dataset_versions()["KNOWS"];
        engine
            .write_node_column(&g, "deg", &degree(&g), false)
            .await
            .unwrap();
        let rows = engine.execute(query, HashMap::new()).await.unwrap();
        assert_eq!(
            read(rows),
            [(10, 1), (20, 1), (30, 2), (40, 0), (50, 1), (60, 1)]
                .map(|(k, d)| (k, Some(d)))
                .to_vec()
        );
        // Only the node table got a new version.
        assert_eq!(engine.dataset_versions()["KNOWS"], before);

        // A second write needs overwrite.
        let err = engine
            .write_node_column(&g, "DEG", &degree(&g), false)
            .await
            .unwrap_err();
        assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");
        let zeros: ArrayRef = Arc::new(Int64Array::from(vec![0; 6]));
        engine
            .write_node_column(&g, "deg", &zeros, true)
            .await
            .unwrap();
        let rows = engine.execute(query, HashMap::new()).await.unwrap();
        assert!(read(rows).iter().all(|(_, d)| *d == Some(0)));

        // The key column, a wrong length and in-memory data are refused.
        for (column, values) in [("id", zeros.clone()), ("x", zeros.slice(0, 5))] {
            let err = engine
                .write_node_column(&g, column, &values, true)
                .await
                .unwrap_err();
            assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");
        }
        let arrow = arrow_engine(person_config(), vec![("Person", nodes), ("KNOWS", edges)]).await;
        let err = arrow
            .write_node_column(&g, "deg", &zeros, false)
            .await
            .unwrap_err();
        assert!(matches!(err, GraphError::ConfigError { .. }), "{err}");
    }

    #[tokio::test]
    async fn optimize_datasets_compacts_cleans_up_and_repins() {
        let dir = tempfile::tempdir().unwrap();
        let uri = |name: &str| {
            dir.path()
                .join(format!("{name}.lance"))
                .display()
                .to_string()
        };
        let (nodes, edges) = people();
        write(&uri("Person"), nodes, WriteMode::Create).await;
        write(&uri("KNOWS"), edges, WriteMode::Create).await;
        // Small appends: several fragments in each table.
        for key in [70, 80] {
            write(
                &uri("Person"),
                batch(&[("id", vec![Some(key)])]),
                WriteMode::Append,
            )
            .await;
            write(
                &uri("KNOWS"),
                batch(&[("src", vec![Some(key)]), ("dst", vec![Some(10)])]),
                WriteMode::Append,
            )
            .await;
        }
        let mut sources = HashMap::new();
        for name in ["Person", "KNOWS"] {
            let source = EngineSource::open_lance(uri(name), None, HashMap::new())
                .await
                .unwrap();
            sources.insert(name.to_string(), source);
        }
        let engine = CypherEngine::new(person_config(), sources, true)
            .await
            .unwrap();
        let g = engine.export_graph(None, true, None).await.unwrap();
        let ones: ArrayRef = Arc::new(Int64Array::from(vec![1; g.num_nodes as usize]));
        engine
            .write_node_column(&g, "x", &ones, false)
            .await
            .unwrap();
        engine
            .write_node_column(&g, "x", &ones, true)
            .await
            .unwrap();

        let query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n, sum(a.x) AS s";
        let before = engine.execute(query, HashMap::new()).await.unwrap();
        let stats = engine
            .optimize_datasets(std::time::Duration::ZERO)
            .await
            .unwrap();
        for name in ["Person", "KNOWS"] {
            let s = &stats[name];
            assert_eq!((s.fragments_removed, s.fragments_added), (3, 1), "{name}");
            assert!(s.old_versions > 0 && s.bytes_removed > 0, "{name}");
            let ds = Dataset::open(&uri(name)).await.unwrap();
            assert_eq!(ds.versions().await.unwrap().len(), 1, "{name}");
            assert_eq!(ds.get_fragments().len(), 1, "{name}");
            assert_eq!(
                engine.dataset_versions()[name],
                ds.version().version,
                "{name}"
            );
        }
        // The engine reads the compacted files, with the CSR, and gives the same answer.
        assert_eq!(engine.execute(query, HashMap::new()).await.unwrap(), before);

        // Nothing left to do: no compaction, no versions removed.
        let again = engine
            .optimize_datasets(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert!(
            again
                .values()
                .all(|s| *s == DatasetOptimizeStats::default()),
            "{again:?}"
        );
    }
}
