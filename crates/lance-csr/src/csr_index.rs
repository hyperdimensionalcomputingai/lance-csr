// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! CSR (Compressed Sparse Row) adjacency index for native graph traversal
//!
//! The compact neighbour store behind [`crate::csr_expand::Adjacency`]. Upstream's
//! traversal (BFS, shortest path) and Arrow export helpers were removed as unused
//! in this fork (shortest paths now live in [`crate::shortest_path`]).
//!
//! # Layout
//!
//! ```text
//! Offset Array:  [0, 3, 5, 5, 9, ...]   (one entry per vertex + 1 sentinel)
//! Neighbor Array: [2, 5, 7, 1, 4, 0, 3, 6, 8, ...]  (destination vertex IDs)
//! Edge rows:      [...]                            (one source row per neighbour)
//! ```
//!
//! For vertex `v`, its neighbors are `neighbors[offsets[v]..offsets[v+1]]`, and
//! `edge_rows` at the same positions holds the row each edge came from.
//!
//! Edge rows are stored packed ([`PackedU64`]): in blocks of 128 values, each held as
//! its offset from the block's minimum in as few bits as the block needs. They are
//! Lance row addresses, so a single width for the whole array would waste most of the
//! saving; per-block widths keep any position directly readable, which traversal
//! needs to resume part-way through a list. Neighbours stay a plain array: every hop
//! reads them, and packing them slowed lookup-heavy queries (custom Q30 by about 22%)
//! for a smaller saving than edge rows give.

use std::collections::HashMap;
use std::ops::Range;

use arrow::buffer::ScalarBuffer;

/// Values per block of a [`PackedU64`].
const BLOCK: usize = 128;

/// A `u64` array packed block by block, with direct access to any position.
///
/// Each block of [`BLOCK`] values stores its minimum (`bases`) and its start bit
/// and width (`meta`: start bit << 7 | width); the values are bit-packed offsets
/// from the minimum, back to back in `words`. A block whose values are all equal
/// takes no bits at all.
#[derive(Debug, Clone)]
pub struct PackedU64 {
    len: usize,
    bases: ScalarBuffer<u64>,
    meta: ScalarBuffer<u64>,
    /// One spare word at the end, so reading two words never runs past it.
    words: ScalarBuffer<u64>,
}

impl PackedU64 {
    pub fn pack(values: &[u64]) -> Self {
        let blocks = values.len().div_ceil(BLOCK);
        let mut bases = Vec::with_capacity(blocks);
        let mut meta = Vec::with_capacity(blocks);
        let mut widths = Vec::with_capacity(blocks);
        let mut bits = 0u64;
        for block in values.chunks(BLOCK) {
            let min = *block.iter().min().expect("chunks are not empty");
            let max = *block.iter().max().expect("chunks are not empty");
            let width = u64::from(64 - (max - min).leading_zeros());
            bases.push(min);
            meta.push(bits << 7 | width);
            widths.push(width);
            bits += width * block.len() as u64;
        }
        let mut words = vec![0u64; bits.div_ceil(64) as usize + 1];
        for ((block, &min), (&m, &width)) in values
            .chunks(BLOCK)
            .zip(&bases)
            .zip(meta.iter().zip(&widths))
        {
            let mut bit = m >> 7;
            for &value in block {
                write_bits(&mut words, bit, width as u32, value - min);
                bit += width;
            }
        }
        Self {
            len: values.len(),
            bases: bases.into(),
            meta: meta.into(),
            words: words.into(),
        }
    }

    /// A packed array from [`Self::parts`] and its length, or `None` unless every
    /// block header is consistent with `len` and the words hold exactly its bits.
    pub fn from_parts(len: usize, [bases, meta, words]: [ScalarBuffer<u64>; 3]) -> Option<Self> {
        let blocks = len.div_ceil(BLOCK);
        if bases.len() != blocks || meta.len() != blocks {
            return None;
        }
        let mut bits = 0u64;
        for (block, &m) in meta.iter().enumerate() {
            let (start, width) = split_meta(m);
            if start != bits || width > 64 {
                return None;
            }
            bits += u64::from(width) * (len - block * BLOCK).min(BLOCK) as u64;
        }
        (words.len() as u64 == bits.div_ceil(64) + 1).then_some(Self {
            len,
            bases,
            meta,
            words,
        })
    }

    /// The block bases, block headers and words, for storing.
    pub fn parts(&self) -> [ScalarBuffer<u64>; 3] {
        [self.bases.clone(), self.meta.clone(), self.words.clone()]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Value at `index`.
    ///
    /// # Panics
    /// If `index` is out of bounds.
    #[inline]
    pub fn get(&self, index: usize) -> u64 {
        assert!(index < self.len, "index {index} out of bounds");
        let block = index / BLOCK;
        let (start, width) = split_meta(self.meta[block]);
        let bit = start + (index % BLOCK) as u64 * u64::from(width);
        self.bases[block].wrapping_add(read_bits(&self.words, bit, width))
    }

    /// The values at `range`, in order, reading each block's header once.
    #[inline]
    pub fn iter(&self, range: Range<usize>) -> PackedIter<'_> {
        assert!(range.end <= self.len, "range {range:?} out of bounds");
        PackedIter {
            packed: self,
            position: range.start,
            end: range.end.max(range.start),
            block_end: range.start,
            bit: 0,
            width: 0,
            base: 0,
        }
    }

    /// Every value, unpacked.
    pub fn to_vec(&self) -> Vec<u64> {
        self.iter(0..self.len).collect()
    }

    /// Bytes held by the packed arrays.
    pub fn size_bytes(&self) -> usize {
        8 * (self.bases.len() + self.meta.len() + self.words.len())
    }
}

#[inline(always)]
fn split_meta(meta: u64) -> (u64, u32) {
    (meta >> 7, (meta & 0x7f) as u32)
}

#[inline(always)]
fn read_bits(words: &[u64], bit: u64, width: u32) -> u64 {
    if width == 0 {
        return 0;
    }
    let word = (bit >> 6) as usize;
    let shift = (bit & 63) as u32;
    let mut value = words[word] >> shift;
    if shift + width > 64 {
        value |= words[word + 1] << (64 - shift);
    }
    if width < 64 {
        value & ((1u64 << width) - 1)
    } else {
        value
    }
}

fn write_bits(words: &mut [u64], bit: u64, width: u32, value: u64) {
    if width == 0 {
        return;
    }
    let word = (bit >> 6) as usize;
    let shift = (bit & 63) as u32;
    words[word] |= value << shift;
    if shift + width > 64 {
        words[word + 1] |= value >> (64 - shift);
    }
}

/// Iterator over a range of a [`PackedU64`]; see [`PackedU64::iter`].
pub struct PackedIter<'a> {
    packed: &'a PackedU64,
    position: usize,
    end: usize,
    /// Where the block loaded into `bit`, `width` and `base` ends.
    block_end: usize,
    bit: u64,
    width: u32,
    base: u64,
}

impl Iterator for PackedIter<'_> {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        if self.position == self.end {
            return None;
        }
        if self.position == self.block_end {
            let block = self.position / BLOCK;
            let (start, width) = split_meta(self.packed.meta[block]);
            self.width = width;
            self.bit = start + (self.position % BLOCK) as u64 * u64::from(width);
            self.base = self.packed.bases[block];
            self.block_end = ((block + 1) * BLOCK).min(self.packed.len);
        }
        let value = read_bits(&self.packed.words, self.bit, self.width);
        self.bit += u64::from(self.width);
        self.position += 1;
        Some(self.base.wrapping_add(value))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.end - self.position;
        (n, Some(n))
    }
}

impl ExactSizeIterator for PackedIter<'_> {}

/// In-memory CSR adjacency index for fast neighbor lookup.
///
/// Stores graph topology in three arrays:
/// - `offsets[v]` = start position of vertex v's neighbors in the neighbor array
/// - `neighbors[offsets[v]..offsets[v+1]]` = destination vertex IDs
/// - `edge_rows`, aligned with `neighbors` = the row (a Lance row address, or a
///   row index for in-memory input) each edge came from
///
/// Edge rows are packed; see the module docs.
#[derive(Debug, Clone)]
pub struct CsrIndex {
    offsets: ScalarBuffer<u64>,
    neighbors: ScalarBuffer<u64>,
    edge_rows: PackedU64,
}

impl CsrIndex {
    /// An index from its three arrays, as [`Self::packed`] returns them, or `None`
    /// if `offsets` is empty, descends or doesn't end at `neighbors.len()`, or
    /// `edge_rows` and `neighbors` differ in length.
    pub fn from_packed(
        offsets: ScalarBuffer<u64>,
        neighbors: ScalarBuffer<u64>,
        edge_rows: PackedU64,
    ) -> Option<Self> {
        let well_formed = offsets.first() == Some(&0)
            && offsets.last() == Some(&(neighbors.len() as u64))
            && offsets.windows(2).all(|w| w[0] <= w[1])
            && edge_rows.len() == neighbors.len();
        well_formed.then_some(Self {
            offsets,
            neighbors,
            edge_rows,
        })
    }

    /// The offsets, the neighbours and the packed edge rows.
    pub fn packed(&self) -> (&ScalarBuffer<u64>, &ScalarBuffer<u64>, &PackedU64) {
        (&self.offsets, &self.neighbors, &self.edge_rows)
    }

    /// Positions of a vertex's neighbours; empty for vertices with no edges or
    /// beyond the index range.
    #[inline]
    pub fn range(&self, vertex_id: u64) -> Range<usize> {
        let v = vertex_id as usize;
        if v >= self.offsets.len() - 1 {
            return 0..0;
        }
        self.offsets[v] as usize..self.offsets[v + 1] as usize
    }

    /// The neighbours at `positions`, a sub-range of one or more [`Self::range`]s.
    #[inline]
    pub fn neighbors_at(&self, positions: Range<usize>) -> &[u64] {
        &self.neighbors[positions]
    }

    /// The edge rows at `positions`, aligned with [`Self::neighbors_at`].
    #[inline]
    pub fn edge_rows_at(&self, positions: Range<usize>) -> PackedIter<'_> {
        self.edge_rows.iter(positions)
    }

    /// All neighbours of a vertex; empty for vertices with no edges or beyond the
    /// index range.
    pub fn neighbors(&self, vertex_id: u64) -> &[u64] {
        self.neighbors_at(self.range(vertex_id))
    }

    /// The edge rows of a vertex's neighbours, in the same order as [`Self::neighbors`].
    pub fn edge_rows(&self, vertex_id: u64) -> PackedIter<'_> {
        self.edge_rows_at(self.range(vertex_id))
    }

    /// Return the total number of edges in the index.
    pub fn num_edges(&self) -> u64 {
        self.neighbors.len() as u64
    }

    /// Bytes held by the offsets, neighbours and edge rows.
    pub fn size_bytes(&self) -> usize {
        8 * (self.offsets.len() + self.neighbors.len()) + self.edge_rows.size_bytes()
    }
}

/// Builder for constructing a CSR index from edge data.
///
/// Accepts edges as (source, destination, edge row) triples and builds the
/// compressed sparse row representation.
#[derive(Debug)]
pub struct CsrIndexBuilder {
    edges: Vec<(u64, u64, u64)>,
    num_vertices: Option<u64>,
}

impl CsrIndexBuilder {
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            num_vertices: None,
        }
    }

    /// Set the total number of vertices explicitly. If not set, it is inferred
    /// from the maximum vertex ID seen in the edges.
    pub fn with_num_vertices(mut self, n: u64) -> Self {
        self.num_vertices = Some(n);
        self
    }

    /// Add a single directed edge from `src` to `dst`, read from row `row`.
    pub fn add_edge(mut self, src: u64, dst: u64, row: u64) -> Self {
        self.edges.push((src, dst, row));
        self
    }

    /// Build the CSR index.
    ///
    /// Sorts edges by source vertex, then builds offset and neighbor arrays.
    pub fn build(mut self) -> CsrIndex {
        let num_vertices = self.num_vertices.unwrap_or_else(|| {
            self.edges
                .iter()
                .flat_map(|&(s, d, _)| [s, d])
                .max()
                .map(|m| m + 1)
                .unwrap_or(0)
        });

        // Sort by source vertex for CSR construction
        self.edges.sort_unstable_by_key(|&(src, _, _)| src);

        // Build offset and neighbor arrays
        let mut offsets = vec![0u64; num_vertices as usize + 1];
        let mut neighbors = Vec::with_capacity(self.edges.len());

        // Count degrees
        let mut degree_map: HashMap<u64, u64> = HashMap::new();
        for &(src, _, _) in &self.edges {
            *degree_map.entry(src).or_insert(0) += 1;
        }

        // Build prefix-sum offsets
        let mut running = 0u64;
        for v in 0..num_vertices {
            offsets[v as usize] = running;
            running += degree_map.get(&v).copied().unwrap_or(0);
        }
        offsets[num_vertices as usize] = running;

        let edge_rows: Vec<u64> = self.edges.iter().map(|&(_, _, row)| row).collect();
        neighbors.extend(self.edges.iter().map(|&(_, dst, _)| dst));
        // Freed before packing, so the peak holds the unpacked rows or the edges, not both.
        drop(std::mem::take(&mut self.edges));

        CsrIndex {
            offsets: offsets.into(),
            neighbors: neighbors.into(),
            edge_rows: PackedU64::pack(&edge_rows),
        }
    }
}

impl Default for CsrIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test graph:
    //   0 -> 1, 2, 3
    //   1 -> 2
    //   2 -> 3
    //   3 -> (none)
    fn sample_index() -> CsrIndex {
        CsrIndexBuilder::new()
            .with_num_vertices(4)
            .add_edge(0, 1, 0)
            .add_edge(0, 2, 1)
            .add_edge(0, 3, 2)
            .add_edge(1, 2, 3)
            .add_edge(2, 3, 4)
            .build()
    }

    #[test]
    fn test_basic_neighbor_lookup() {
        let idx = sample_index();

        assert_eq!(idx.neighbors(0), &[1, 2, 3]);
        assert_eq!(idx.neighbors(1), &[2]);
        assert_eq!(idx.neighbors(2), &[3]);
        assert_eq!(idx.neighbors(3), &[] as &[u64]);
        assert_eq!(idx.edge_rows(0).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(idx.edge_rows(2).collect::<Vec<_>>(), [4]);
        assert_eq!(idx.edge_rows(99).collect::<Vec<_>>(), Vec::<u64>::new());
    }

    #[test]
    fn test_metadata() {
        let idx = sample_index();
        assert_eq!(idx.num_edges(), 5);
    }

    #[test]
    fn test_out_of_range_vertex() {
        let idx = sample_index();
        assert_eq!(idx.neighbors(99), &[] as &[u64]);
    }

    #[test]
    fn test_empty_graph() {
        let idx = CsrIndexBuilder::new().with_num_vertices(0).build();
        assert_eq!(idx.num_edges(), 0);
        assert_eq!(idx.neighbors(0), &[] as &[u64]);
    }

    #[test]
    fn test_isolated_vertices() {
        let idx = CsrIndexBuilder::new()
            .with_num_vertices(5)
            .add_edge(1, 3, 5)
            .build();

        assert_eq!(idx.neighbors(0), &[] as &[u64]);
        assert_eq!(idx.neighbors(1), &[3]);
        assert_eq!(idx.neighbors(2), &[] as &[u64]);
        assert_eq!(idx.neighbors(3), &[] as &[u64]);
        assert_eq!(idx.neighbors(4), &[] as &[u64]);
    }

    #[test]
    fn test_auto_inferred_num_vertices() {
        let idx = CsrIndexBuilder::new()
            .add_edge(0, 5, 6)
            .add_edge(3, 7, 7)
            .build();

        // Inferred from the max ID (7): vertices up to 7 are addressable.
        assert_eq!(idx.neighbors(7), &[] as &[u64]);
        assert_eq!(idx.neighbors(0), &[5]);
        assert_eq!(idx.neighbors(3), &[7]);
    }

    #[test]
    fn test_self_loops() {
        let idx = CsrIndexBuilder::new()
            .with_num_vertices(3)
            .add_edge(0, 0, 8)
            .add_edge(1, 1, 9)
            .add_edge(0, 1, 10)
            .build();

        assert_eq!(idx.neighbors(0), &[0, 1]);
        assert_eq!(idx.neighbors(1), &[1]);
    }

    #[test]
    fn packed_values_round_trip() {
        // Constant blocks, full 64-bit spans, values straddling words and a short
        // last block.
        let mut values: Vec<u64> = vec![7; 130];
        values.extend([0, u64::MAX, 1 << 63, 3]);
        values.extend((0..300u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (i % 60)));
        values.extend((0..200u64).map(|i| 2_199_023_255_552 + i * 37));
        let packed = PackedU64::pack(&values);
        assert_eq!(packed.to_vec(), values);
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(packed.get(i), v, "position {i}");
        }
        for start in [0, 1, 127, 128, 129, 300, values.len()] {
            let got: Vec<u64> = packed.iter(start..values.len()).collect();
            assert_eq!(got, values[start..], "from {start}");
        }
        assert_eq!(packed.iter(5..5).count(), 0);
        assert!(packed.size_bytes() < 8 * values.len());
        assert!(PackedU64::pack(&[]).is_empty());

        // Parts round-trip, and inconsistent ones are refused.
        let again = PackedU64::from_parts(values.len(), packed.parts()).unwrap();
        assert_eq!(again.to_vec(), values);
        assert!(PackedU64::from_parts(values.len() + 128, packed.parts()).is_none());
        let [bases, meta, words] = packed.parts();
        let short = words.slice(0, words.len() - 1);
        assert!(
            PackedU64::from_parts(values.len(), [bases.clone(), meta.clone(), short]).is_none()
        );
        let mut bad_meta = meta.to_vec();
        bad_meta[1] += 1 << 7;
        assert!(PackedU64::from_parts(values.len(), [bases, bad_meta.into(), words]).is_none());
    }

    #[test]
    fn test_parallel_edges() {
        let idx = CsrIndexBuilder::new()
            .with_num_vertices(2)
            .add_edge(0, 1, 11)
            .add_edge(0, 1, 12)
            .add_edge(0, 1, 13)
            .build();

        // CSR preserves multi-edges
        assert_eq!(idx.neighbors(0), &[1, 1, 1]);
    }
}
