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
//! ```
//!
//! For vertex `v`, its neighbors are `neighbors[offsets[v]..offsets[v+1]]`.

use std::collections::HashMap;

/// In-memory CSR adjacency index for fast neighbor lookup.
///
/// Stores graph topology in two arrays:
/// - `offsets[v]` = start position of vertex v's neighbors in the neighbor array
/// - `neighbors[offsets[v]..offsets[v+1]]` = destination vertex IDs
#[derive(Debug, Clone)]
pub struct CsrIndex {
    offsets: Vec<u64>,
    neighbors: Vec<u64>,
}

impl CsrIndex {
    /// Look up all neighbors of a vertex. Returns an empty slice for vertices
    /// with no outgoing edges or vertex IDs beyond the index range.
    pub fn neighbors(&self, vertex_id: u64) -> &[u64] {
        let v = vertex_id as usize;
        if v >= self.offsets.len() - 1 {
            return &[];
        }
        let start = self.offsets[v] as usize;
        let end = self.offsets[v + 1] as usize;
        &self.neighbors[start..end]
    }

    /// Return the total number of edges in the index.
    pub fn num_edges(&self) -> u64 {
        self.neighbors.len() as u64
    }
}

/// Builder for constructing a CSR index from edge data.
///
/// Accepts edges as (source, destination) pairs and builds the compressed
/// sparse row representation.
#[derive(Debug)]
pub struct CsrIndexBuilder {
    edges: Vec<(u64, u64)>,
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

    /// Add a single directed edge from `src` to `dst`.
    pub fn add_edge(mut self, src: u64, dst: u64) -> Self {
        self.edges.push((src, dst));
        self
    }

    /// Build the CSR index.
    ///
    /// Sorts edges by source vertex, then builds offset and neighbor arrays.
    pub fn build(mut self) -> CsrIndex {
        let num_vertices = self.num_vertices.unwrap_or_else(|| {
            self.edges
                .iter()
                .flat_map(|&(s, d)| [s, d])
                .max()
                .map(|m| m + 1)
                .unwrap_or(0)
        });

        // Sort by source vertex for CSR construction
        self.edges.sort_unstable_by_key(|&(src, _)| src);

        // Build offset and neighbor arrays
        let mut offsets = vec![0u64; num_vertices as usize + 1];
        let mut neighbors = Vec::with_capacity(self.edges.len());

        // Count degrees
        let mut degree_map: HashMap<u64, u64> = HashMap::new();
        for &(src, _) in &self.edges {
            *degree_map.entry(src).or_insert(0) += 1;
        }

        // Build prefix-sum offsets
        let mut running = 0u64;
        for v in 0..num_vertices {
            offsets[v as usize] = running;
            running += degree_map.get(&v).copied().unwrap_or(0);
        }
        offsets[num_vertices as usize] = running;

        // Fill neighbor array
        for &(_, dst) in &self.edges {
            neighbors.push(dst);
        }

        CsrIndex { offsets, neighbors }
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
            .add_edge(0, 1)
            .add_edge(0, 2)
            .add_edge(0, 3)
            .add_edge(1, 2)
            .add_edge(2, 3)
            .build()
    }

    #[test]
    fn test_basic_neighbor_lookup() {
        let idx = sample_index();

        assert_eq!(idx.neighbors(0), &[1, 2, 3]);
        assert_eq!(idx.neighbors(1), &[2]);
        assert_eq!(idx.neighbors(2), &[3]);
        assert_eq!(idx.neighbors(3), &[] as &[u64]);
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
            .add_edge(1, 3)
            .build();

        assert_eq!(idx.neighbors(0), &[] as &[u64]);
        assert_eq!(idx.neighbors(1), &[3]);
        assert_eq!(idx.neighbors(2), &[] as &[u64]);
        assert_eq!(idx.neighbors(3), &[] as &[u64]);
        assert_eq!(idx.neighbors(4), &[] as &[u64]);
    }

    #[test]
    fn test_auto_inferred_num_vertices() {
        let idx = CsrIndexBuilder::new().add_edge(0, 5).add_edge(3, 7).build();

        // Inferred from the max ID (7): vertices up to 7 are addressable.
        assert_eq!(idx.neighbors(7), &[] as &[u64]);
        assert_eq!(idx.neighbors(0), &[5]);
        assert_eq!(idx.neighbors(3), &[7]);
    }

    #[test]
    fn test_self_loops() {
        let idx = CsrIndexBuilder::new()
            .with_num_vertices(3)
            .add_edge(0, 0)
            .add_edge(1, 1)
            .add_edge(0, 1)
            .build();

        assert_eq!(idx.neighbors(0), &[0, 1]);
        assert_eq!(idx.neighbors(1), &[1]);
    }

    #[test]
    fn test_parallel_edges() {
        let idx = CsrIndexBuilder::new()
            .with_num_vertices(2)
            .add_edge(0, 1)
            .add_edge(0, 1)
            .add_edge(0, 1)
            .build();

        // CSR preserves multi-edges
        assert_eq!(idx.neighbors(0), &[1, 1, 1]);
    }
}
