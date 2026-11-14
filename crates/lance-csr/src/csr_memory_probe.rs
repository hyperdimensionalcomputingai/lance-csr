//! Ignored allocation ledger for issue #79; CSR_MEMORY_DATA points to Lance datasets.
use super::*;

#[tokio::test]
#[ignore]
async fn sf1_memory_ledger() {
    let root = std::env::var("CSR_MEMORY_DATA").expect("CSR_MEMORY_DATA");
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("lance") {
            continue;
        }
        let dataset = Arc::new(Dataset::open(path.to_str().unwrap()).await.unwrap());
        if dataset.schema().field("src").is_none() {
            continue;
        }
        for (from, to) in [("src", "dst"), ("dst", "src")] {
            let mut scan = dataset.scan();
            scan.project(&["src", "dst"]).unwrap();
            scan.with_row_address();
            let adj = Adjacency::build(scan.try_into_stream().await.unwrap(), from, to)
                .await
                .unwrap();
            // Allocated bytes of the source layout: 17 per hash bucket for a
            // per-source entry, 25 for a 64-key bitmap, plus the wide layout's offsets.
            let buckets = |capacity: usize| capacity / 7 * 8;
            let (layout, source_bytes) = match &adj.base.sources {
                Sources::Ranges(map) => ("ranges", buckets(map.capacity()) * 17),
                Sources::Wide { positions, offsets } => (
                    "wide",
                    buckets(positions.capacity()) * 17 + offsets.len() * 8,
                ),
                Sources::Bitmap(bitmaps) => ("bitmap", buckets(bitmaps.capacity()) * 25),
            };
            println!(
                "LEDGER,{},{},{},{},{},{},{}",
                path.file_stem().unwrap().to_str().unwrap(),
                from,
                adj.num_sources(),
                layout,
                source_bytes,
                adj.base.neighbors.len() * 8,
                adj.base.edge_rows.size_bytes(),
            );
        }
    }
}
