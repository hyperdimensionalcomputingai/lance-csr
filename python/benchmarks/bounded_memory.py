# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Peak memory of streamed Cypher scans over Lance data larger than RAM limits.

``generate`` writes a deterministic Person table with a wide unused ``payload``
column, in batches so the generator itself stays small. ``run`` executes a query in
a fresh subprocess, consumes ``execute_batches()`` incrementally (count + checksum,
nothing retained), and reports peak RSS from ``getrusage``, which includes native
allocations. Apply the memory cap outside, e.g. ``docker run --memory=2g``.

    python benchmarks/bounded_memory.py generate --out /data/large --rows 2000000
    python benchmarks/bounded_memory.py run --data /data/large --query scan
"""

import argparse
import json
import resource
import subprocess
import sys
import time
from pathlib import Path

PAYLOAD_BYTES = 4096
QUERIES = {
    # Narrow projection: only `id` should be read from storage.
    "scan": "MATCH (p:Person) RETURN p.id",
    # Selective graph join; the KNOWS side is filtered before the join.
    "join": "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id < 1000 RETURN b.id",
}


def generate(out: Path, rows: int, batch_rows: int = 16_384) -> None:
    import lance
    import numpy as np
    import pyarrow as pa

    rng = np.random.default_rng(42)
    schema = pa.schema([("id", pa.int64()), ("payload", pa.binary(PAYLOAD_BYTES))])

    def person_batches():
        for start in range(0, rows, batch_rows):
            n = min(batch_rows, rows - start)
            # Random bytes so storage compression cannot shrink the input.
            payload = rng.integers(0, 256, n * PAYLOAD_BYTES, dtype=np.uint8)
            yield pa.record_batch(
                [
                    pa.array(np.arange(start, start + n, dtype=np.int64)),
                    pa.FixedSizeBinaryArray.from_buffers(
                        pa.binary(PAYLOAD_BYTES), n, [None, pa.py_buffer(payload)]
                    ),
                ],
                schema=schema,
            )

    reader = pa.RecordBatchReader.from_batches(schema, person_batches())
    lance.write_dataset(reader, out / "person.lance", mode="overwrite")
    ids = np.arange(rows, dtype=np.int64)
    knows = pa.table({"src": ids, "dst": (ids + 1) % rows})
    lance.write_dataset(knows, out / "knows.lance", mode="overwrite")
    logical = rows * (8 + PAYLOAD_BYTES)
    print(json.dumps({"rows": rows, "logical_input_bytes": logical}))


def measure(data: Path, query: str) -> dict:
    import lance
    import pyarrow.compute as pc
    from lance_csr import CypherEngine, GraphConfig

    config = (
        GraphConfig.builder()
        .with_node_label("Person", "id")
        .with_relationship("KNOWS", "src", "dst")
        .build()
    )
    start = time.perf_counter()
    engine = CypherEngine(
        config,
        {
            "Person": lance.dataset(data / "person.lance"),
            "KNOWS": lance.dataset(data / "knows.lance"),
        },
    )
    reader = engine.execute_batches(QUERIES[query])
    rows = checksum = batches = 0
    first_batch_s = None
    for batch in reader:
        if first_batch_s is None:
            first_batch_s = time.perf_counter() - start
        batches += 1
        rows += batch.num_rows
        checksum += pc.sum(batch.column(0)).as_py() or 0
    elapsed = time.perf_counter() - start
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB, macOS reports bytes.
    peak_bytes = peak if sys.platform == "darwin" else peak * 1024
    input_rows = lance.dataset(data / "person.lance").count_rows()
    return {
        "query": query,
        "input_rows": input_rows,
        "output_rows": rows,
        "checksum": checksum,
        "batches": batches,
        "time_to_first_batch_s": first_batch_s,
        "elapsed_s": elapsed,
        "peak_rss_bytes": peak_bytes,
        "plan_scan": next(
            (
                line.strip()
                for line in engine.explain(QUERIES[query]).splitlines()
                if "LanceRead" in line and "person" in line
            ),
            None,
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    gen = sub.add_parser("generate")
    gen.add_argument("--out", type=Path, required=True)
    gen.add_argument("--rows", type=int, required=True)
    run = sub.add_parser("run")
    run.add_argument("--data", type=Path, required=True)
    run.add_argument("--query", choices=QUERIES, default="scan")
    child = sub.add_parser("child")
    child.add_argument("--data", type=Path, required=True)
    child.add_argument("--query", choices=QUERIES, default="scan")
    args = parser.parse_args()

    if args.cmd == "generate":
        args.out.mkdir(parents=True, exist_ok=True)
        generate(args.out, args.rows)
    elif args.cmd == "child":
        print(json.dumps(measure(args.data, args.query)))
    else:
        # Fresh interpreter per measurement so peak RSS reflects only this query.
        cmd = [sys.executable, __file__, "child", "--data", str(args.data)]
        proc = subprocess.run(
            [*cmd, "--query", args.query], capture_output=True, text=True
        )
        result = {"returncode": proc.returncode}
        if proc.returncode == 0:
            result.update(json.loads(proc.stdout.strip().splitlines()[-1]))
        else:
            result["stderr_tail"] = proc.stderr[-2000:]
        print(json.dumps(result))


if __name__ == "__main__":
    main()
