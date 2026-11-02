# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Engine start-up with a stored CSR (``persist_csr=True``) against building it.

For each edge count, writes a deterministic random graph to Lance (nodes =
edges / AVG_DEGREE), then runs each case in a fresh subprocess on its own copy
of the relationship dataset:

- ``off``: ``persist_csr=False``, today's build;
- ``first``: ``persist_csr=True`` with nothing stored, so a build plus the write;
- ``loaded``: ``persist_csr=True`` with the file from ``first``;
- ``loaded_delta``: as ``loaded``, after appending 1% more edges and deleting about
  1% of the rows, so the changes since the stored version apply as a delta.

Each case reports construction time, the CSR's ``build_ms`` and ``update``, peak
RSS, the size of ``_lance_csr/`` and warm latency of a few queries, whose row
counts and checksums must match across cases.

    python benchmarks/csr_persist.py --data /tmp/csr-persist --edges 1000000 10000000
"""

import argparse
import json
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

from csr_expand import AVG_DEGREE, generate, summary

CASES = ("off", "first", "loaded", "loaded_delta")
QUERIES = {
    "point": "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 42 RETURN b.id",
    "two_hop": (
        "MATCH (a:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(c:Person) "
        "WHERE a.id < 100 RETURN count(c) AS n"
    ),
    "all_edges": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n",
}


def peak_rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return (peak if sys.platform == "darwin" else peak * 1024) / 2**20


def folder_mib(path: Path) -> float:
    return sum(f.stat().st_size for f in path.glob("*")) / 2**20 if path.exists() else 0


def child(root: Path, knows: Path, case: str, repeats: int) -> dict:
    import lance
    import pyarrow.compute as pc
    from lance_csr import CypherEngine, GraphConfig

    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .build()
    )
    datasets = {
        "Person": lance.dataset(root / "person.lance"),
        "KNOWS": lance.dataset(knows),
    }
    kwargs = {} if case == "off" else {"persist_csr": True}
    start = time.perf_counter()
    engine = CypherEngine(config, datasets, use_csr=True, **kwargs)
    out = {"case": case, "construct_ms": (time.perf_counter() - start) * 1e3}
    stats = engine.csr_stats()["KNOWS"]
    out |= {k: stats[k] for k in ("update", "build_ms", "edges", "delta_edges")}
    out["node_index_build_ms"] = engine.node_index_stats()["Person"]["build_ms"]
    out["peak_rss_mib"] = peak_rss_mib()
    out["stored_mib"] = folder_mib(knows / "_lance_csr")
    for name, query in QUERIES.items():
        result = engine.execute(query)
        check = {"rows": result.num_rows, "checksum": pc.sum(result.column(0)).as_py()}
        samples = []
        for _ in range(repeats):
            t = time.perf_counter()
            engine.execute(query)
            samples.append(time.perf_counter() - t)
        out[name] = {**check, **summary(samples)}
    return out


def change(knows: Path) -> None:
    """Append 1% more edges and delete about 1% of the rows."""
    import lance
    import numpy as np
    import pyarrow as pa

    ds = lance.dataset(knows)
    rows = ds.count_rows()
    nodes = rows // AVG_DEGREE
    rng = np.random.default_rng(11)
    added = pa.table(
        {
            "src": rng.integers(0, nodes, rows // 100, dtype=np.int64),
            "dst": rng.integers(0, nodes, rows // 100, dtype=np.int64),
        }
    )
    lance.write_dataset(added, knows, mode="append")
    lance.dataset(knows).delete(f"src < {nodes // 100}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--edges", type=int, nargs="+", default=[1_000_000])
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--child", nargs=3, metavar=("ROOT", "KNOWS", "CASE"))
    args = parser.parse_args()

    if args.child:
        root, knows, case = args.child
        print(json.dumps(child(Path(root), Path(knows), case, args.repeats)))
        return

    for edges in args.edges:
        root = args.data / f"e{edges}"
        root.mkdir(parents=True, exist_ok=True)
        generate(root, edges)
        # `first` writes into this copy; `loaded` and `loaded_delta` read it back.
        knows = root / "knows-persist.lance"
        shutil.rmtree(knows, ignore_errors=True)
        shutil.copytree(root / "knows.lance", knows)
        results = {}
        for case in args.cases:
            if case == "loaded_delta":
                change(knows)
            target = root / "knows.lance" if case == "off" else knows
            cmd = [sys.executable, __file__, "--data", str(args.data)]
            cmd += ["--repeats", str(args.repeats), "--child", str(root), str(target), case]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                print(json.dumps({"edges": edges, "case": case, "error": proc.stderr[-2000:]}))
                continue
            results[case] = json.loads(proc.stdout.strip().splitlines()[-1])
            print(json.dumps({"edges": edges, **results[case]}), flush=True)
        same = [c for c in ("off", "first", "loaded") if c in results]
        for name in QUERIES:
            answers = {c: (results[c][name]["rows"], results[c][name]["checksum"]) for c in same}
            assert len(set(answers.values())) <= 1, (edges, name, answers)


if __name__ == "__main__":
    main()
