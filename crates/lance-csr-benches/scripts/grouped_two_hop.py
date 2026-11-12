"""Profile one generated grouped-count case on macOS or Linux against a release wheel.

Run with the benchmark suite's uv environment and ``uv run --with <wheel> python
<path-to-this-script> balanced a.id``. JSON includes cached execution latency,
process peak RSS before profiling, and a separate EXPLAIN ANALYZE plan. Operator
row counts are approximate because DataFusion prints rounded metric units.
"""

import argparse
import hashlib
import json
import re
import resource
import statistics
import sys
import time

import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig, SqlEngine

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "case",
    choices=["balanced", "skewed", "same_type", "same_end", "low_fanout", "selective"],
)
parser.add_argument("key", choices=["a.id", "b.id", "a.bucket", "b.bucket"])
args = parser.parse_args()
case, key = args.case, args.key
n, d = 1024, 64
if case == "skewed":
    pairs = [p for i in range(n) for p in [(i, 0), (0, i)]]
else:
    if case == "low_fanout":
        d = 1
    pairs = [(i, (i + j) % n) for i in range(n) for j in range(d)]
config = (
    GraphConfig.builder()
    .node_table("N", "id")
    .rel_table("R", "N", "N", "src", "dst")
    .rel_table("S", "N", "N", "src", "dst")
    .build()
)
nodes = pa.table({"id": list(range(n)), "bucket": [i % 8 for i in range(n)]})
edges = pa.table({"src": [a for a, b in pairs], "dst": [b for a, b in pairs]})
e = CypherEngine(config, {"N": nodes, "R": edges, "S": edges}, use_csr=True)
rel = "R" if case in ["same_type", "same_end"] else "S"
hop = f"<-[:{rel}]-" if case == "same_end" else f"-[:{rel}]->"
where = "WHERE a.id = 0 AND c.id = 1" if case == "selective" else ""
q = f"MATCH (a:N)-[:R]->(b:N){hop}(c:N) {where} RETURN {key}, count(*) AS n"
plan = e.explain(q)
start = time.perf_counter()
r = e.execute(q)
first = time.perf_counter() - start
values = []
for i in range(11):
    start = time.perf_counter()
    e.execute(q)
    values.append(time.perf_counter() - start)
rows = sorted(r.to_pylist(), key=lambda r: list(r.values()))
peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
    1 if sys.platform == "darwin" else 1024
)
profile = (
    SqlEngine.from_engine(e)
    .execute("EXPLAIN ANALYZE SELECT * FROM graph_table('" + q + "')")
    .to_pylist()[0]["plan"]
)
scales = {"": 1, "K": 1000, "M": 1000000, "G": 1000000000}
intermediate_rows = max(
    round(float(n.replace(",", "")) * scales[s])
    for n, s in re.findall(r"\boutput_rows=([0-9.,]+)(?: ([KMG]))?", profile)
)
reference = e.execute(q.replace("count(*)", "(count(*) + 0)")).to_pylist()
assert sorted(reference, key=lambda row: list(row.values())) == rows
print(
    json.dumps(
        {
            "case": case,
            "key": key,
            "edges": len(pairs),
            "paths": sum(x["n"] for x in rows),
            "groups": len(rows),
            "median_ms": statistics.median(values) * 1000,
            "first_ms": first * 1000,
            "peak_rss_bytes": peak_rss,
            "max_operator_output_rows": intermediate_rows,
            "profile": profile,
            "checksum": hashlib.sha256(
                json.dumps(rows, sort_keys=True).encode()
            ).hexdigest(),
            "plan": plan,
        }
    )
)
