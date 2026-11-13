"""Run the opt-in release example sequentially; standard library only, macOS RSS."""

import csv
import os
import re
import subprocess
import sys
from pathlib import Path

binary = str(Path(sys.argv[1]).resolve())
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
cases = [
    (shape, 64, targets)
    for shape in (
        "hub",
        "uniform",
        "chain",
        "tree",
        "tree_near",
        "disconnected",
        "layered",
    )
    for targets in (1, 3)
]
cases += [("hub", sources, 3) for sources in (1, 8, 65, 256)]
cases = [(*case, 4096) for case in cases]
cases += [
    (shape, 64, targets, 32768)
    for shape in ("hub", "uniform", "layered")
    for targets in (1, 3)
]
modes = ("current", "forward", "batched", "masked", "parallel")
fields = [
    "shape",
    "sources",
    "targets",
    "mode",
    "vertices",
    "search_ms",
    "reconstruct_ms",
    "scans",
    "edges",
    "visited",
    "parents",
    "output_nodes",
    "checksum",
]
subprocess.run([binary, "check"], check=True)
# Verification uses a reference search in each process; it is excluded from RSS runs.
for case in cases:
    for mode in modes:
        subprocess.run(
            [binary, *map(str, case[:3]), mode],
            env={**os.environ, "MSBFS_VERIFY": "1", "MSBFS_VERTICES": str(case[3])},
            check=True,
            stdout=subprocess.DEVNULL,
        )
print("All workload target distances match the current search.", flush=True)
answers = {}
with (out / "raw.csv").open("w") as handle:
    writer = csv.DictWriter(
        handle, fieldnames=fields + ["repeat", "peak_rss_bytes"], lineterminator="\n"
    )
    writer.writeheader()
    for repeat in (1, 2):
        for case in cases:
            # Reverse the order on the second process repetition.
            for mode in modes if repeat == 1 else tuple(reversed(modes)):
                result = subprocess.run(
                    ["/usr/bin/time", "-l", binary, *map(str, case[:3]), mode],
                    text=True,
                    capture_output=True,
                    check=True,
                    env={**os.environ, "MSBFS_VERTICES": str(case[3])},
                )
                values = result.stdout.strip().split(",")
                assert len(values) == len(fields), result.stdout
                row = dict(zip(fields, values))
                key = tuple(case)
                answer = (row["output_nodes"], row["checksum"])
                assert answers.setdefault(key, answer) == answer, (case, mode, answer)
                rss = re.search(r"(\d+)\s+maximum resident set size", result.stderr)
                assert rss, result.stderr
                row.update(repeat=repeat, peak_rss_bytes=rss[1])
                writer.writerow(row)
                handle.flush()
            print(f"repeat {repeat}: {case}", flush=True)
