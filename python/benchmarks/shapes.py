# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""Generated shape benchmark: the third suite every performance claim reports.

The two SF1 suites write each question one way, over one graph. This suite writes the
general multi-hop shapes that investigative queries use, several ways each, over a
seeded synthetic graph with hubs, wide text columns and ids that don't follow table
order. Each group reports a headline ratio (slowest phrasing over fastest, growth with
a hub's degree, ...) that a good plan keeps close to its ideal; the checklist in issue
#223 is meant to move these.

Every query runs in its own process, so its peak memory is its own. The child checks
that the plan reaches the operator the query is meant to exercise, then times warm
runs. Queries that share a `check` key must give the same answer.

    uv run --no-project --with pylance==14.0.3 --with numpy==2.5.4 --with <wheel> \\
        python benchmarks/shapes.py generate --out /tmp/shapes --size small
    uv run ... python benchmarks/shapes.py run --data /tmp/shapes --mode memory \\
        --label main --results /tmp/shapes-runs
    uv run ... python benchmarks/shapes.py report --results /tmp/shapes-runs \\
        --base main --head branch

See CONTRIBUTING.md for comparing two wheels.
"""

import argparse
import hashlib
import json
import math
import platform
import resource
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SIZES = {
    # tiny is for the test suite; small runs in minutes; large is for final numbers.
    # About 12 T edges per account (see `generate`). The hub degrees in `levels` span
    # four orders of magnitude, so work that grows with one node's degree shows up as a
    # ratio between the first and last level; 100,000 is where such a cliff dominates.
    "tiny": {"accounts": 2_000, "levels": [10, 100]},
    "small": {"accounts": 20_000, "levels": [10, 1_000, 10_000, 100_000]},
    "large": {"accounts": 200_000, "levels": [10, 1_000, 10_000, 100_000]},
}
# Wide enough that copying three notes per row (1.5 KB) costs far more than copying ids.
NOTE_BYTES = 500
# Start nodes in each ring level: enough rows that a per-row cost which grows with the
# hub's degree outweighs the query's fixed costs.
RING_PATHS = 200
# How many of them close a triangle: a small answer that stays the same at every level,
# so the check across levels isn't vacuous.
TRIANGLES = 8


# --- Graph ------------------------------------------------------------------------


def generate(out: Path, size: str, seed: int) -> dict:
    import numpy as np
    import pyarrow as pa

    rng = np.random.default_rng(seed)
    n = SIZES[size]["accounts"]

    # Accounts: ids shuffled against table order; `rank` picks k outer rows. Spaced by 3
    # and offset by 1,000, so no id equals its row position and no code can rely on
    # that.
    ids = rng.permutation(n).astype(np.int64) * 3 + 1_000
    rank = rng.permutation(n).astype(np.int64)
    letters = rng.integers(97, 123, n * NOTE_BYTES, dtype=np.uint8)  # ASCII a to z
    offsets = np.arange(0, (n + 1) * NOTE_BYTES, NOTE_BYTES, dtype=np.int32)
    notes = pa.StringArray.from_buffers(n, pa.py_buffer(offsets), pa.py_buffer(letters))
    accounts = pa.table(
        {
            "id": ids,
            "name": pa.array([f"acct{r}" for r in rank]),
            "rank": rank,
            # Filters of known selectivity: half, a tenth and a hundredth of the
            # accounts.
            "active": rng.integers(0, 2, n),
            "region": rng.integers(0, 10, n),
            "flag": (rng.random(n) < 0.01).astype(np.int64),
            "note": notes,
        }
    )

    # T: heavy-tailed out-degree (Pareto, mean about 12) with a few out-hubs, and
    # 20% of edges aimed at ten in-hubs. S: one edge per account (low fan-out).
    # Pareto shape 1.5 has a long tail; scaled by 4 its mean is 4 * 1.5 / 0.5 = 12. The
    # cap of n / 20 keeps every random degree below the five explicit out-hubs (n / 10
    # each). Each of the ten in-hubs receives about 2% of all edges.
    degree = np.minimum((rng.pareto(1.5, n) + 1) * 4, n // 20).astype(np.int64)
    out_hubs = rng.choice(n, 5, replace=False)
    degree[out_hubs] = n // 10
    src = np.repeat(np.arange(n), degree)
    in_hubs = rng.choice(n, 10, replace=False)
    dst = np.where(
        rng.random(len(src)) < 0.2,
        rng.choice(in_hubs, len(src)),
        rng.integers(0, n, len(src)),
    )
    s_dst = rng.integers(0, n, n)

    # The anchor: an ordinary account (out- and in-degree 8 to 15, around the mean of
    # 12) with an edge into an in-hub, so walking the wrong way from it is costly.
    out_deg = np.bincount(src, minlength=n)
    in_deg = np.bincount(dst, minlength=n)
    into_hub = np.zeros(n, dtype=bool)
    into_hub[src[np.isin(dst, in_hubs)]] = True
    ordinary = (out_deg >= 8) & (out_deg <= 15) & (in_deg >= 8) & (in_deg <= 15)
    anchor = int(np.flatnonzero(ordinary & into_hub)[0])
    # One T edge from the anchor's S neighbour to one of its T neighbours, so the
    # hop-order question has a non-empty answer.
    src = np.append(src, s_dst[anchor])
    dst = np.append(dst, dst[np.flatnonzero(src == anchor)[0]])

    order = rng.permutation(len(src))  # edge rows not grouped by source
    t = pa.table(
        {
            "src": ids[src[order]],
            "dst": ids[dst[order]],
            "day": rng.integers(0, 30, len(src)),  # a month of days, for edge filters
            "amount": np.round(rng.random(len(src)) * 1000, 2),
        }
    )
    s = pa.table({"src": ids, "dst": ids[s_dst]})

    out.mkdir(parents=True, exist_ok=True)
    for name, table in [("account", accounts), ("t", t), ("s", s)]:
        write(table, out / f"{name}.lance")

    meta = {
        "size": size,
        "seed": seed,
        "accounts": n,
        "t_edges": len(src),
        "anchor": int(ids[anchor]),
        "anchor_name": f"acct{rank[anchor]}",
        "levels": SIZES[size]["levels"],
        "ring": write_ring(out, SIZES[size]["levels"]),
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def write(table, path: Path) -> None:
    import lance

    lance.write_dataset(table, path, mode="overwrite" if path.exists() else "create")


def write_ring(out: Path, levels: list[int]) -> dict:
    """Triangles beside a hub whose degree is the only thing that changes per level.

    Per level `d`, relationship types `R{d}` and `P{d}`:
    - start `s_i` -R-> middle `m_i` -R-> hub `h`, and `m_i` -R-> `c_i`;
      `c_i` -R-> `s_i` and `c_i` -P-> `s_i` for the first TRIANGLES of them;
    - `h` -R-> and -P-> `d` spokes;
    - target `g` has `d` fillers -R-> `g`, and `g` -R-> `m'_j` -R-> `h` -R-> `g`, so a
      triangle through `g` closes on an edge with a hub at both ends.
    """
    import numpy as np
    import pyarrow as pa

    ids, roles, info = [], [], {}
    next_id = 0

    def nodes(count: int, role: str) -> np.ndarray:
        nonlocal next_id
        block = np.arange(next_id, next_id + count, dtype=np.int64)
        next_id += count
        ids.append(block)
        roles.extend([role] * count)
        return block

    for d in levels:
        s, m, c = (nodes(RING_PATHS, r) for r in ("start", "middle", "other"))
        h, g = nodes(1, "hub")[0], nodes(1, "target")[0]
        spokes, fillers = nodes(d, "spoke"), nodes(d, "filler")
        m2 = nodes(RING_PATHS, "middle")
        k = TRIANGLES
        r_edges = [
            (s, m),
            (m, np.full(RING_PATHS, h)),
            (m, c),
            (c[:k], s[:k]),
            (np.full(d, h), spokes),
            (fillers, np.full(d, g)),
            (np.full(RING_PATHS, g), m2),
            (m2, np.full(RING_PATHS, h)),
            (np.array([h]), np.array([g])),
        ]
        p_edges = [(c[:k], s[:k]), (np.full(d, h), spokes)]
        for name, edges in [(f"r{d}", r_edges), (f"p{d}", p_edges)]:
            table = pa.table(
                {
                    "src": np.concatenate([a for a, _ in edges]),
                    "dst": np.concatenate([b for _, b in edges]),
                }
            )
            write(table, out / f"{name}.lance")
        info[str(d)] = {"target": int(g)}
    write(pa.table({"id": np.concatenate(ids), "role": roles}), out / "ring.lance")
    return info


def engine_for(data: Path, graph: str, mode: str, use_csr: bool):
    import lance
    from lance_csr import CypherEngine, GraphConfig

    meta = json.loads((data / "meta.json").read_text())
    builder = GraphConfig.builder()
    if graph == "account":
        builder = builder.node_table("Account", "id")
        tables = {"Account": "account", "T": "t", "S": "s"}
        for rel in ("T", "S"):
            builder = builder.rel_table(rel, "Account", "Account", "src", "dst")
    else:
        builder = builder.node_table("Ring", "id")
        tables = {"Ring": "ring"}
        for d in meta["levels"]:
            for rel in (f"R{d}", f"P{d}"):
                builder = builder.rel_table(rel, "Ring", "Ring", "src", "dst")
                tables[rel] = rel.lower()
    datasets = {
        key: lance.dataset(str(data / f"{name}.lance")) for key, name in tables.items()
    }
    if mode == "memory":
        datasets = {key: ds.to_table() for key, ds in datasets.items()}
    return CypherEngine(builder.build(), datasets, use_csr=use_csr)


# --- Queries ----------------------------------------------------------------------


def q(name, cypher, graph="account", csr=True, operator=None, check=None, count=False):
    """One query. `operator` must appear in its plan (`!X`: must not); queries with
    the same `check` must give the same answer; `count` compares a row count."""
    return locals()


def groups(meta: dict) -> list[dict]:
    a, name = meta["anchor"], meta["anchor_name"]
    levels = meta["levels"]
    top = levels[-1]
    path = "(c:Account{s})-[:T*1..2]->(d:Account{e})"
    back = "(d:Account{e})<-[:T*1..2]-(c:Account{s})"
    anchored = f" {{id: {a}}}"
    phrasings = []
    for named in ("", "p = "):
        for text, label in [(path, "start"), (back, "end")]:
            inline = text.format(s=anchored, e="")
            where = text.format(s="", e="") + f" WHERE c.id = {a}"
            kind = "named" if named else "unnamed"
            for how, pattern in [("inline", inline), ("where", where)]:
                phrasings.append(
                    q(
                        f"{kind}_{label}_{how}",
                        f"MATCH {named}{pattern} RETURN count(*) AS n",
                        check="paths",
                    )
                )

    two_filters = "WHERE a.active = 0 AND c.name = '{}'".format(name)
    fwd = f"MATCH (a:Account)-[:T]->(b:Account)-[:T]->(c:Account) {two_filters}"
    rev = f"MATCH (c:Account)<-[:T]-(b:Account)<-[:T]-(a:Account) {two_filters}"

    hub = []
    for d in levels:
        cycle = f"MATCH (a:Ring)-[:R{d}]->(b:Ring)-[:R{d}]->(c:Ring)-[:R{d}]->(a)"
        both = (
            f"MATCH (a:Ring {{id: {meta['ring'][str(d)]['target']}}})"
            f"-[:R{d}]->(b:Ring)-[:R{d}]->(c:Ring)-[:R{d}]->(a)"
        )
        predicate = (
            f"MATCH (a:Ring)-[:R{d}]->(b:Ring) WHERE a.role = 'start' "
            f"AND (b)-[:R{d}]->()-[:P{d}]->(a)"
        )
        start = " WHERE a.role = 'start'"
        hub += [
            q(
                f"cycle_{d}",
                f"{cycle}{start} RETURN count(*) AS n",
                "ring",
                operator="CsrExpandExec",
                check="cycle",
            ),
            q(
                f"cycle_join_{d}",
                f"{cycle}{start} RETURN count(*) AS n",
                "ring",
                csr=False,
                operator="!CsrExpandExec",
                check="cycle",
            ),
            q(
                f"both_hubs_{d}",
                f"{both} RETURN count(*) AS n",
                "ring",
                operator="CsrExpandExec",
                check="both_hubs",
            ),
            q(
                f"predicate_{d}",
                f"{predicate} RETURN count(*) AS n",
                "ring",
                operator="PathExistsExec",
                check="predicate",
            ),
            q(
                f"predicate_join_{d}",
                f"{predicate} RETURN count(*) AS n",
                "ring",
                csr=False,
                operator="!PathExistsExec",
                check="predicate",
            ),
        ]

    # 200 outer accounts: about 2,400 first hops, enough to time the second hop.
    hop = (
        "MATCH (a:Account)-[:T]->(x:Account) WHERE a.rank < 200 "
        "{} (x)-[:T]->(b:Account)"
    )
    flagged = " WHERE b.flag = 1"
    returns = " RETURN a.id, x.id, b.id"

    # Three hops from 50 accounts gives about 10^5 rows: enough to time, and small
    # enough to hold three 500-byte notes per row in memory.
    chain = (
        "MATCH (a:Account)-[:T]->(b:Account)-[:T]->(c:Account)-[:T]->(d:Account) "
        "WHERE a.rank < 50"
    )
    ids, notes = "a.id, b.id, c.id, d.id", "b.note, c.note, d.note"
    wide = []
    for case, tail in [
        ("far_filter", " AND d.flag = 1 RETURN {}"),
        ("cross_filter", " AND d.id < a.id RETURN {}"),
        ("top_k", " RETURN {} ORDER BY d.id LIMIT 10"),
        ("all", " RETURN {}"),
    ]:
        for cols, label in [(ids, "ids"), (notes, "notes")]:
            wide.append(
                q(f"{case}_{label}", chain + tail.format(cols), check=case, count=True)
            )

    one_hop = "MATCH (a:Account)-[:T]->(b:Account)"
    expand = "CsrExpandExec"  # a real expansion, not a scan of the edge table
    some = " WHERE a.rank < 2000"  # about 24,000 one-hop rows of notes

    result = [
        {
            "group": "phrasing",
            "catches": "Plans that depend on query text",
            "headlines": [("slowest / fastest of 8 phrasings", "*", "*", "1")],
            "queries": phrasings,
        },
        {
            "group": "selectivity",
            "catches": "A start or hop order chosen without statistics",
            "headlines": [
                (
                    "two filters: slowest / fastest end",
                    ["rows_fwd", "rows_rev"],
                    ["rows_fwd", "rows_rev"],
                    "1",
                ),
                (
                    "hop order: slowest / fastest",
                    ["hop_cheap_first", "hop_hub_first"],
                    ["hop_cheap_first", "hop_hub_first"],
                    "1",
                ),
            ],
            "queries": [
                q(
                    "rows_fwd",
                    f"{fwd} RETURN a.id, c.id",
                    check="two_filters",
                    count=True,
                ),
                q(
                    "rows_rev",
                    f"{rev} RETURN a.id, c.id",
                    check="two_filters",
                    count=True,
                ),
                q(
                    "hop_cheap_first",
                    f"MATCH (a:Account {{name: '{name}'}})-[:S]->(c:Account), "
                    "(c)-[:T]->(b:Account), (a)-[:T]->(b) RETURN count(*) AS n",
                    check="hop_order",
                ),
                q(
                    "hop_hub_first",
                    f"MATCH (a:Account {{name: '{name}'}})-[:T]->(b:Account), "
                    "(b)<-[:T]-(c:Account), (c)<-[:S]-(a) RETURN count(*) AS n",
                    check="hop_order",
                ),
            ],
        },
        {
            "group": "count_rows",
            "catches": "A rewrite slower than the plan it replaces",
            "headlines": [
                (
                    "slowest count / fastest rows",
                    ["count_fwd", "count_rev"],
                    ["rows_fwd", "rows_rev"],
                    "<= 1",
                )
            ],
            "queries": [
                q("count_fwd", f"{fwd} RETURN count(*) AS n", check="two_filters"),
                q("count_rev", f"{rev} RETURN count(*) AS n", check="two_filters"),
            ],
        },
        {
            "group": "hubs",
            "catches": "Work that grows with a hub's degree; CSR losing to joins",
            "headlines": [
                (
                    f"cycle: degree {top} / degree {levels[0]}",
                    [f"cycle_{top}"],
                    [f"cycle_{levels[0]}"],
                    "1",
                ),
                (
                    f"cycle, hub at both ends: degree {top} / {levels[0]}",
                    [f"both_hubs_{top}"],
                    [f"both_hubs_{levels[0]}"],
                    "1",
                ),
                (
                    f"pattern predicate: degree {top} / {levels[0]}",
                    [f"predicate_{top}"],
                    [f"predicate_{levels[0]}"],
                    "1",
                ),
                (
                    f"cycle at degree {top}: CSR / join",
                    [f"cycle_{top}"],
                    [f"cycle_join_{top}"],
                    "<= 1",
                ),
                (
                    f"predicate at degree {top}: CSR / join",
                    [f"predicate_{top}"],
                    [f"predicate_join_{top}"],
                    "<= 1",
                ),
            ],
            "queries": hub,
        },
        {
            "group": "optional",
            "catches": "Falling off the outer CSR hop",
            "headlines": [
                (
                    "filtered / unfiltered OPTIONAL MATCH",
                    ["optional_where"],
                    ["optional"],
                    "<= 1",
                ),
                (
                    "filtered OPTIONAL MATCH / filtered MATCH",
                    ["optional_where"],
                    ["match_where"],
                    "about 1",
                ),
            ],
            "queries": [
                q("match", hop.format("MATCH") + returns),
                q("optional", hop.format("OPTIONAL MATCH") + returns),
                q("optional_where", hop.format("OPTIONAL MATCH") + flagged + returns),
                q("match_where", hop.format("MATCH") + flagged + returns),
            ],
        },
        {
            "group": "wide_columns",
            "catches": "Columns copied through hops that never read them",
            "headlines": [
                (f"{case}: notes / ids", [f"{case}_notes"], [f"{case}_ids"], ideal)
                for case, ideal in [
                    ("cross_filter", "about 1"),
                    ("top_k", "about 1"),
                    ("far_filter", "about 1"),
                    ("all", "> 1"),
                ]
            ],
            "queries": wide,
        },
        {
            "group": "early_exit",
            "catches": "Operators that hide limits from DataFusion",
            "headlines": [
                (
                    "LIMIT 10 / all rows, ids",
                    ["hop_limit_ids"],
                    ["hop_all_ids"],
                    "<< 1",
                ),
                (
                    "LIMIT 10 / all rows, notes",
                    ["hop_limit_notes"],
                    ["hop_all_notes"],
                    "<< 1",
                ),
            ],
            "queries": [
                q("hop_limit_ids", f"{one_hop} RETURN b.id LIMIT 10", operator=expand),
                q("hop_all_ids", f"{one_hop} RETURN b.id", operator=expand),
                q(
                    "hop_limit_notes",
                    f"{one_hop}{some} RETURN b.note LIMIT 10",
                    operator=expand,
                ),
                q("hop_all_notes", f"{one_hop}{some} RETURN b.note", operator=expand),
            ],
        },
    ]
    names = [spec["name"] for group in result for spec in group["queries"]]
    assert len(names) == len(set(names)), "query names must be unique"
    return result


def headline(spec, times: dict, group: set[str]) -> float | None:
    """max(time of `nums`) / min(time of `dens`); `"*"` is every query in the group."""
    _, nums, dens, _ = spec
    nums, dens = (group, group) if nums == "*" else (nums, dens)
    try:
        return max(times[n] for n in nums) / min(times[n] for n in dens)
    except (KeyError, TypeError, ZeroDivisionError):
        return None


# --- Running ----------------------------------------------------------------------


def peak_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024  # macOS bytes, Linux KiB


def rss_bytes() -> int:
    import os

    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())])
    return int(out) * 1024


def answer(table, spec: dict) -> str | int:
    if spec["count"]:
        return table.num_rows
    if table.num_rows == 1 and table.num_columns == 1:
        return table.column(0)[0].as_py()
    rows = sorted(repr(tuple(r.values())) for r in table.to_pylist())
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()[:16]


def child(data: Path, mode: str, name: str, timed: bool = True) -> dict:
    meta = json.loads((data / "meta.json").read_text())
    spec = next(s for g in groups(meta) for s in g["queries"] if s["name"] == name)
    engine = engine_for(data, spec["graph"], mode, spec["csr"])
    plan = engine.explain(spec["cypher"])
    op = spec["operator"]
    plan_ok = op is None or (op[1:] not in plan if op.startswith("!") else op in plan)
    before = peak_bytes()
    start = time.perf_counter()
    table = engine.execute(spec["cypher"])
    warm = time.perf_counter() - start
    times: list[float] = []
    # At least 5 runs; up to 50 or 2 s for fast queries; stop after 10 s for slow ones.
    # Sub-millisecond queries then get enough samples for a stable median, and a cliff
    # on `main` can't stretch a run past a few minutes.
    while timed and (len(times) < 5 or (sum(times) < 2 and len(times) < 50)):
        start = time.perf_counter()
        engine.execute(spec["cypher"])
        times.append(time.perf_counter() - start)
        if sum(times) > 10:
            break
    times = times or [warm]
    return {
        "rows": table.num_rows,
        "answer": answer(table, spec) if spec["check"] else None,
        "plan_ok": plan_ok,
        "first_ms": warm * 1e3,
        "median_ms": statistics.median(times) * 1e3,
        "min_ms": min(times) * 1e3,
        "runs": len(times),
        "peak_mib": peak_bytes() / 2**20,
        "peak_growth_mib": max(0, peak_bytes() - before) / 2**20,
    }


def index_child(data: Path, graph: str) -> dict:
    """Resident memory the CSR and node indexes add, with tables left on disk."""
    engine_for(data, graph, "lazy", False)  # warm the Lance and Arrow imports
    before = rss_bytes()
    engine = engine_for(data, graph, "lazy", True)
    return {
        "index_mib": (rss_bytes() - before) / 2**20,
        "build_ms": sum(s["build_ms"] for s in engine.csr_stats().values())
        + sum(s["build_ms"] for s in engine.node_index_stats().values()),
        "edges": sum(s["edges"] for s in engine.csr_stats().values()),
    }


def spawn(args: list[str], timeout: float) -> dict:
    try:
        proc = subprocess.run(
            [sys.executable, __file__, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"error": f"timeout after {timeout:.0f} s"}
    if proc.returncode != 0:
        lines = proc.stderr.strip().splitlines() or [f"exit code {proc.returncode}"]
        return {"error": lines[-1][:300]}
    return json.loads(proc.stdout.strip().splitlines()[-1])


def run(data: Path, mode: str, label: str, results: Path, timeout: float, only) -> int:
    import lance_csr

    meta = json.loads((data / "meta.json").read_text())
    out = {
        "label": label,
        "mode": mode,
        "lance_csr": lance_csr.__file__,
        "machine": f"{platform.platform()}, {platform.processor()}",
        "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "graph": {
            k: meta[k] for k in ("size", "seed", "accounts", "t_edges", "levels")
        },
        "index": {
            g: spawn(["index", "--data", str(data), "--graph", g], timeout)
            for g in ("account", "ring")
        },
        "queries": {},
    }
    print(f"{label} {mode} {lance_csr.__file__}", flush=True)
    for group in groups(meta):
        if only and group["group"] not in only:
            continue
        for spec in group["queries"]:
            args = [
                "child",
                "--data",
                str(data),
                "--mode",
                mode,
                "--query",
                spec["name"],
            ]
            result = spawn(args, timeout)
            out["queries"][spec["name"]] = {"group": group["group"], **result}
            shown = result.get("error") or (
                f"{result['median_ms']:9.2f} ms {result['peak_growth_mib']:7.1f} MiB"
                + ("" if result["plan_ok"] else "  PLAN CHECK FAILED")
            )
            print(f"{group['group']:13} {spec['name']:22} {shown}", flush=True)
    failures = check(meta, out["queries"])
    out["failures"] = failures
    results.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = results / f"{label}-{mode}-{stamp}.json"
    path.write_text(json.dumps(out, indent=2))
    print(f"wrote {path}")
    for failure in failures:
        print("FAILED:", failure)
    return 1 if failures else 0


def check(meta: dict, results: dict) -> list[str]:
    failures, answers = [], {}
    for group in groups(meta):
        for spec in group["queries"]:
            r = results.get(spec["name"])
            if r is None:
                continue
            if "error" in r:
                # A timeout is a (very) slow result; any other error is a failure.
                if not r["error"].startswith("timeout"):
                    failures.append(f"{spec['name']}: {r['error']}")
                continue
            if not r["plan_ok"]:
                failures.append(f"{spec['name']}: plan lacks {spec['operator']}")
            if spec["check"]:
                answers.setdefault(spec["check"], {})[spec["name"]] = r["answer"]
    for key, by_query in answers.items():
        if len(set(map(str, by_query.values()))) > 1:
            failures.append(f"answers differ for '{key}': {by_query}")
    return failures


# --- Report -----------------------------------------------------------------------


def report(results: Path, base: str, head: str | None) -> None:
    runs = [json.loads(p.read_text()) for p in sorted(results.glob("*.json"))]
    for mode in ("memory", "lazy"):
        by_label = {
            label: [r for r in runs if r["mode"] == mode and r["label"] == label]
            for label in filter(None, (base, head))
        }
        by_label = {label: found for label, found in by_label.items() if found}
        if base not in by_label:
            continue
        pair = len(by_label) == 2
        labels = list(by_label)
        first = by_label[base][0]
        meta = {
            "anchor": 0,
            "anchor_name": "",
            **first["graph"],
            "ring": {str(d): {"target": 0} for d in first["graph"]["levels"]},
        }

        def times(label: str) -> dict:
            out = {}
            for name in by_label[label][0]["queries"]:
                values = [r["queries"][name].get("median_ms") for r in by_label[label]]
                if all(v is not None for v in values):
                    out[name] = statistics.mean(values)
            return out

        t = {label: times(label) for label in labels}
        g = first["graph"]
        print(
            f"\n## {mode}: {g['size']} graph ({g['accounts']:,} accounts, "
            f"{g['t_edges']:,} T edges, hub degrees {g['levels']})\n"
        )
        print(f"Runs: {', '.join(f'{lb} x{len(by_label[lb])}' for lb in labels)}.\n")
        print("| Group | Headline | Ideal | " + " | ".join(labels) + " |")
        print("| --- | --- | --- |" + " ---: |" * len(labels))
        for group in groups(meta):
            names = {s["name"] for s in group["queries"]}
            for spec in group["headlines"]:
                values = [headline(spec, t[lb], names) for lb in labels]
                shown = [f"{v:.2f}x" if v is not None else "n/a" for v in values]
                print(
                    f"| {group['group']} | {spec[0]} | {spec[3]} | "
                    + " | ".join(shown)
                    + " |"
                )
        print(
            "\n| Query | "
            + " | ".join(f"{lb} ms" for lb in labels)
            + (" | Change" if pair else "")
            + " | "
            + " | ".join(f"{lb} peak MiB" for lb in labels)
            + " |"
        )
        print("| --- |" + " ---: |" * (len(labels) * 2 + pair))
        for name, r in first["queries"].items():
            ms = [t[lb].get(name) for lb in labels]
            peaks = [
                max(
                    run["queries"][name].get("peak_growth_mib", math.nan)
                    for run in by_label[lb]
                )
                for lb in labels
            ]
            change = ""
            if pair:
                change = (
                    f" | {100 * (ms[1] / ms[0] - 1):+.0f}%"
                    if None not in ms
                    else " | n/a"
                )
            print(
                f"| {r['group']}/{name} | "
                + " | ".join(
                    f"{m:.2f}"
                    if m is not None
                    else by_label[lb][0]["queries"][name].get("error", "n/a")
                    for m, lb in zip(ms, labels)
                )
                + change
                + " | "
                + " | ".join(f"{p:.1f}" for p in peaks)
                + " |"
            )
        print(
            "\n| Index | "
            + " | ".join(f"{lb} MiB" for lb in labels)
            + " | "
            + " | ".join(f"{lb} build ms" for lb in labels)
            + " |"
        )
        print("| --- |" + " ---: |" * (len(labels) * 2))
        for graph in ("account", "ring"):
            rows = [by_label[lb][0]["index"][graph] for lb in labels]
            print(
                f"| {graph} | "
                + " | ".join(f"{r.get('index_mib', math.nan):.1f}" for r in rows)
                + " | "
                + " | ".join(f"{r.get('build_ms', math.nan):.0f}" for r in rows)
                + " |"
            )
        if len(by_label[base]) > 1:
            gaps = [
                run["queries"][n]["median_ms"]
                / by_label[base][0]["queries"][n]["median_ms"]
                for run in by_label[base][1:]
                for n in t[base]
                if "median_ms" in run["queries"].get(n, {})
            ]
            if gaps:
                geo = math.exp(sum(map(math.log, gaps)) / len(gaps))
                print(
                    f"\n{base} run-to-run geomean: {geo:.3f}; largest single-query gap "
                    f"{100 * (max(max(gaps), 1 / min(gaps)) - 1):.0f}%."
                )
        for label in labels:
            for run in by_label[label]:
                for failure in run["failures"]:
                    print(f"\nFAILED ({label}): {failure}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    gen = sub.add_parser("generate", help="write the graph")
    gen.add_argument("--out", type=Path, required=True)
    gen.add_argument("--size", choices=SIZES, default="small")
    gen.add_argument("--seed", type=int, default=0)
    r = sub.add_parser("run", help="run every query against the installed lance_csr")
    r.add_argument("--data", type=Path, required=True)
    r.add_argument("--mode", choices=("memory", "lazy"), required=True)
    r.add_argument("--label", required=True, help="the build, such as main or branch")
    r.add_argument("--results", type=Path, required=True)
    # Generous, because a cliff on `main` at the large size can take minutes; a timeout
    # is reported as a slow result, not a failure.
    r.add_argument("--timeout", type=float, default=300, help="seconds per query")
    r.add_argument("--group", action="append", help="only these groups")
    rep = sub.add_parser("report", help="markdown tables from saved runs")
    rep.add_argument("--results", type=Path, required=True)
    rep.add_argument("--base", required=True)
    rep.add_argument("--head")
    c = sub.add_parser("child")
    c.add_argument("--data", type=Path, required=True)
    c.add_argument("--mode", required=True)
    c.add_argument("--query", required=True)
    i = sub.add_parser("index")
    i.add_argument("--data", type=Path, required=True)
    i.add_argument("--graph", required=True)
    args = parser.parse_args()

    if args.cmd == "generate":
        print(json.dumps(generate(args.out, args.size, args.seed)))
    elif args.cmd == "run":
        sys.exit(
            run(
                args.data, args.mode, args.label, args.results, args.timeout, args.group
            )
        )
    elif args.cmd == "report":
        report(args.results, args.base, args.head)
    elif args.cmd == "child":
        print(json.dumps(child(args.data, args.mode, args.query)))
    else:
        print(json.dumps(index_child(args.data, args.graph)))


if __name__ == "__main__":
    main()
