"""Rewrite the benchmark tables in README.md and docs/benchmarks.md from the benchmark repositories.

Each benchmark repository commits pytest-benchmark's output for every engine to `results/`, with
the machine and date in `results/environment.yml`. This script downloads those files at the
commit pinned in BENCHMARKS and replaces the text between each pair of markers:

    <!-- bench:<key>:<block> -->
    ...generated...
    <!-- /bench:<key>:<block> -->

Blocks are `summary` (geometric-mean ratios), `table` (per-query mean latency) and `notes`
(where and when the numbers were measured, and any failed queries). A marker indented inside an
admonition gets its block indented to match.

To publish a new run, merge it in the benchmark repository, set `ref` to that commit and run:

    uv run --project docs --locked python docs/sync_benchmarks.py
"""

from __future__ import annotations

import math
import re
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGETS = [ROOT / "README.md", ROOT / "docs" / "benchmarks.md"]

# Columns: (results file stem, column title). The first two are lance-csr's modes; the rest are
# the systems it is compared with.
BENCHMARKS = {
    "ic": {
        "repo": "hyperdimensionalcomputingai/graph-benchmark-ldbc-complex",
        "ref": "c185123edd58b37b417bc4deb2fec643fdbd5e61",
        "label": "IC{n}",
        "engines": [
            ("lance-csr-0.2.0-memory", "lance-csr 0.2.0, in memory"),
            ("lance-csr-0.2.0-lazy", "lance-csr 0.2.0, lazy"),
            ("neo4j-2026.09.0", "Neo4j 2026.09.0"),
            ("kuzu-0.11.3", "Kuzu 0.11.3"),
            ("ladybug-0.21.2", "Ladybug 0.21.2"),
        ],
    },
    "q30": {
        "repo": "hyperdimensionalcomputingai/graph-benchmark-ldbc",
        "ref": "6260689aa1fba4eeb4e99958430386b2fcd13f71",
        "label": "Q{n}",
        "engines": [
            ("lance-csr-0.2.0-memory", "lance-csr 0.2.0, in memory"),
            ("lance-csr-0.2.0-lazy", "lance-csr 0.2.0, lazy"),
            ("neo4j-2026.09.0", "Neo4j 2026.09.0"),
            ("kuzu-0.11.3", "Kuzu 0.11.3"),
            ("ladybug-0.21.2", "Ladybug 0.21.2"),
            ("lance-graph-0.5.4", "lance-graph 0.5.4"),
        ],
    },
}

HEADER_RE = re.compile(r"Name \(time in (?P<unit>[^)]+)\)")
FAILED_RE = re.compile(r"^FAILED \S+::\S*?(?P<n>\d+)\S* - (?P<error>\w+)")
UNIT_TO_MS = {"s": 1000.0, "ms": 1.0, "us": 0.001, "µs": 0.001, "ns": 0.000001}
FAILURE_TEXT = {
    "AssertionError": "returned a wrong answer",
    "TimeoutError": "timed out",
}


def fetch(repo: str, ref: str, path: str) -> str:
    url = f"https://raw.githubusercontent.com/{repo}/{ref}/{path}"
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read().decode()


def parse_results(text: str) -> tuple[dict[int, float], dict[int, str]]:
    "Mean ms per passing query number, and the exception name per failing one."
    lines = text.splitlines()
    failures = {int(m["n"]): m["error"] for m in map(FAILED_RE.match, lines) if m}
    scale: float | None = None
    means: dict[int, float] = {}
    for line in lines:
        if scale is None:
            if match := HEADER_RE.search(line):
                scale = UNIT_TO_MS[match["unit"].strip()]
            continue
        if line.startswith("test_"):
            columns = re.split(r"\s{2,}", line.strip())
            n = int(re.search(r"(\d+)\D*$", columns[0])[1])
            if n not in failures:
                means[n] = float(columns[3].split()[0].replace(",", "")) * scale
    if scale is None:
        raise ValueError("no pytest-benchmark table found")
    return means, failures


def geomean_ratio(
    ours: dict[int, float], theirs: dict[int, float]
) -> tuple[float, int]:
    common = [n for n in ours if n in theirs]
    return math.exp(
        sum(math.log(theirs[n] / ours[n]) for n in common) / len(common)
    ), len(common)


def describe(ratio: float) -> str:
    return (
        f"{ratio:.1f}× faster"
        if ratio >= 1.05
        else f"{ratio:.2f}× ({'slower' if ratio < 0.95 else 'on par'})"
    )


def render(key: str, spec: dict) -> dict[str, str]:
    engines = spec["engines"]
    env = dict(
        line.split(": ", 1)
        for line in fetch(
            spec["repo"], spec["ref"], "results/environment.yml"
        ).splitlines()
        if ": " in line
    )
    results = {
        stem: parse_results(fetch(spec["repo"], spec["ref"], f"results/{stem}.txt"))
        for stem, _ in engines
    }
    numbers = sorted(
        {n for means, failures in results.values() for n in [*means, *failures]}
    )

    rows = [
        "| Query | " + " | ".join(title for _, title in engines) + " |",
        "| --- |" + " ---: |" * len(engines),
    ]
    for n in numbers:
        values = [results[stem][0].get(n) for stem, _ in engines]
        best = min(v for v in values if v is not None)
        cells = [
            "n/a" if v is None else (f"**{v:,.2f}**" if v == best else f"{v:,.2f}")
            for v in values
        ]
        rows.append(f"| {spec['label'].format(n=n)} | " + " | ".join(cells) + " |")

    others = engines[2:]
    summary = [
        "| lance-csr mode | "
        + " | ".join(f"vs {title.rsplit(' ', 1)[0]}" for _, title in others)
        + " |",
        "| --- |" + " ---: |" * len(others),
    ]
    for (stem, _), mode in zip(engines[:2], ["In memory", "Lazy (Lance on disk)"]):
        cells = [
            describe(geomean_ratio(results[stem][0], results[other][0])[0])
            for other, _ in others
        ]
        summary.append(f"| {mode} | " + " | ".join(cells) + " |")

    commit = f"https://github.com/{spec['repo']}/tree/{spec['ref']}"
    notes = [
        (
            f"- Measured on {env['date']} on {env['machine']}, {env['os']}, from the benchmark repository at "
            f"commit [`{spec['ref'][:7]}`]({commit}). Every engine ran one after another with the repository's "
            "`run_benchmarks.sh` settings; see its README to reproduce the runs."
        ),
        "- Ratios are geometric means of the per-query ratios, over the queries both engines answered correctly.",
    ]
    for stem, title in engines:
        failures = results[stem][1]
        if failures:
            what = "; ".join(
                f"{spec['label'].format(n=n)} {FAILURE_TEXT.get(error, f'failed ({error})')}"
                for n, error in sorted(failures.items())
            )
            notes.append(
                f"- {title}: {what}, so {'it is' if len(failures) == 1 else 'they are'} not timed."
            )
    return {
        "summary": "\n".join(summary),
        "table": "\n".join(rows),
        "notes": "\n".join(notes),
    }


def replace_blocks(text: str, key: str, blocks: dict[str, str]) -> str:
    for block, body in blocks.items():
        pattern = re.compile(
            rf"^(?P<indent>[ \t]*)<!-- bench:{key}:{block} -->\n.*?^[ \t]*<!-- /bench:{key}:{block} -->$",
            re.MULTILINE | re.DOTALL,
        )

        def fill(match: re.Match[str], block: str = block, body: str = body) -> str:
            indent = match["indent"]
            inner = "\n".join(
                indent + line if line else line for line in body.splitlines()
            )
            # Blank lines keep the markers out of the Markdown block between them.
            return f"{indent}<!-- bench:{key}:{block} -->\n\n{inner}\n\n{indent}<!-- /bench:{key}:{block} -->"

        text = pattern.sub(fill, text)
    return text


def main() -> None:
    rendered = {
        key: render(key, spec) for key, spec in BENCHMARKS.items() if spec["ref"]
    }
    for path in TARGETS:
        text = original = path.read_text()
        for key, blocks in rendered.items():
            text = replace_blocks(text, key, blocks)
        if text != original:
            path.write_text(text)
            print(f"updated {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
