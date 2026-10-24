"""Run the Python code blocks in the documentation pages.

Each page's ```python blocks run in order, in one namespace, inside a fresh temporary
directory. A block directly after a `<!-- skip-check -->` line is skipped: use it for
snippets that need files or services the page doesn't create.

Run it with an environment that has lance-csr installed, from the repository root:

    cd python && uv run --no-sync python ../docs/check_snippets.py
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import textwrap
import traceback
from pathlib import Path

DOCS = Path(__file__).resolve().parent
FENCE = re.compile(r"^( *)```python[^\n]*\n(.*?)^\1```", re.M | re.S)


def blocks(text: str) -> list[tuple[int, str]]:
    found = []
    for match in FENCE.finditer(text):
        before = text[: match.start()].rstrip().splitlines()
        if before and before[-1].strip() == "<!-- skip-check -->":
            continue
        line = text.count("\n", 0, match.start()) + 1
        found.append((line, textwrap.dedent(match.group(2))))
    return found


def main(pages: list[str]) -> int:
    paths = [DOCS / p for p in pages] or sorted(DOCS.glob("*.md"))
    failures = 0
    for path in paths:
        code = blocks(path.read_text())
        if not code:
            continue
        namespace: dict = {"__name__": "__main__"}
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                for line, source in code:
                    try:
                        exec(compile(source, f"{path.name}:{line}", "exec"), namespace)
                    except Exception:
                        failures += 1
                        print(f"FAIL {path.name}:{line}")
                        traceback.print_exc()
                        break
            finally:
                os.chdir(cwd)
        print(f"ran {len(code)} blocks from {path.name}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
