# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""The shape benchmark's phrasings agree and reach the operators they measure."""

import importlib.util
import json
from pathlib import Path

import pytest

_path = Path(__file__).resolve().parents[2] / "benchmarks" / "shapes.py"
_spec = importlib.util.spec_from_file_location("shapes", _path)
assert _spec and _spec.loader
shapes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(shapes)


@pytest.fixture(scope="module")
def graph(tmp_path_factory):
    out = tmp_path_factory.mktemp("shapes")
    shapes.generate(out, "tiny", seed=0)
    return out


@pytest.mark.parametrize("mode", ["memory", "lazy"])
def test_every_query_runs_and_phrasings_agree(graph, mode):
    meta = json.loads((graph / "meta.json").read_text())
    results = {
        spec["name"]: shapes.child(graph, mode, spec["name"], timed=False)
        for group in shapes.groups(meta)
        for spec in group["queries"]
    }
    assert shapes.check(meta, results) == []
    # Each phrasing group answers something, so agreement is not vacuous.
    assert results["named_end_where"]["answer"] > 0
    assert results["hop_cheap_first"]["answer"] > 0
    assert results["cycle_10"]["answer"] == shapes.TRIANGLES
    assert results["both_hubs_10"]["answer"] == shapes.RING_PATHS
    assert results["predicate_100"]["answer"] == shapes.TRIANGLES


def test_generation_is_deterministic(tmp_path):
    first = shapes.generate(tmp_path / "a", "tiny", seed=3)
    second = shapes.generate(tmp_path / "b", "tiny", seed=3)
    assert first == second
