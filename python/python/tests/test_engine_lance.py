# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""CypherEngine over on-disk Lance datasets: snapshots, refresh and streaming.

The engine pins the dataset version it was given; writes become visible only
after ``refresh()``. Expected values are computed independently of the engine.
"""

import re
import shutil
import threading

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig

# Past the 10,000-row cap the old conversion fallback silently applied.
N = 12_000

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)


def people(start, stop):
    ids = list(range(start, stop))
    return pa.table({"id": ids, "name": [f"p{i}" for i in ids]})


@pytest.fixture
def graph(tmp_path):
    # Several fragments so scans produce multiple batches.
    person = lance.write_dataset(
        people(0, N), tmp_path / "person.lance", max_rows_per_file=4_000
    )
    knows = lance.write_dataset(
        pa.table({"src": list(range(N - 1)), "dst": list(range(1, N))}),
        tmp_path / "knows.lance",
    )
    return tmp_path, person, knows


def count_people(engine):
    return engine.execute("MATCH (p:Person) RETURN p.id").num_rows


def test_cypher_query_reads_lance_rows_beyond_10k(graph):
    # Regression: the one-shot path used to stop at 10,000 rows.
    _, person, knows = graph
    query = CypherQuery("MATCH (p:Person) RETURN p.id").with_config(CONFIG)
    out = query.execute({"Person": person, "KNOWS": knows})
    assert out.num_rows == N
    assert pc.sum(out.column(0)).as_py() == sum(range(N))


def test_empty_arrow_table_input():
    empty = pa.table(
        {"id": pa.array([], pa.int64()), "name": pa.array([], pa.string())}
    )
    out = (
        CypherQuery("MATCH (p:Person) RETURN p.name")
        .with_config(CONFIG)
        .execute({"Person": empty})
    )
    assert out.num_rows == 0


def test_engine_reads_all_rows_and_traverses(graph):
    _, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})

    out = engine.execute("MATCH (p:Person) RETURN p.id")
    assert out.num_rows == N
    assert pc.sum(out.column(0)).as_py() == sum(range(N))

    hop = engine.execute(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 11998 RETURN b.name"
    )
    assert hop.column(0).to_pylist() == ["p11999"]


def test_engine_mixes_lance_and_arrow_inputs(graph):
    _, person, _ = graph
    knows = pa.table({"src": [0, 1], "dst": [1, 2]})
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    out = engine.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.id")
    assert sorted(out.column(0).to_pylist()) == [1, 2]
    assert engine.dataset_versions() == {"Person": 1}


def test_engine_empty_lance_dataset(tmp_path):
    empty = lance.write_dataset(
        pa.table({"id": pa.array([], pa.int64())}), tmp_path / "empty.lance"
    )
    engine = CypherEngine(CONFIG, {"Person": empty})
    assert count_people(engine) == 0


def test_snapshot_until_refresh_after_append(graph):
    path, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})

    lance.write_dataset(people(N, N + 5), path / "person.lance", mode="append")
    assert count_people(engine) == N

    engine.refresh()
    assert count_people(engine) == N + 5
    assert engine.dataset_versions()["Person"] == 2


def test_refresh_without_changes_keeps_results(graph):
    _, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    before = engine.execute("MATCH (p:Person) WHERE p.id < 3 RETURN p.name")
    engine.refresh()
    after = engine.execute("MATCH (p:Person) WHERE p.id < 3 RETURN p.name")
    assert before.equals(after)
    assert engine.dataset_versions() == {"Person": 1, "KNOWS": 1}


def test_refresh_sees_overwrite_with_added_and_retyped_columns(graph):
    path, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    with pytest.raises(Exception):
        engine.execute("MATCH (p:Person) RETURN p.age")

    replacement = pa.table(
        {"id": [1, 2], "name": [10, 20], "age": [30, 40]}  # name retyped to int
    )
    lance.write_dataset(replacement, path / "person.lance", mode="overwrite")
    assert count_people(engine) == N  # still pinned

    engine.refresh()
    out = engine.execute("MATCH (p:Person) RETURN p.name, p.age")
    assert out.column(0).type == pa.int64()
    assert sorted(out.column(1).to_pylist()) == [30, 40]


def test_refresh_sees_table_replaced_at_same_location(graph):
    path, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})

    # Delete and recreate: the new dataset is version 1 again, like the old one.
    shutil.rmtree(path / "person.lance")
    lance.write_dataset(people(0, 3), path / "person.lance")
    engine.refresh()
    assert count_people(engine) == 3


def test_failed_refresh_keeps_previous_snapshot(graph):
    path, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})

    # Dropping the configured id column must not be accepted.
    lance.write_dataset(
        pa.table({"name": ["x"]}), path / "person.lance", mode="overwrite"
    )
    with pytest.raises(ValueError, match="id"):
        engine.refresh()
    assert count_people(engine) == N
    assert engine.dataset_versions()["Person"] == 1


def test_constructor_rejects_missing_id_column(tmp_path):
    bad = lance.write_dataset(pa.table({"name": ["x"]}), tmp_path / "bad.lance")
    with pytest.raises(ValueError, match="id"):
        CypherEngine(CONFIG, {"Person": bad})


def test_historical_version_is_pinned_and_engines_independent(graph):
    path, person, knows = graph
    lance.write_dataset(people(N, N + 5), path / "person.lance", mode="append")

    old = CypherEngine(
        CONFIG, {"Person": lance.dataset(path / "person.lance", version=1)}
    )
    new = CypherEngine(CONFIG, {"Person": lance.dataset(path / "person.lance")})
    assert count_people(old) == N
    assert count_people(new) == N + 5

    # refresh() moves a historical snapshot to the latest version.
    old.refresh()
    assert count_people(old) == N + 5


def test_lance_dataset_subclass_is_read_lazily(graph):
    path, person, knows = graph

    class Wrapped(lance.LanceDataset):
        pass

    person.__class__ = Wrapped
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    # Treated as Lance (pinned, refreshable), not materialized as Arrow.
    assert engine.dataset_versions()["Person"] == 1
    lance.write_dataset(people(N, N + 1), path / "person.lance", mode="append")
    engine.refresh()
    assert count_people(engine) == N + 1


def test_concurrent_execute_and_refresh(graph):
    path, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    allowed = {N + 5 * i for i in range(6)}
    seen, errors = [], []

    def reader():
        try:
            seen.extend(count_people(engine) for _ in range(20))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    for i in range(1, 6):
        lance.write_dataset(
            people(N + 5 * (i - 1), N + 5 * i), path / "person.lance", mode="append"
        )
        engine.refresh()
    for t in threads:
        t.join()

    assert not errors
    assert set(seen) <= allowed
    assert count_people(engine) == N + 25


def test_execute_batches_streams_all_rows(graph):
    _, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    reader = engine.execute_batches("MATCH (p:Person) RETURN p.id")
    assert isinstance(reader, pa.RecordBatchReader)
    total, checksum, batches = 0, 0, 0
    for batch in reader:
        batches += 1
        total += batch.num_rows
        checksum += pc.sum(batch.column(0)).as_py() or 0
    assert (total, checksum) == (N, sum(range(N)))
    assert batches > 1


def test_execute_batches_partial_consumption(graph):
    _, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    reader = engine.execute_batches("MATCH (p:Person) RETURN p.id")
    assert reader.read_next_batch().num_rows > 0
    del reader  # dropping a half-read stream must not affect the engine
    assert count_people(engine) == N


def test_execute_batches_reports_planning_errors(graph):
    _, person, knows = graph
    engine = CypherEngine(CONFIG, {"Person": person, "KNOWS": knows})
    with pytest.raises(Exception):
        engine.execute_batches("MATCH (p:Unknown) RETURN p.id")


def test_projection_and_filter_pushed_into_lance_scan(tmp_path):
    wide = pa.table(
        {
            "id": list(range(100)),
            "payload": [b"x" * 1_000] * 100,
            "age": list(range(100)),
        }
    )
    ds = lance.write_dataset(wide, tmp_path / "wide.lance")
    engine = CypherEngine(CONFIG, {"Person": ds})
    plan = engine.explain("MATCH (p:Person) WHERE p.age > 90 RETURN p.id")
    scan = next(
        line for line in plan.splitlines() if "LanceRead" in line or "LanceScan" in line
    )
    assert "payload" not in scan
    out = engine.execute("MATCH (p:Person) WHERE p.age > 90 RETURN p.id")
    assert sorted(out.column(0).to_pylist()) == list(range(91, 100))


def test_in_memory_tables_are_split_across_partitions():
    # One large batch would put all work on one thread for non-repartitioning
    # operators; the engine slices it (zero-copy) into several partitions.
    n = 50_000
    people = pa.table({"id": list(range(n)), "name": [f"p{i}" for i in range(n)]})
    engine = CypherEngine(CONFIG, {"Person": people})
    plan = engine.explain("MATCH (p:Person) RETURN p.id")
    sizes = re.search(r"DataSourceExec: partitions=(\d+)", plan)
    assert sizes and int(sizes.group(1)) > 1
    out = engine.execute("MATCH (p:Person) RETURN p.id")
    assert out.num_rows == n and pc.sum(out.column(0)).as_py() == sum(range(n))
    empty = people.slice(0, 0)
    assert (
        CypherEngine(CONFIG, {"Person": empty})
        .execute("MATCH (p:Person) RETURN p.id")
        .num_rows
        == 0
    )
