# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The lance-csr Authors

"""A single deadline covers execution and idle time in native Arrow readers."""

import time

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, CypherQuery, GraphConfig

CONFIG = GraphConfig.builder().node_table("Person", "id").build()
TABLES = {"Person": pa.table({"id": list(range(20_000))})}
QUERY = "MATCH (p:Person) RETURN p.id AS id"


@pytest.fixture
def engine():
    return CypherEngine(CONFIG, TABLES)


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf"), 1e300])
@pytest.mark.parametrize("method", ["execute", "execute_batches", "query"])
def test_invalid_timeout(engine, timeout, method):
    with pytest.raises(ValueError, match="timeout"):
        if method == "query":
            CypherQuery(QUERY).with_config(CONFIG).execute(TABLES, timeout=timeout)
        else:
            getattr(engine, method)(QUERY, timeout=timeout)


@pytest.mark.parametrize("method", ["execute", "execute_batches", "query"])
def test_zero_timeout_then_engine_reuse(engine, method):
    with pytest.raises(TimeoutError, match="Query timeout exceeded"):
        if method == "query":
            CypherQuery(QUERY).with_config(CONFIG).execute(TABLES, timeout=0)
        else:
            getattr(engine, method)(QUERY, timeout=0)
    assert engine.execute("RETURN 1 AS n").to_pylist() == [{"n": 1}]


def test_native_reader_deadline_expires_while_idle(engine):
    reader = engine.execute_batches(QUERY, timeout=1.0)
    assert isinstance(reader, pa.RecordBatchReader)
    time.sleep(1.05)
    with pytest.raises(pa.ArrowInvalid, match="Query timeout exceeded"):
        reader.read_next_batch()
    reader.close()
    assert engine.execute("RETURN 1 AS n", timeout=10).to_pylist() == [{"n": 1}]


def test_stream_keeps_one_deadline_between_reads(tmp_path):
    dataset = lance.write_dataset(
        TABLES["Person"], tmp_path / "people.lance", max_rows_per_file=4000
    )
    engine = CypherEngine(CONFIG, {"Person": dataset})
    reader = engine.execute_batches(QUERY, timeout=1.0)
    first = reader.read_next_batch()
    assert 0 < first.num_rows < 20_000
    time.sleep(1.05)
    with pytest.raises(pa.ArrowInvalid, match="Query timeout exceeded"):
        reader.read_all()


def test_no_deadline_and_normal_execution_errors(engine):
    assert engine.execute(QUERY, timeout=None).num_rows == 20_000
    assert engine.execute_batches(QUERY, timeout=10).read_all().num_rows == 20_000
    with pytest.raises(ValueError):
        engine.execute("RETURN unknown", timeout=10)


def test_expensive_query_stops_and_engine_stays_usable(engine):
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="Query timeout exceeded"):
        engine.execute(
            "MATCH (a:Person), (b:Person), (c:Person) "
            "WHERE a.id + b.id + c.id > 30000 RETURN count(*) AS n",
            timeout=0.02,
        )
    assert time.monotonic() - started < 2.0
    assert engine.execute("RETURN 2 AS n").to_pylist() == [{"n": 2}]
