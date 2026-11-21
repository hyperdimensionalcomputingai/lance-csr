"""CypherEngine.export_graph: dense CSR arrays for graph-algorithm libraries."""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig, GraphExport

CONFIG = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)

# Non-contiguous keys in shuffled order, an isolated node (40), a duplicate edge
# (10->20), a self-loop (30->30), an edge to a missing node (99), a null endpoint.
NODES = pa.table({"id": pa.array([60, 10, 50, 30, 20, 40], pa.int64())})
EDGES = pa.table(
    {
        "src": pa.array([10, 10, 20, 30, 30, 10, 50, 60, None], pa.int64()),
        "dst": pa.array([20, 20, 30, 10, 30, 99, 60, 50, 20], pa.int64()),
    }
)


def test_directed_export():
    g = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES}).export_graph()
    assert isinstance(g, GraphExport)
    assert (g.num_nodes, g.directed, g.dropped_edges) == (6, True, 1)
    # Dense ids follow row order: 60->0, 10->1, 50->2, 30->3, 20->4, 40->5.
    assert g.node_keys.to_pylist() == [60, 10, 50, 30, 20, 40]
    assert g.label_ranges == {"Person": (0, 6)}
    assert g.node_keys.type == pa.int64()
    for array in (g.out_indptr, g.out_indices, g.in_indptr, g.in_indices):
        assert array is not None and array.type == pa.uint64()
    assert g.out_indptr.to_pylist() == [0, 1, 2, 3, 5, 6, 6]
    assert g.out_indices.to_pylist() == [2, 4, 0, 1, 3, 3]
    assert g.in_indptr is not None and g.in_indices is not None
    assert g.in_indptr.to_pylist() == [0, 1, 2, 3, 5, 6, 6]
    assert g.in_indices.to_pylist() == [2, 3, 0, 3, 4, 1]


def test_undirected_export():
    g = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES}).export_graph(
        ["knows"], directed=False
    )
    assert g.in_indptr is None and g.in_indices is None
    assert g.out_indptr.to_pylist() == [0, 1, 3, 4, 7, 9, 9]
    assert g.out_indices.to_pylist() == [2, 3, 4, 0, 1, 3, 4, 1, 3]


def test_labels_with_colliding_keys():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("City", "id")
        .rel_table("LIVES_IN", "Person", "City", "src", "dst")
        .build()
    )
    ids = pa.table({"id": [1, 2]})
    lives_in = pa.table({"src": [1, 2], "dst": [2, 1]})
    g = CypherEngine(
        config, {"Person": ids, "City": ids, "LIVES_IN": lives_in}
    ).export_graph()
    assert g.label_ranges == {"City": (0, 2), "Person": (2, 4)}
    assert g.node_keys.to_pylist() == [1, 2, 1, 2]
    assert g.out_indptr.to_pylist() == [0, 0, 0, 1, 2]
    assert g.out_indices.to_pylist() == [1, 0]


def test_errors():
    engine = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES})
    with pytest.raises(ValueError, match="LIKES"):
        engine.export_graph(["LIKES"])
    duplicate = pa.table({"id": [10, 10]})
    with pytest.raises(RuntimeError, match="appears twice"):
        CypherEngine(CONFIG, {"Person": duplicate, "KNOWS": EDGES}).export_graph()
    names = pa.table({"id": ["a", "b"]})
    with pytest.raises(ValueError, match="not an integer"):
        CypherEngine(CONFIG, {"Person": names, "KNOWS": EDGES}).export_graph()


def test_lance_snapshot_and_checkout_latest(tmp_path):
    lance.write_dataset(NODES, tmp_path / "Person.lance")
    lance.write_dataset(EDGES, tmp_path / "KNOWS.lance")
    datasets = {
        name: lance.dataset(tmp_path / f"{name}.lance") for name in ("Person", "KNOWS")
    }
    engine = CypherEngine(CONFIG, datasets)
    assert engine.export_graph().out_indptr.to_pylist() == [0, 1, 2, 3, 5, 6, 6]

    lance.write_dataset(
        pa.table({"src": [40], "dst": [60]}), tmp_path / "KNOWS.lance", mode="append"
    )
    assert (
        engine.export_graph().out_indptr.to_pylist()[-1] == 6
    )  # still the pinned version
    engine.checkout_latest()
    g = engine.export_graph()
    assert g.out_indptr.to_pylist() == [0, 1, 2, 3, 5, 6, 7]
    assert g.out_indices.to_pylist()[-1] == 0


def test_icebug_pagerank_and_components():
    ib = pytest.importorskip("icebug")
    g = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES}).export_graph()
    graph = ib.Graph.fromCSR(
        g.num_nodes, g.directed, g.out_indices, g.out_indptr, g.in_indices, g.in_indptr
    )
    scores = (
        ib.centrality.PageRank(
            graph, distributeSinks=ib.centrality.SinkHandling.DistributeSinks
        )
        .run()
        .scores()
    )
    assert len(scores) == 6 and sum(scores) == pytest.approx(1.0)

    u = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES}).export_graph(
        directed=False
    )
    # Keep the graph referenced: the algorithm object doesn't.
    undirected = ib.Graph.fromCSR(u.num_nodes, False, u.out_indices, u.out_indptr)
    components = ib.components.ConnectedComponents(undirected)
    # {10, 20, 30}, {50, 60} and the isolated 40.
    assert components.run().numberOfComponents() == 3


def test_write_node_column(tmp_path):
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .node_table("City", "id")
        .rel_table("LIVES_IN", "Person", "City", "src", "dst")
        .build()
    )
    lance.write_dataset(pa.table({"id": [1, 2]}), tmp_path / "person.lance")
    lance.write_dataset(pa.table({"id": [1, 2, 3]}), tmp_path / "city.lance")
    lance.write_dataset(
        pa.table({"src": [1, 2], "dst": [2, 2]}), tmp_path / "lives_in.lance"
    )
    engine = CypherEngine(
        config,
        {
            name: lance.dataset(tmp_path / f"{name.lower()}.lance")
            for name in ("Person", "City", "LIVES_IN")
        },
    )
    g = engine.export_graph()
    assert g.label_ranges == {"City": (0, 3), "Person": (3, 5)}
    # In-degree per dense id, as numpy: City 1, 2, 3 then Person 1, 2.
    assert g.in_indptr is not None
    indptr = g.in_indptr.to_numpy()
    engine.write_node_column(g, "in_degree", indptr[1:] - indptr[:-1])

    def degrees(label: str) -> list[tuple[int, int]]:
        rows = engine.execute(
            f"MATCH (n:{label}) RETURN n.id AS id, n.in_degree AS d ORDER BY id"
        ).to_pylist()
        return [(r["id"], r["d"]) for r in rows]

    assert degrees("City") == [(1, 0), (2, 2), (3, 0)]
    assert degrees("Person") == [(1, 0), (2, 0)]
    assert "in_degree" in lance.dataset(tmp_path / "city.lance").schema.names

    with pytest.raises(ValueError, match="already exists"):
        engine.write_node_column(g, "in_degree", [0] * 5)
    engine.write_node_column(
        g, "in_degree", pa.chunked_array([[1, 1], [1, 1, 1]]), overwrite=True
    )
    assert degrees("City") == [(1, 1), (2, 1), (3, 1)]
    with pytest.raises(ValueError, match="Expected 5 values"):
        engine.write_node_column(g, "x", [0] * 4)
    with pytest.raises(ValueError, match="key column"):
        engine.write_node_column(g, "id", [0] * 5, overwrite=True)

    arrow = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES})
    with pytest.raises(ValueError, match="in-memory"):
        arrow.write_node_column(arrow.export_graph(), "x", [0] * 6)


def test_optimize_datasets(tmp_path):
    from datetime import timedelta

    uri = tmp_path / "person.lance"
    lance.write_dataset(NODES, uri)
    lance.write_dataset(EDGES, tmp_path / "knows.lance")
    for key in (70, 80):  # small appends: three fragments
        lance.write_dataset(
            pa.table({"id": pa.array([key], pa.int64())}), uri, mode="append"
        )
    datasets = {
        "Person": lance.dataset(uri),
        "KNOWS": lance.dataset(tmp_path / "knows.lance"),
    }
    engine = CypherEngine(CONFIG, datasets, use_csr=True)
    g = engine.export_graph()
    engine.write_node_column(g, "x", [1] * g.num_nodes)
    query = "MATCH (p:Person) RETURN count(*) AS n, sum(p.x) AS s"
    before = engine.execute(query).to_pylist()

    # The default window keeps every version made just now.
    stats = engine.optimize_datasets()
    assert stats["Person"]["fragments_removed"] == 3
    assert stats["Person"]["fragments_added"] == 1
    assert stats["Person"]["old_versions"] == 0
    assert engine.dataset_versions()["Person"] == lance.dataset(uri).version

    stats = engine.optimize_datasets(cleanup_older_than=timedelta(0))
    assert stats["Person"]["old_versions"] > 0
    assert stats["Person"]["bytes_removed"] > 0
    assert len(lance.dataset(uri).versions()) == 1
    assert engine.execute(query).to_pylist() == before

    with pytest.raises(TypeError, match="timedelta"):
        engine.optimize_datasets(cleanup_older_than=7)  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError, match="negative"):
        engine.optimize_datasets(cleanup_older_than=timedelta(days=-1))
    # PyArrow inputs are skipped.
    arrow = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": EDGES})
    assert arrow.optimize_datasets() == {}
