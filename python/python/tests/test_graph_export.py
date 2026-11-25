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


WEIGHTED = EDGES.append_column("w", pa.array([1, 2, 4, 8, 16, 32, 64, 128, 256]))


def test_weights():
    engine = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": WEIGHTED})
    plain = engine.export_graph()
    assert plain.out_weights is None and plain.in_weights is None

    g = engine.export_graph(weight="W")
    assert g.out_indices.equals(plain.out_indices)
    assert g.out_weights is not None and g.in_weights is not None
    assert g.out_weights.type == pa.float64()
    # The duplicate 10->20 (weights 1 and 2) sums to 3.
    assert g.out_weights.to_pylist() == [128, 3, 64, 8, 16, 4]
    assert g.in_weights.to_pylist() == [64, 8, 128, 16, 4, 3]

    # Undirected: 50->60 and 60->50 combine; the self-loop 30->30 counts once.
    u = engine.export_graph(directed=False, weight="w")
    assert u.in_weights is None and u.out_weights is not None
    assert u.out_weights.to_pylist() == [192, 8, 3, 192, 8, 16, 4, 3, 4]

    # min/max instead: the duplicate 10->20 has 1 and 2, 50-60 has 64 and 128.
    low = engine.export_graph(weight="w", combine_edges="min")
    assert low.out_weights is not None and low.out_weights.to_pylist()[1] == 1
    high = engine.export_graph(directed=False, weight="w", combine_edges="max")
    assert high.out_weights is not None
    assert high.out_weights.to_pylist() == [128, 8, 2, 128, 8, 16, 4, 2, 4]

    # A null weight drops its edge and counts it; unweighted exports ignore it.
    gaps = EDGES.append_column("w", pa.array([1, None, 4, 8, 16, 32, 64, 128, 256]))
    gappy = CypherEngine(CONFIG, {"Person": NODES, "KNOWS": gaps})
    dropped = gappy.export_graph(weight="w")
    assert dropped.dropped_edges == 2 and dropped.out_weights is not None
    assert dropped.out_weights.to_pylist() == [128, 1, 64, 8, 16, 4]
    assert gappy.export_graph().dropped_edges == 1

    with pytest.raises(ValueError, match="cost"):
        engine.export_graph(weight="cost")
    with pytest.raises(ValueError, match="combine_edges"):
        engine.export_graph(weight="w", combine_edges="mean")  # pyright: ignore[reportArgumentType]
    named = EDGES.append_column("w", pa.array(["x"] * 9))
    with pytest.raises(ValueError, match="not a numeric"):
        CypherEngine(CONFIG, {"Person": NODES, "KNOWS": named}).export_graph(weight="w")


def test_weights_per_type_and_default():
    config = (
        GraphConfig.builder()
        .node_table("Person", "id")
        .rel_table("KNOWS", "Person", "Person", "src", "dst")
        .rel_table("LIKES", "Person", "Person", "src", "dst")
        .build()
    )
    # KNOWS has a weight column; LIKES has only an unrelated `since`.
    engine = CypherEngine(
        config,
        {
            "Person": pa.table({"id": [1, 2, 3]}),
            "KNOWS": pa.table({"src": [1, 2], "dst": [2, 3], "w": [5.0, 7.0]}),
            "LIKES": pa.table({"src": [1, 3], "dst": [2, 1], "since": [2020, 2021]}),
        },
    )

    def out_weights(**kwargs) -> list[float]:
        g = engine.export_graph(**kwargs)
        assert g.out_weights is not None
        return g.out_weights.to_pylist()

    # Out-edges: 1->2 (KNOWS 5 and LIKES), 2->3 (KNOWS 7), 3->1 (LIKES).
    assert out_weights(weight="w", default_weight=1.0) == [6, 7, 1]
    assert out_weights(weight={"KNOWS": "w", "likes": "since"}) == [2025, 7, 2021]
    assert out_weights(weight={"KNOWS": "w"}, default_weight=2.0) == [7, 7, 2]
    assert engine.export_graph(["LIKES"], weight={"LIKES": "since"}).num_nodes == 3

    with pytest.raises(ValueError, match="'LIKES'"):
        engine.export_graph(weight="w")
    with pytest.raises(ValueError, match="'LIKES'"):
        engine.export_graph(weight={"KNOWS": "w"})
    with pytest.raises(ValueError, match="not found in 'LIKES'"):
        engine.export_graph(weight={"LIKES": "w"}, default_weight=1.0)
    with pytest.raises(ValueError, match="FOLLOWS"):
        engine.export_graph(weight={"FOLLOWS": "w"}, default_weight=1.0)
    with pytest.raises(ValueError, match="any exported"):
        engine.export_graph(weight="cost", default_weight=1.0)
    with pytest.raises(ValueError, match="default_weight needs weight"):
        engine.export_graph(default_weight=1.0)
    with pytest.raises(TypeError, match="weight"):
        engine.export_graph(weight=3)  # pyright: ignore[reportArgumentType]


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


def test_icebug_weighted_shortest_paths():
    ib = pytest.importorskip("icebug")
    # 0->1 and 1->2 cost 1 each; the direct 0->2 costs 5.
    nodes = pa.table({"id": [0, 1, 2]})
    edges = pa.table({"src": [0, 1, 0], "dst": [1, 2, 2], "cost": [1.0, 1.0, 5.0]})
    g = CypherEngine(CONFIG, {"Person": nodes, "KNOWS": edges}).export_graph(
        weight="cost"
    )
    graph = ib.Graph.fromCSR(
        g.num_nodes,
        g.directed,
        g.out_indices,
        g.out_indptr,
        g.in_indices,
        g.in_indptr,
        g.out_weights,
        g.in_weights,
    )
    assert graph.isWeighted()
    dijkstra = ib.distance.Dijkstra(graph, 0)
    dijkstra.run()
    assert dijkstra.getDistances() == [0.0, 1.0, 2.0]


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
