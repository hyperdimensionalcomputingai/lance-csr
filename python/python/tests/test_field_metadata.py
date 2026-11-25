# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Returned properties keep their source field's Arrow metadata, such as a GeoArrow
extension name, whatever the number of hops and whether CSR is on."""

import lance
import pyarrow as pa
import pytest
from lance_csr import CypherEngine, GraphConfig

POINT = {b"ARROW:extension:name": b"geoarrow.point"}
WKB = {b"ARROW:extension:name": b"geoarrow.wkb"}
CONFIG = (
    GraphConfig.builder()
    .node_table("Place", "place_id")
    .node_table("Visit", "visit_id")
    .rel_table("AT", "Visit", "Place", "src", "dst")
    .build()
)
POINTS = [("here", POINT), ("xy", POINT), ("wkb", WKB)]
QUERIES = [
    ("MATCH (p:Place) RETURN p.location AS here, p.xy AS xy, p.wkb AS wkb", POINTS),
    (
        "MATCH (v:Visit)-[:AT]->(p:Place) "
        "RETURN v.day AS day, p.location AS here, p.xy AS xy, p.wkb AS wkb",
        POINTS,
    ),
    ("MATCH (v:Visit)-[:AT]->(p:Place) RETURN p.location", [("p.location", POINT)]),
    ("MATCH (v:Visit)-[:AT*1..2]->(p:Place) RETURN p.location AS here", POINTS[:1]),
    ("MATCH (v:Visit)-[a:AT]->(p:Place) RETURN a.route AS route", [("route", WKB)]),
]


@pytest.fixture(scope="module")
def datasets(tmp_path_factory):
    path = tmp_path_factory.mktemp("graph")
    xy = pa.struct([("x", pa.float64()), ("y", pa.float64())])
    place = pa.table(
        [
            pa.array([1, 2], pa.int64()),
            pa.array([[0.0, 1.0], [2.0, 3.0]], pa.list_(pa.float64(), 2)),
            pa.array([{"x": 0.0, "y": 1.0}, {"x": 2.0, "y": 3.0}], xy),
            pa.array([b"\x01", b"\x02"], pa.binary()),
        ],
        schema=pa.schema(
            [
                pa.field("place_id", pa.int64()),
                pa.field("location", pa.list_(pa.float64(), 2), metadata=POINT),
                pa.field("xy", xy, metadata=POINT),
                pa.field("wkb", pa.binary(), metadata=WKB),
            ]
        ),
    )
    visit = pa.table(
        {"visit_id": pa.array([10, 11], pa.int64()), "day": ["mon", "tue"]}
    )
    at = pa.table(
        [
            pa.array([10, 11], pa.int64()),
            pa.array([1, 2], pa.int64()),
            pa.array([b"\x03", b"\x04"], pa.binary()),
        ],
        schema=pa.schema(
            [
                pa.field("src", pa.int64()),
                pa.field("dst", pa.int64()),
                pa.field("route", pa.binary(), metadata=WKB),
            ]
        ),
    )
    tables = {"Place": place, "Visit": visit, "AT": at}
    return {name: lance.write_dataset(t, path / name) for name, t in tables.items()}


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(("query", "expected"), QUERIES)
def test_returned_properties_keep_field_metadata(datasets, use_csr, query, expected):
    schema = CypherEngine(CONFIG, datasets, use_csr=use_csr).execute(query).schema
    for name, metadata in expected:
        assert schema.field(name).metadata == metadata, name
