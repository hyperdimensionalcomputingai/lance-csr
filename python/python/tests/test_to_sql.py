# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Tests for the to_sql API that converts Cypher queries to SQL."""

import pyarrow as pa
import pytest
from lance_csr import CypherQuery, GraphConfig, SqlDialect, SqlEngine


@pytest.fixture
def scalar_export_env():
    config = GraphConfig.builder().node_table("Person", "id").build()
    datasets = {
        "Person": pa.table(
            {
                "id": [1, 2, 3],
                "number": pa.array([-1.5, None, 2.0], pa.float32()),
                "integer": pa.array([-100, None, 2], pa.int8()),
                "text": pa.array(["a猫😀z", None, ""], pa.large_string()),
                "items": pa.array([[1, None], None, []], pa.list_(pa.int64())),
                "nested": pa.array(
                    [[[1, 2], [3]], None, []], pa.list_(pa.list_(pa.int64()))
                ),
                "end": pa.array([3, None, -1], pa.int8()),
            }
        )
    }
    return config, datasets


@pytest.mark.parametrize(
    "expression",
    [
        "size(p.items)",
        "size(p.nested)",
        "range(1, p.end)",
        "range(p.end, 1, -1)",
        "size(range(1, p.end))",
        "toUpper(p.text)",
        "round(-1.5)",
        "round(null)",
        "abs(null)",
        "sqrt(null)",
        "floor(null)",
        "ceil(null)",
        "toUpper(null)",
        "null",
        "left('a猫😀z', 3)",
    ],
)
def test_sql_export_preserves_scalar_values_and_types(scalar_export_env, expression):
    config, datasets = scalar_export_env
    query = CypherQuery(
        f"MATCH (p:Person) RETURN p.id AS id, {expression} AS value ORDER BY id"
    ).with_config(config)
    cypher = query.execute(datasets)
    sql = SqlEngine(datasets).execute(query.to_sql(datasets))
    assert sql.to_pylist() == cypher.to_pylist()
    assert sql.schema.types == cypher.schema.types


@pytest.mark.parametrize(
    ("expression", "function", "expected"),
    [
        ("round(p.number)", "round", [-1.0, None, 2.0]),
        ("abs(p.integer)", "abs", [100, None, 2]),
        ("left(p.text, 3)", "left", ["a猫😀", None, ""]),
        ("size(p.text)", "size", [4, None, 0]),
        ("reverse(p.text)", "reverse", ["z😀猫a", None, ""]),
    ],
)
def test_sql_export_rejects_runtime_only_cypher_functions(
    scalar_export_env, expression, function, expected
):
    config, datasets = scalar_export_env
    query = CypherQuery(
        f"MATCH (p:Person) RETURN p.id AS id, {expression} AS value ORDER BY id"
    ).with_config(config)
    result = query.execute(datasets)
    assert result.column("value").to_pylist() == expected
    with pytest.raises(
        NotImplementedError, match=f"SQL export of Cypher function '{function}'"
    ):
        query.to_sql(datasets)

    # A same-named SQL function can differ in values or Arrow result width.
    engine = SqlEngine(datasets)
    if function == "round":
        assert engine.execute(
            "SELECT round(number) AS value FROM person ORDER BY id"
        ).column("value").to_pylist() == [-2.0, None, 2.0]
    if function == "abs":
        sql = engine.execute("SELECT abs(integer) AS value FROM person ORDER BY id")
        assert sql.schema.field("value").type == pa.int8()
        assert result.schema.field("value").type == pa.int64()


@pytest.mark.parametrize(
    "dialect",
    [SqlDialect.Spark, SqlDialect.PostgreSql, SqlDialect.MySql, SqlDialect.Sqlite],
)
def test_sql_export_typed_null_is_bare_null_outside_default(scalar_export_env, dialect):
    # Only the Default dialect has arrow_cast; elsewhere typed nulls export as NULL,
    # so ordinary queries such as coalesce(..., null) keep exporting.
    config, datasets = scalar_export_env
    for text in [
        "MATCH (p:Person) RETURN round(null) AS value",
        "MATCH (p:Person) RETURN coalesce(p.text, null) AS value",
        "MATCH (p:Person) RETURN CASE WHEN p.id > 1 THEN p.text ELSE null END AS value",
        "MATCH (p:Person) RETURN null AS value",
    ]:
        sql = CypherQuery(text).with_config(config).to_sql(datasets, dialect)
        assert "NULL" in sql.upper(), sql
        assert "arrow_cast" not in sql, sql


def test_sql_export_checks_filter_functions(scalar_export_env):
    config, datasets = scalar_export_env
    query = CypherQuery(
        "MATCH (p:Person) WHERE round(p.number) = -1 RETURN p.id"
    ).with_config(config)
    with pytest.raises(
        NotImplementedError, match="SQL export of Cypher function 'round'"
    ):
        query.to_sql(datasets)


@pytest.fixture
def knowledge_graph_env():
    """Create a complex knowledge graph with multiple entity types and relationships."""
    # Authors and their publications
    authors_table = pa.table(
        {
            "author_id": [1, 2, 3, 4, 5],
            "name": [
                "Alice Chen",
                "Bob Smith",
                "Carol Wang",
                "David Lee",
                "Eve Martinez",
            ],
            "institution": ["MIT", "Stanford", "CMU", "Berkeley", "MIT"],
            "h_index": [45, 38, 52, 41, 29],
            "country": ["USA", "USA", "USA", "USA", "Spain"],
        }
    )

    papers_table = pa.table(
        {
            "paper_id": [101, 102, 103, 104, 105, 106],
            "title": [
                "Deep Learning Advances",
                "Graph Neural Networks",
                "Transformer Architecture",
                "Reinforcement Learning",
                "Computer Vision Methods",
                "Natural Language Processing",
            ],
            "year": [2020, 2021, 2019, 2022, 2021, 2020],
            "citations": [450, 320, 890, 210, 380, 520],
            "venue": ["NeurIPS", "ICML", "NeurIPS", "ICLR", "CVPR", "ACL"],
        }
    )

    authorship_table = pa.table(
        {
            "author_id": [1, 1, 2, 2, 3, 3, 4, 5, 5],
            "paper_id": [101, 102, 102, 103, 103, 104, 105, 105, 106],
            "author_position": [1, 1, 2, 1, 2, 1, 1, 2, 1],
        }
    )

    citations_table = pa.table(
        {
            "citing_paper_id": [102, 103, 104, 104, 105, 106],
            "cited_paper_id": [101, 101, 102, 103, 103, 101],
        }
    )

    config = (
        GraphConfig.builder()
        .node_table("Author", "author_id")
        .node_table("Paper", "paper_id")
        .rel_table("AUTHORED", "Author", "Paper", "author_id", "paper_id")
        .rel_table("CITES", "Paper", "Paper", "citing_paper_id", "cited_paper_id")
        .build()
    )

    datasets = {
        "Author": authors_table,
        "Paper": papers_table,
        "AUTHORED": authorship_table,
        "CITES": citations_table,
    }

    return config, datasets


def test_multi_hop_relationship_with_aggregation(knowledge_graph_env):
    """Test complex multi-hop query with aggregation and filtering.

    Find authors who have written highly cited papers (>400 citations) and count
    how many such papers they have, filtering for prolific authors.
    """
    config, datasets = knowledge_graph_env
    query = CypherQuery(
        """
        MATCH (a:Author)-[:AUTHORED]->(p:Paper)
        WHERE p.citations > 400
        RETURN a.name, a.institution, COUNT(*) AS high_impact_papers
        ORDER BY high_impact_papers DESC
        """
    ).with_config(config)

    sql = query.to_sql(datasets)

    assert isinstance(sql, str)
    sql_upper = sql.upper()
    assert "SELECT" in sql_upper
    assert "JOIN" in sql_upper
    assert "WHERE" in sql_upper
    assert "COUNT" in sql_upper
    assert "GROUP BY" in sql_upper
    assert "ORDER BY" in sql_upper


def test_citation_network_analysis(knowledge_graph_env):
    """Test citation network traversal with multiple joins.

    Find papers that cite other papers, along with author information,
    filtered by venue and year range.
    """
    config, datasets = knowledge_graph_env
    query = CypherQuery(
        """
        MATCH (citing:Paper)-[:CITES]->(cited:Paper)
        WHERE citing.year >= 2020 AND citing.venue = 'NeurIPS'
        RETURN citing.title, cited.title, citing.year, cited.citations
        ORDER BY cited.citations DESC
        LIMIT 10
        """
    ).with_config(config)

    sql = query.to_sql(datasets)

    assert isinstance(sql, str)
    sql_upper = sql.upper()
    assert "SELECT" in sql_upper
    assert "JOIN" in sql_upper
    assert "WHERE" in sql_upper
    assert "ORDER BY" in sql_upper
    assert "LIMIT" in sql_upper


def test_collaborative_network_query(knowledge_graph_env):
    """Test finding collaboration patterns through shared papers.

    Find pairs of authors who have co-authored papers, with filtering
    on institution and h-index.
    """
    config, datasets = knowledge_graph_env
    query = CypherQuery(
        """
        MATCH (a1:Author)-[:AUTHORED]->(p:Paper)<-[:AUTHORED]-(a2:Author)
        WHERE a1.author_id < a2.author_id
          AND a1.institution = 'MIT'
          AND a2.h_index > 30
        RETURN DISTINCT a1.name, a2.name, p.title, p.year
        ORDER BY p.year DESC
        """
    ).with_config(config)

    sql = query.to_sql(datasets)

    assert isinstance(sql, str)
    sql_upper = sql.upper()
    assert "SELECT" in sql_upper
    # DISTINCT may be converted to GROUP BY by the SQL unparser
    assert "DISTINCT" in sql_upper or "GROUP BY" in sql_upper
    assert "JOIN" in sql_upper
    assert "WHERE" in sql_upper
    assert "ORDER BY" in sql_upper


def test_to_sql_without_config_raises_error(knowledge_graph_env):
    """Test that to_sql fails gracefully without config."""
    _, datasets = knowledge_graph_env
    query = CypherQuery("MATCH (a:Author) RETURN a.name")

    with pytest.raises(Exception):
        query.to_sql(datasets)
