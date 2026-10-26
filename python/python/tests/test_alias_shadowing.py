"""Output aliases shadow input nodes only after their expressions are evaluated."""

import pytest
from lance_csr import CypherEngine
from test_engine_csr import CONFIG, KNOWS, PEOPLE


@pytest.mark.parametrize("use_csr", [False, True])
@pytest.mark.parametrize(
    ("query", "columns", "expected"),
    [
        (
            "MATCH (p:Person) RETURN p.name AS p ORDER BY p",
            ["p"],
            [(name,) for name in "abcdef"],
        ),
        (
            "MATCH (p:Person) RETURN p.name AS p, count(*) AS n ORDER BY p",
            ["p", "n"],
            [(name, 1) for name in "abcdef"],
        ),
        (
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "RETURN p.name AS p, f.name AS f ORDER BY p, f",
            ["p", "f"],
            [
                ("a", "b"),
                ("a", "b"),
                ("a", "c"),
                ("b", "c"),
                ("c", "c"),
                ("d", None),
                ("e", "a"),
                ("f", None),
            ],
        ),
        (
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "RETURN p.name AS p, count(f) AS n ORDER BY p",
            ["p", "n"],
            list(zip("abcdef", [3, 1, 1, 0, 1, 0], strict=True)),
        ),
        (
            "MATCH (p:Person) RETURN p.name AS p, p.id AS id ORDER BY p",
            ["p", "id"],
            list(zip("abcdef", range(1, 7), strict=True)),
        ),
        (
            "MATCH (p:Person) RETURN p.name AS P ORDER BY p DESC",
            ["p"],
            [(name,) for name in "fedcba"],
        ),
        (
            "MATCH (p:Person) RETURN p.name AS p ORDER BY toUpper(p) DESC",
            ["p"],
            [(name,) for name in "fedcba"],
        ),
        (
            "MATCH (p:Person) WITH p.name AS p ORDER BY p RETURN p",
            ["p"],
            [(name,) for name in "abcdef"],
        ),
        (
            "MATCH (p:Person) RETURN p AS p ORDER BY p",
            ["p.id"],
            [(key,) for key in range(1, 7)],
        ),
    ],
)
def test_output_alias_shadows_node(use_csr, query, columns, expected):
    engine = CypherEngine(CONFIG, {"Person": PEOPLE, "KNOWS": KNOWS}, use_csr=use_csr)
    result = engine.execute(query)
    assert result.column_names == columns
    assert [tuple(row.values()) for row in result.to_pylist()] == expected
