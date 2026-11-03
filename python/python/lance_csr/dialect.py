"""The SQL dialects Cypher queries export to."""

from enum import IntEnum


class SqlDialect(IntEnum):
    """The SQL dialect [`CypherQuery.to_sql`][lance_csr.CypherQuery.to_sql] writes.

    Attributes
    ----------
    Default
        Generic DataFusion SQL.
    Spark
        Spark SQL.
    PostgreSql
        PostgreSQL.
    MySql
        MySQL.
    Sqlite
        SQLite.
    """

    Default = 0
    Spark = 1
    PostgreSql = 2
    MySql = 3
    Sqlite = 4
