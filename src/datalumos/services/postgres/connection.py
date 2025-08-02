from collections import namedtuple

import psycopg2
from psycopg2 import sql

from datalumos.services.postgres.config import DEFAULT_POSTGRES_CONFIG, PostgreSQLConfig

Column = namedtuple("Column", ["name", "data_type"])


class TableProperties:
    def __init__(
        self, table_name: str, schema: str, total_rows: int, column_stats: list[dict]
    ) -> None:
        self.table_name = table_name
        self.schema = schema
        self.total_rows = total_rows
        self.column_stats = column_stats


class PostgresDB:
    """Database inspector tool for PostgreSQL databases."""

    def __init__(
        self,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        config: PostgreSQLConfig | None = None,
    ):
        """Initialize PostgresDB with connection parameters.

        Args:
            dbname: Database name (falls back to config)
            user: Username (falls back to config)
            password: Password (falls back to config)
            host: Host (falls back to config)
            port: Port (falls back to config)
            config: PostgreSQLConfig instance (falls back to DEFAULT_POSTGRES_CONFIG)
        """
        if config is None:
            config = DEFAULT_POSTGRES_CONFIG

        self.dbname = dbname or config.database
        self.user = user or config.username
        self.password = password or config.password
        self.host = host or config.host
        self.port = port or config.port
        self.conn: psycopg2.extensions.connection | None = None

    def connect(self):
        """Establish database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def get_column_names(self, table: str, schema: str) -> list[Column]:
        """Get column names and data types for a table.

        Args:
            table: Table name
            schema: Schema name

        Returns:
            List of Column namedtuples with name and data_type
        """
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(
                f"""
                SELECT column_name,
                data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}' and table_name = '{table}'
                ORDER BY table_schema, table_name, ordinal_position;
            """
            )
            cur.execute(query)
            columns = [
                Column(name=row[0], data_type=row[1])
                for row in cur.fetchall()
                if not row[0].startswith("_")
            ]
        return columns

    def get_random_sample(
        self, table: str, schema: str, sample_size: int
    ) -> list[dict]:
        """Get a random sample of rows from a table. Include the column names in the
        result."""
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(
                f"SELECT * FROM {schema}.{table} ORDER BY RANDOM() LIMIT {sample_size}"
            )
            cur.execute(query)
            return [
                dict(zip([col[0] for col in cur.description], row, strict=False))
                for row in cur.fetchall()
            ]

    def get_count_distinct_values(self, column: str, table: str, schema: str) -> int:
        """Get the count of distinct values for a column."""
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(f"SELECT COUNT(DISTINCT {column}) FROM {schema}.{table}")
            cur.execute(query)
            return cur.fetchone()[0]

    def get_distinct_values(self, column: str, table: str, schema: str) -> list[str]:
        """Get all distinct values for a column."""
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(f"SELECT DISTINCT {column} FROM {schema}.{table}")
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    def get_sample_values(
        self, column: str, table: str, schema: str, limit: int
    ) -> list[str]:
        """Get a sample of distinct values for a column."""
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(
                f"SELECT {column} FROM (SELECT DISTINCT {column} FROM {schema}.{table}) AS sub ORDER BY RANDOM() LIMIT {limit}"  # noqa: E501
            )
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    def get_column_type(self, column: str, table: str, schema: str) -> str:
        """Get the type of te column"""
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(
                f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                AND column_name = '{column}'
                """
            )
            cur.execute(query)
            result = cur.fetchone()
            return result[0] if result else None

    def get_table_stats(self, table: str, schema: str) -> TableProperties:
        """Get comprehensive table statistics including column-level stats.

        Args:
            table: Table name
            schema: Schema name

        Returns:
            TableProperties object with table and column statistics
        """
        self.connect()
        with self.conn.cursor() as cur:
            # Get total row count
            count_query = sql.SQL(f"SELECT COUNT(*) FROM {schema}.{table}")
            cur.execute(count_query)
            total_count = cur.fetchone()[0]

            # Get column stats
            column_stats = []
            columns = self.get_column_names(table, schema)

            for col in columns:
                # Get non-null count
                null_query = sql.SQL(
                    f"""
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE {col.name} IS NOT NULL
                """
                )
                cur.execute(null_query)
                non_null_count = cur.fetchone()[0]

                # Calculate fill rate
                fill_rate = (
                    (non_null_count / total_count * 100) if total_count > 0 else 0
                )

                # Get distinct values count
                distinct_query = sql.SQL(
                    f"""
                    SELECT COUNT(DISTINCT {col.name})
                    FROM {schema}.{table}
                """
                )
                cur.execute(distinct_query)
                distinct_count = cur.fetchone()[0]

                column_stats.append(
                    {
                        "column_name": col.name,
                        "data_type": col.data_type,
                        "total_rows": total_count,
                        "non_null_count": non_null_count,
                        "fill_rate": round(fill_rate, 2),
                        "distinct_count": distinct_count,
                    }
                )

            return TableProperties(table, schema, total_count, column_stats)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
