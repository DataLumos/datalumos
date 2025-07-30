from collections import namedtuple

import psycopg2
from psycopg2 import sql

from datalumos.services.postgres.config import PostgreSQLConfig, DEFAULT_POSTGRES_CONFIG

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
        dbname: str = None,
        user: str = None,
        password: str = None,
        host: str = None,
        port: int = None,
        config: PostgreSQLConfig = None,
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
            columns = [Column(name=row[0], data_type=row[1]) for row in cur.fetchall()]
        return columns

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
