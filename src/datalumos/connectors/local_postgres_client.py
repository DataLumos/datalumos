import psycopg2
from psycopg2 import sql
from collections import namedtuple

Column = namedtuple('Column', ['name', 'data_type'])


class TableProperties:
    def __init__(self, table_name: str, schema: str, total_rows: int, column_stats: list[dict]) -> None:
        self.table_name = table_name
        self.schema = schema
        self.total_rows = total_rows
        self.column_stats = column_stats


class PostgresDB:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn: psycopg2.extensions.connection | None = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def get_column_names(self, table: str, schema: str) -> list[Column]:
        self.connect()
        with self.conn.cursor() as cur:
            query = sql.SQL(f"""
                SELECT column_name,
                data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}' and table_name = '{table}'
                ORDER BY table_schema, table_name, ordinal_position;
            """)
            cur.execute(query)
            columns = [Column(name=row[0], data_type=row[1])
                       for row in cur.fetchall()]
        return columns

    def get_table_stats(self, table: str, schema: str) -> TableProperties:
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
                null_query = sql.SQL(f"""
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE {col.name} IS NOT NULL
                """)
                cur.execute(null_query)
                non_null_count = cur.fetchone()[0]

                # Calculate fill rate
                fill_rate = (non_null_count / total_count *
                             100) if total_count > 0 else 0

                # Get distinct values count
                distinct_query = sql.SQL(f"""
                    SELECT COUNT(DISTINCT {col.name})
                    FROM {schema}.{table}
                """)
                cur.execute(distinct_query)
                distinct_count = cur.fetchone()[0]

                column_stats.append({
                    'column_name': col.name,
                    'data_type': col.data_type,
                    'total_rows': total_count,
                    'non_null_count': non_null_count,
                    'fill_rate': round(fill_rate, 2),
                    'distinct_count': distinct_count
                })

            return TableProperties(table, schema, total_count, column_stats)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
