"""PostgreSQL source connector for Data Lumos."""

from typing import Dict, Any, Optional, List
from dlt.sources.sql_database import sql_database
from datalumos.connectors.utils import logger, sanitize_table_name


def create_postgres_source(config: Dict[str, Any]):
    """
    Create a PostgreSQL source for dlt pipeline.
    
    Args:
        config: Configuration dictionary containing:
            - connection_string: PostgreSQL connection string
            - tables: Optional list of table names to load (loads all if not specified)
            - schema: Optional schema name (defaults to 'public')
    
    Returns:
        dlt source object for PostgreSQL
    """
    connection_string = config["connection_string"]
    tables = config.get("tables")
    schema = config.get("schema", "public")
    
    logger.info(f"Creating PostgreSQL source for schema: {schema}")
    if tables:
        logger.info(f"Loading specific tables: {', '.join(tables)}")
    else:
        logger.info("Loading all tables from the database")
    
    # Create the SQL database source
    source = sql_database(
        credentials=connection_string,
        schema=schema,
        table_names=tables,  # If None, loads all tables
        chunk_size=10000,  # Process in chunks for better memory usage
    )
    
    # Add metadata to help with naming
    source._table_prefix = schema if schema != "public" else None
    
    return source


def list_postgres_tables(connection_string: str, schema: str = "public") -> List[str]:
    """
    List available tables in a PostgreSQL database.
    
    Args:
        connection_string: PostgreSQL connection string
        schema: Schema name (defaults to 'public')
    
    Returns:
        List of table names
    """
    try:
        import psycopg2
        from psycopg2 import sql
        
        # Parse connection string to get connection parameters
        import urllib.parse as urlparse
        parsed = urlparse.urlparse(connection_string)
        
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path.lstrip('/'),
            user=parsed.username,
            password=parsed.password
        )
        
        with conn.cursor() as cursor:
            query = sql.SQL(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE'"
            )
            cursor.execute(query, (schema,))
            tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return tables
        
    except ImportError:
        logger.warning("psycopg2 not available. Cannot list tables directly.")
        return []
    except Exception as e:
        logger.error(f"Error listing PostgreSQL tables: {e}")
        return []


def validate_postgres_connection(connection_string: str) -> bool:
    """
    Test PostgreSQL connection.
    
    Args:
        connection_string: PostgreSQL connection string
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        import psycopg2
        import urllib.parse as urlparse
        
        parsed = urlparse.urlparse(connection_string)
        
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path.lstrip('/'),
            user=parsed.username,
            password=parsed.password
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
        
        conn.close()
        return result is not None
        
    except ImportError:
        logger.warning("psycopg2 not available. Cannot test connection.")
        return False
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {e}")
        return False 