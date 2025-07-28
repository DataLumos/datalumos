"""Main entry point for Data Lumos connectors."""

from typing import Dict, Any, Optional, List
import dlt
from datalumos.connectors.config import config
import psycopg2
from datalumos.connectors.utils import (
    logger,
    validate_source_type,
    validate_postgres_config,
    validate_s3_config,
    validate_filesystem_config,
    extract_source_name,
)


class DataLoadError(Exception):
    """Custom exception for data loading errors."""
    pass


def _extract_load_metadata(load_info, pipeline=None) -> Dict[str, Any]:
    """Extract metadata from dlt load info."""
    metadata = {
        "total_tables": 0,
        "total_rows": 0,
        "total_size_bytes": 0,
        "total_size_mb": 0,
        "table_details": [],
        "load_duration": None,
    }

    try:
        if not load_info or not load_info.load_packages:
            return metadata

        table_names = []
        for package in load_info.load_packages:
            if package.schema and package.schema.tables:
                metadata["total_tables"] = len(package.schema.tables)
                table_names.extend(package.schema.tables.keys())

                # Try to extract row counts from jobs first
                if hasattr(package, 'jobs') and package.jobs:
                    for job in package.jobs:
                        # Try different ways to get row count
                        rows = 0
                        size_bytes = 0
                        table_name = "unknown"

                        # Get table name
                        if hasattr(job, 'job_file_info') and job.job_file_info:
                            table_name = job.job_file_info.table_name or "unknown"
                            size_bytes = getattr(
                                job.job_file_info, 'file_size', 0) or 0

                        # Try to get row count from job state/metrics
                        if hasattr(job, '_job_stats') and job._job_stats:
                            rows = job._job_stats.get('rows', 0)
                        elif hasattr(job, 'metrics') and job.metrics:
                            rows = job.metrics.get(
                                'rows', 0) or job.metrics.get('rows_loaded', 0)
                        elif hasattr(job, 'state') and hasattr(job.state, 'rows'):
                            rows = job.state.rows or 0

                        metadata["total_rows"] += rows
                        metadata["total_size_bytes"] += size_bytes

                        # Find or create table detail
                        table_detail = next(
                            (t for t in metadata["table_details"]
                             if t["name"] == table_name),
                            None
                        )
                        if not table_detail:
                            table_detail = {
                                "name": table_name,
                                "rows": 0,
                                "size_bytes": 0,
                                "size_mb": 0,
                                "files_processed": 0,
                            }
                            metadata["table_details"].append(table_detail)

                        table_detail["rows"] += rows
                        table_detail["size_bytes"] += size_bytes
                        table_detail["files_processed"] += 1

        # If no row counts found from dlt jobs, query the database directly
        if metadata["total_rows"] == 0 and pipeline and table_names:
            logger.info(
                "dlt metadata doesn't contain row counts, querying database directly...")
            try:
                db_metadata = _query_database_metadata(pipeline, table_names)
                metadata.update(db_metadata)
                logger.info(
                    f"Retrieved row counts from database: {metadata['total_rows']:,} total rows")
            except Exception as e:
                logger.warning(f"Could not query database for row counts: {e}")

        # If still no table details, create basic entries
        if not metadata["table_details"] and table_names:
            for table_name in table_names:
                metadata["table_details"].append({
                    "name": table_name,
                    "rows": 0,  # Will be filled by database query if available
                    "size_bytes": 0,
                    "size_mb": 0,
                    "files_processed": 1,
                })

        # Convert bytes to MB
        metadata["total_size_mb"] = round(
            metadata["total_size_bytes"] / (1024 * 1024), 2)
        for table in metadata["table_details"]:
            table["size_mb"] = round(table["size_bytes"] / (1024 * 1024), 2)

    except Exception as e:
        logger.warning(f"Could not extract detailed metadata: {e}")

    return metadata


def _query_database_metadata(pipeline, table_names: List[str]) -> Dict[str, Any]:
    """Query the database directly to get row counts and sizes."""

    metadata = {
        "total_rows": 0,
        "total_size_bytes": 0,
        "table_details": []
    }

    try:
        # Get database connection info
        dest_config = config.get_destination_config()
        credentials = dest_config["credentials"]

        # Connect to database
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            database=credentials["database"],
            user=credentials["username"],
            password=credentials["password"]
        )

        with conn.cursor() as cursor:
            dataset_name = pipeline.dataset_name

            for table_name in table_names:
                try:
                    # Query row count
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {dataset_name}.{table_name}"
                    )
                    row_count = cursor.fetchone()[0]

                    # Query table size (approximate)
                    cursor.execute("""
                        SELECT pg_total_relation_size(c.oid) as size_bytes
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s AND n.nspname = %s
                    """, (table_name, dataset_name))

                    size_result = cursor.fetchone()
                    size_bytes = size_result[0] if size_result else 0

                    metadata["total_rows"] += row_count
                    metadata["total_size_bytes"] += size_bytes

                    # Update or create table detail
                    table_detail = next(
                        (t for t in metadata["table_details"]
                         if t["name"] == table_name),
                        None
                    )
                    if not table_detail:
                        table_detail = {
                            "name": table_name,
                            "rows": 0,
                            "size_bytes": 0,
                            "size_mb": 0,
                            "files_processed": 1,
                        }
                        metadata["table_details"].append(table_detail)

                    table_detail["rows"] = row_count
                    table_detail["size_bytes"] = size_bytes

                except Exception as e:
                    logger.warning(
                        f"Could not query metadata for table {table_name}: {e}")
                    continue

        conn.close()

    except Exception as e:
        logger.warning(f"Database metadata query failed: {e}")
        raise

    return metadata


def load_data(
    source_type: str,
    source_config: Dict[str, Any],
    dataset_name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load data from various sources into local PostgreSQL database.

    Args:
        source_type: Type of source ('postgres', 's3', 'filesystem')
        source_config: Configuration dictionary for the source
        dataset_name: Optional custom dataset name
        pipeline_name: Optional custom pipeline name

    Returns:
        Dictionary containing load information and results

    Raises:
        DataLoadError: If loading fails
        ValueError: If configuration is invalid
    """
    logger.info(f"Starting data load from {source_type}")

    try:
        # Validate inputs
        validate_source_type(source_type)
        _validate_source_config(source_type, source_config)

        # Extract source name for naming
        source_name = extract_source_name(source_config, source_type)

        # Generate names if not provided
        if not dataset_name:
            dataset_name = config.get_dataset_name(source_type, source_name)
        if not pipeline_name:
            pipeline_name = config.get_pipeline_name(source_type, source_name)

        logger.info(
            f"Using pipeline: {pipeline_name}, dataset: {dataset_name}")

        # Create the appropriate source
        source = _create_source(source_type, source_config)

        # Create and configure the pipeline
        dest_config = config.get_destination_config()

        # Add loading configuration for better error handling
        dest_config["loader_file_format"] = "jsonl"

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=dest_config["destination"],
            dataset_name=dataset_name
        )

        logger.info("Starting data extraction and load...")

        # Run the pipeline with credentials
        load_info = pipeline.run(
            source, credentials=dest_config["credentials"])

        # Extract detailed metadata
        metadata = _extract_load_metadata(load_info, pipeline)

        # Log results
        logger.info(f"Load completed successfully!")
        logger.info(f"Tables created: {metadata['total_tables']}")
        logger.info(f"Total rows: {metadata['total_rows']:,}")
        logger.info(f"Total size: {metadata['total_size_mb']:.2f} MB")

        # Get table names from schema
        table_names = []
        if load_info.load_packages:
            for package in load_info.load_packages:
                if package.schema and package.schema.tables:
                    table_names.extend(package.schema.tables.keys())

        # Prepare response
        result = {
            "status": "success",
            "pipeline_name": pipeline_name,
            "dataset_name": dataset_name,
            "source_type": source_type,
            "load_info": load_info,
            "table_names": table_names,
            "metadata": metadata,
        }

        if result["table_names"]:
            logger.info(f"Tables created: {', '.join(result['table_names'])}")

        return result

    except Exception as e:
        error_msg = f"Failed to load data from {source_type}: {str(e)}"
        logger.error(error_msg)
        raise DataLoadError(error_msg) from e


def _validate_source_config(source_type: str, source_config: Dict[str, Any]) -> None:
    """Validate source configuration based on source type."""
    validators = {
        "postgres": validate_postgres_config,
        "s3": validate_s3_config,
        "filesystem": validate_filesystem_config,
    }

    validator = validators.get(source_type)
    if validator:
        validator(source_config)


def _create_source(source_type: str, source_config: Dict[str, Any]):
    """Create the appropriate dlt source based on source type."""
    if source_type == "postgres":
        from datalumos.connectors.sources.postgres_source import create_postgres_source
        return create_postgres_source(source_config)

    elif source_type == "s3":
        from datalumos.connectors.sources.s3_source import create_s3_source
        return create_s3_source(source_config)

    elif source_type == "filesystem":
        from datalumos.connectors.sources.filesystem_source import create_filesystem_source
        return create_filesystem_source(source_config)

    else:
        raise ValueError(f"Unknown source type: {source_type}")


def get_pipeline_info(pipeline_name: str) -> Optional[Dict[str, Any]]:
    """Get information about an existing pipeline."""
    try:
        pipeline = dlt.pipeline(pipeline_name)
        return {
            "name": pipeline.pipeline_name,
            "destination": pipeline.destination,
            "dataset_name": pipeline.dataset_name,
            "last_load_info": pipeline.last_load_info,
        }
    except Exception as e:
        logger.warning(
            f"Could not retrieve pipeline info for {pipeline_name}: {e}")
        return None


def save_load_metadata(pipeline_name: str, metadata: Dict[str, Any]) -> None:
    """Save load metadata to a metadata table in PostgreSQL."""
    try:
        import dlt
        from datetime import datetime

        # Create a simple metadata record
        metadata_record = {
            "pipeline_name": pipeline_name,
            "load_timestamp": datetime.utcnow().isoformat(),
            "total_tables": metadata.get("total_tables", 0),
            "total_rows": metadata.get("total_rows", 0),
            "total_size_bytes": metadata.get("total_size_bytes", 0),
            "total_size_mb": metadata.get("total_size_mb", 0),
            # Store as JSON string
            "table_details": str(metadata.get("table_details", [])),
        }

        # Create a metadata source
        @dlt.resource(table_name="datalumos_load_metadata")
        def load_metadata():
            yield metadata_record

        # Get existing pipeline or create new one for metadata
        dest_config = config.get_destination_config()
        metadata_pipeline = dlt.pipeline(
            pipeline_name="datalumos_metadata",
            destination=dest_config["destination"],
            dataset_name="datalumos_meta"
        )

        # Save metadata
        metadata_pipeline.run(
            load_metadata(), credentials=dest_config["credentials"])
        logger.info("Load metadata saved successfully")

    except Exception as e:
        logger.warning(f"Could not save load metadata: {e}")
        # Don't fail the main process if metadata saving fails
