"""
Completeness assessment workflow for data quality evaluation.

This module provides a pipeline to assess data completeness by calculating
fill rates for all columns in a table.
"""

from dataclasses import dataclass

from pydantic import BaseModel, Field

from datalumos.flows.subflows.cache_utils import CacheManager
from datalumos.logging import get_logger
from datalumos.logging_utils import log_column_result, log_step_start
from datalumos.services.postgres.connection import PostgresDB

logger = get_logger(__name__)


@dataclass
class ColumnFillRate:
    """Fill rate information for a single column."""

    column_name: str
    null_count: int
    fill_rate_percentage: float


class CompletenessResults(BaseModel):
    """Complete fill rate results for table columns."""

    column_fill_rates: list[ColumnFillRate] = Field(
        description="Fill rate results for each column"
    )


# Cache manager for completeness results
_cache_manager = CacheManager("completeness_cache", "completeness", CompletenessResults)


async def run_completeness_flow(
    schema: str,
    table_name: str,
    db: PostgresDB,
    force_refresh: bool = True,
) -> CompletenessResults:
    """
    Calculate fill rates for all columns in a table.

    Args:
        columns: List of columns to analyze
        schema: Database schema containing the table
        table_name: Name of the table to analyze
        db: PostgreSQL database connection
        force_refresh: If True, bypass cache and perform fresh analysis

    Returns:
        CompletenessResults: Fill rate results for all columns
    """
    log_step_start("Completeness assessment", f"{schema}.{table_name}")

    if not force_refresh:
        cached_results = _cache_manager.load_cached_results(schema, table_name)
        if cached_results:
            logger.info(f"Using cached completeness results for {schema}.{table_name}")
            return cached_results

    # Get completeness stats from database
    stats = db.get_completeness_stats(table_name, schema)

    # Convert to ColumnFillRate objects
    fill_rates = [
        ColumnFillRate(
            column_name=stat["column_name"],
            null_count=stat["null_count"],
            fill_rate_percentage=stat["fill_rate_percentage"],
        )
        for stat in stats
    ]

    for fill_rate in fill_rates:
        log_column_result(fill_rate.column_name, "completeness", fill_rate)

    results = CompletenessResults(column_fill_rates=fill_rates)
    _cache_manager.save_cached_results(schema, table_name, results)

    logger.info(f"Calculated fill rates for {len(fill_rates)} columns")
    return results
