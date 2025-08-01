"""
Column validation workflow for data quality assessment.

This module provides a validation pipeline to assess data quality and integrity
of table columns using the results from table profiling:

1. **Column Validation**: Validates columns based on their semantic analysis
2. **Quality Assessment**: Identifies data quality issues and anomalies

All validation results can be cached for efficient reuse across workflows.
"""

import asyncio

from agents import Runner
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.agents.data_validator import (
    DataValidatorAgent,
    DataValidatorOutput,
)
from datalumos.agents.utils import run_agent_with_retries
from datalumos.flows.subflows.cache_utils import CacheManager
from datalumos.flows.subflows.table_profiling import TableAnalysisResults
from datalumos.logging import get_logger
from datalumos.logging_utils import (
    log_column_result,
    log_step_start,
)
from datalumos.services.postgres.connection import Column, PostgresDB

logger = get_logger(__name__)


class ValidationResults(BaseModel):
    """Complete validation results for table columns."""

    column_validations: list[DataValidatorOutput] = Field(
        description="Validation results for each column"
    )


# Cache manager for validation results
_cache_manager = CacheManager("validation_cache", "validation", ValidationResults)


async def run_column_validation(
    table_profile_results: TableAnalysisResults,
    columns: list[Column],
    schema: str,
    table_name: str,
    db: PostgresDB,
    mcp_server: MCPServerStdio,
    force_refresh: bool = True,
) -> ValidationResults:
    """
    Run column validation based on table profiling results.

    Validates specified columns using their semantic analysis from the table profiling phase
    to identify data quality issues and anomalies.

    Args:
        table_profile_results: Results from table profiling containing column analyses
        columns: List of column names to validate
        schema: Database schema containing the table
        table_name: Name of the table to validate
        db: PostgreSQL database connection for metadata access
        mcp_server: MCP server providing database query capabilities
        force_refresh: If True, bypass cache and perform fresh validation

    Returns:
        ValidationResults: Complete validation results for specified columns

    """
    log_step_start("Column validation", f"{schema}.{table_name}")

    if not force_refresh:
        cached_results = _cache_manager.load_cached_results(schema, table_name)
        if cached_results:
            logger.info(f"Using cached validation results for {schema}.{table_name}")
            return cached_results

    column_validations = await _validate_columns(
        table_profile_results=table_profile_results,
        columns=columns,
        schema=schema,
        table_name=table_name,
        mcp_server=mcp_server,
    )

    results = ValidationResults(column_validations=column_validations)
    _cache_manager.save_cached_results(schema, table_name, results)
    return results


async def _validate_columns(
    table_profile_results: TableAnalysisResults,
    columns: list[Column],
    schema: str,
    table_name: str,
    mcp_server: MCPServerStdio,
) -> list[DataValidatorOutput]:
    """Validate specified columns using their analyses."""
    # Create a mapping of column names to their analyses
    column_analyses = table_profile_results.column_analyses
    analysis_map = {analysis.column_name: analysis for analysis in column_analyses}

    semaphore = asyncio.Semaphore(3)  # Limit concurrent validation

    async def validate_single_column(column: Column) -> DataValidatorOutput:
        """Validate a single column."""
        async with semaphore:
            # Get the corresponding column analysis
            column_analysis = analysis_map.get(column.name)
            if not column_analysis:
                logger.warning(
                    f"No analysis found for column {column}, skipping validation"
                )
                return None

            validator = DataValidatorAgent(
                mcp_servers=[mcp_server],
            )

            validation_question = (
                f"Validate '{column.name}' column in the table '{schema}.{table_name}' "
                f"\n Column name: '{column.name}'. "
                f"\n Column description: '{column_analysis.business_definition}'. "
                f"\n Column data type: '{column_analysis.data_type}'. "
                f"\n Column tech specs: '{column_analysis.technical_specification}'. "
                f"\n You must execute the validation queries"
            )

            result = await run_agent_with_retries(
                fn=Runner.run, agent=validator, question=validation_question
            )

            log_column_result(column.name, "validation", result.final_output)
            return result.final_output

    validation_tasks = [validate_single_column(col) for col in columns]
    validation_results = await asyncio.gather(*validation_tasks)

    # Filter out None results (columns without analysis). This could happen if the
    # run with retry exhausted retries.
    return [result for result in validation_results if result is not None]
