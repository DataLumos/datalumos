"""
Column validation workflow for data quality assessment.

This module provides a validation pipeline to assess data quality and integrity
of table columns using the results from table profiling:

1. **Column Validation**: Validates columns based on their semantic analysis
2. **Quality Assessment**: Identifies data quality issues and anomalies

All validation results can be cached for efficient reuse across workflows.
"""

import asyncio
from pathlib import Path
from typing import Final

from agents import Runner
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.agents.column_analyser import ColumnAnalysisOutput
from datalumos.agents.agents.data_validator import DataValidatorAgent, DataValidatorOutput
from datalumos.agents.utils import run_agent_with_retries
from datalumos.flows.subflows.table_profiling import TableAnalysisResults
from datalumos.logging import get_logger
from datalumos.logging_utils import (
    log_column_result,
    log_step_start,
    log_summary,
)
from datalumos.services.postgres.connection import Column, PostgresDB

logger = get_logger(__name__)

# Cache directory for storing validation results
_CACHE_DIR: Final = Path.home() / ".datalumos" / "validation_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


class ValidationResults(BaseModel):
    """Complete validation results for table columns."""

    column_validations: list[DataValidatorOutput] = Field(
        description="Validation results for each column"
    )


async def run_column_validation(
    table_profile_results: TableAnalysisResults,
    columns: list[str],
    schema: str,
    table_name: str,
    db: PostgresDB,
    mcp_server: MCPServerStdio,
    force_refresh: bool = False,
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
        cached_results = _load_cached_results(schema, table_name)
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

    _save_cached_results(schema, table_name, results)

    log_summary(
        "Validation Complete",
        {"Columns validated": len(column_validations), "Results cached": "✓"},
    )

    return results


async def _validate_columns(
    table_profile_results: TableAnalysisResults,
    columns: list[str],
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
                table_name=table_name,
                schema_name=schema,
                mcp_servers=[mcp_server],
                input_format=str(ColumnAnalysisOutput.describe()),
                column_name=column.name,
                table_context=table_profile_results.table_context.table_description,
            )

            validation_prompt = (
                f"Validate {column.name} column. Column analysis output: {column_analysis}"
            )

            result = await run_agent_with_retries(
                fn=Runner.run, agent=validator, question=validation_prompt
            )

            log_column_result(column.name, "validation", result.final_output)
            return result.final_output

    validation_tasks = [validate_single_column(col) for col in columns]
    validation_results = await asyncio.gather(*validation_tasks)

    # Filter out None results (columns without analysis). This could happen if the
    # run with retry exhausted retries.
    return [result for result in validation_results if result is not None]


def _cache_file_path(schema: str, table_name: str) -> Path:
    """Get cache file path for validation results."""
    return _CACHE_DIR / f"{schema}.{table_name}.validation.json"


def _load_cached_results(schema: str, table_name: str) -> ValidationResults | None:
    """Load cached validation results if they exist."""
    cache_file = _cache_file_path(schema, table_name)
    if cache_file.exists():
        try:
            return ValidationResults.model_validate_json(cache_file.read_text())
        except Exception as e:
            logger.warning(f"Failed to load cached validation results: {e}")
    return None


def _save_cached_results(schema: str, table_name: str, results: ValidationResults) -> None:
    """Save validation results to cache."""
    cache_file = _cache_file_path(schema, table_name)
    try:
        cache_file.write_text(results.model_dump_json(indent=2))
        logger.info(f"Validation results cached to {cache_file}")
    except Exception as e:
        logger.warning(f"Failed to cache validation results: {e}")
