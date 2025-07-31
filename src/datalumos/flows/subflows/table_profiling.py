"""
Comprehensive table profiling workflow for understanding data context and semantics.

This module provides a complete data profiling pipeline
to understand the business meaning and context behind tables and columns:

1. **Table Context Discovery**: Understands what the table represents in business terms
2. **Column Semantic Analysis**: Investigates the meaning and purpose of each column
3. **Column Importance Triage**: Prioritizes columns by business value for downstream
processing.

All analysis results are cached to enable efficient reuse across multiple workflows.
"""

import asyncio

from agents import Runner
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.agents.column_analyser import (
    ColumnAnalyserAgent,
    ColumnAnalysisOutput,
)
from datalumos.agents.agents.data_explorer import DataExplorerAgent, TableAnalysisOutput
from datalumos.agents.agents.triage_agent import (
    ColumnImportance,
    TriageAgent,
    TriageOutput,
)
from datalumos.agents.utils import run_agent_with_retries
from datalumos.flows.subflows.cache_utils import CacheManager
from datalumos.logging import get_logger
from datalumos.logging_utils import (
    log_column_result,
    log_step_complete,
    log_step_start,
    log_summary,
)
from datalumos.services.postgres.connection import PostgresDB

logger = get_logger(__name__)


class TableAnalysisResults(BaseModel):
    """
    Complete profiling results containing table context, column semantics, and
    prioritization.
    """

    table_context: TableAnalysisOutput = Field(description="Table business context")
    column_analyses: list[ColumnAnalysisOutput] = Field(
        description="Detailed analysis for each column"
    )
    column_triage: TriageOutput = Field(description="Column prioritization results")


# Cache manager for analysis results
_cache_manager = CacheManager("analysis_cache", "analysis", TableAnalysisResults)


async def profile(
    schema: str,
    table_name: str,
    db: PostgresDB,
    mcp_server: MCPServerStdio,
    force_refresh: bool = False,
) -> TableAnalysisResults:
    """
    Comprehensively profile a table to understand its complete business context.

    Creates a deep understanding of the table by investigating its purpose, analyzing
    the semantic meaning of each column, and classifying columns by business importance.
    Results are cached for efficient reuse.

    Args:
        schema: Database schema containing the table
        table_name: Name of the table to profile
        db: PostgreSQL database connection for metadata access
        mcp_server: MCP server providing database query capabilities
        force_refresh: If True, bypass cache and perform fresh analysis

    Returns:
        TableAnalysisResults: Complete profiling results including table context,
        column semantic analysis, and column importance classification

    """
    logger.info(f"Starting analysis for {schema}.{table_name}")

    if not force_refresh:
        cached_results = _cache_manager.load_cached_results(schema, table_name)
        if cached_results:
            logger.info(f"Using cached results for {schema}.{table_name}")
            return cached_results

    log_step_start("Getting table context", f"{schema}.{table_name}")
    table_context = await _get_table_context(
        schema=schema,
        table=table_name,
        db=db,
        mcp_server=mcp_server,
        force_refresh=force_refresh,
    )
    log_step_complete("Table context analysis")

    log_step_start("Analyzing columns", f"{schema}.{table_name}")
    column_analyses = await _analyze_columns(
        schema=schema,
        table_name=table_name,
        table_context=table_context,
        db=db,
        mcp_server=mcp_server,
    )
    log_step_complete("Column analysis", len(column_analyses))

    log_step_start("Triaging columns", f"{schema}.{table_name}")
    column_triage = await _triage_columns(
        schema=schema,
        table_name=table_name,
        table_context=table_context,
        db=db,
        mcp_server=mcp_server,
        column_analyses=column_analyses,
    )
    log_step_complete("Column triage")

    results = TableAnalysisResults(
        table_context=table_context,
        column_analyses=column_analyses,
        column_triage=column_triage,
    )

    _cache_manager.save_cached_results(schema, table_name, results)

    # Log profiling summary
    high_priority_cols = [
        c
        for c in results.column_triage.column_classifications
        if c.classification.value == "HIGH"
    ]
    log_summary(
        "Profiling Complete",
        {
            "Total columns analyzed": len(column_analyses),
            "High priority columns": len(high_priority_cols),
            "Table context": "✓ Complete",
        },
    )

    return results


async def _get_table_context(
    schema: str, table: str, db, mcp_server, *, force_refresh: bool = False
) -> TableAnalysisOutput:
    """
    Infer and understand the table context.
    """
    explorer = DataExplorerAgent(
        mcp_servers=[mcp_server],
        table_name=table,
        columns=db.get_column_names(schema=schema, table=table),
    )
    res = await Runner.run(explorer, f"Analyse {schema}.{table}")
    log_column_result("Table profiling", "profile", res.final_output)
    return res.final_output


async def _analyze_columns(
    schema: str,
    table_name: str,
    table_context: TableAnalysisOutput,
    db: PostgresDB,
    mcp_server: MCPServerStdio,
) -> list[ColumnAnalysisOutput]:
    """Analyze all columns in the table."""
    columns = db.get_column_names(table=table_name, schema=schema)
    semaphore = asyncio.Semaphore(3)  # Limit concurrent analysis

    async def analyze_single_column(column) -> ColumnAnalysisOutput:
        """Analyze a single column."""
        async with semaphore:
            analyzer = ColumnAnalyserAgent(
                mcp_servers=[mcp_server],
            )

            question = (
                f"Analyze {column.name} column of type {column.data_type} "
                f"in table {table_name}. Table context: {table_context.table_description}"
            )

            result = await run_agent_with_retries(
                fn=Runner.run, agent=analyzer, question=question
            )

            logger.info(f"Column analysis complete: {column.name}")
            log_column_result(column.name, "analysis", result.final_output)
            return result.final_output

    # Run all column analyses concurrently
    analysis_tasks = [analyze_single_column(col) for col in columns]
    return await asyncio.gather(*analysis_tasks)


async def _triage_columns(
    schema: str,
    table_name: str,
    table_context: TableAnalysisOutput,
    db: PostgresDB,
    mcp_server: MCPServerStdio,
    column_analyses: list[ColumnAnalysisOutput],
) -> TriageOutput:
    """Triage columns by importance."""
    columns = db.get_column_names(table=table_name, schema=schema)

    triage_agent = TriageAgent(mcp_servers=[mcp_server])

    question = (
        f"Analyze and triage the columns in table {schema}.{table_name}. "
        f"Table description: {table_context.table_description}. "
        f"Columns to classify: {[(c.name, c.data_type) for c in columns]}"
    )

    result = await Runner.run(triage_agent, question)
    logger.info(
        f"Column triage complete for {schema}.{table_name}. "
        f"Column analyses: {column_analyses}"
    )

    return result.final_output


def get_high_priority_columns(results: TableAnalysisResults) -> list[tuple[str, str]]:
    """Extract high priority columns from triage results."""
    return [
        (c.column_name, c.column_type)
        for c in results.column_triage.column_classifications
        if c.classification == ColumnImportance.HIGH
    ]
