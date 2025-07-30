"""
Comprehensive table profiling workflow for understanding data context and semantics.

This module provides a complete data profiling pipeline
to understand the business meaning and context behind tables and columns:

1. **Table Context Discovery**: Understands what the table represents in business terms
2. **Column Semantic Analysis**: Investigates the meaning and purpose of each column
3. **Column Importance Triage**: Prioritizes columns by business value for downstream processing

All analysis results are cached to enable efficient reuse across multiple workflows.
"""
import asyncio
from pathlib import Path
from typing import Final

from agents import Runner
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.agents.column_analyser import ColumnAnalysisOutput, ColumnAnalyserAgent
from datalumos.agents.agents.data_explorer import TableAnalysisOutput, DataExplorerAgent
from datalumos.agents.agents.triage_agent import TriageOutput, TriageAgent, ColumnImportance
from datalumos.agents.utils import run_agent_with_retries

from datalumos.logging import get_logger
from datalumos.services.postgres.connection import PostgresDB

logger = get_logger(__name__)
_CACHE_DIR: Final = Path.home() / ".datalumos" / "analysis_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


class TableAnalysisResults(BaseModel):
    """Complete profiling results containing table context, column semantics, and prioritization."""

    table_context: TableAnalysisOutput = Field(description="Table business context")
    column_analyses: list[ColumnAnalysisOutput] = Field(
        description="Detailed analysis for each column"
    )
    column_triage: TriageOutput = Field(description="Column prioritization results")


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
        cached_results = _load_cached_results(schema, table_name)
        if cached_results:
            logger.info(f"Using cached results for {schema}.{table_name}")
            return cached_results

    logger.info("Getting table context...")
    table_context = await _get_table_context(
        schema=schema,
        table=table_name,
        db=db,
        mcp_server=mcp_server,
        force_refresh=force_refresh,
    )

    logger.info("Step 2: Analyzing columns...")
    column_analyses = await _analyze_columns(
        schema=schema,
        table_name=table_name,
        table_context=table_context,
        db=db,
        mcp_server=mcp_server,
    )

    logger.info("Step 3: Triaging columns...")
    column_triage = await _triage_columns(
        schema=schema,
        table_name=table_name,
        table_context=table_context,
        db=db,
        mcp_server=mcp_server,
        column_analyses=column_analyses,
    )

    results = TableAnalysisResults(
        table_context=table_context,
        column_analyses=column_analyses,
        column_triage=column_triage,
    )

    _save_cached_results(schema, table_name, results)
    logger.info(f"Analysis complete for {schema}.{table_name}")

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
                column_name=column.name,
                table_context=table_context.table_description,
            )

            question = f"Analyze {column.name} column of type {column.data_type} in table {table_name}"

            result = await run_agent_with_retries(
                fn=Runner.run, agent=analyzer, question=question
            )

            logger.info(f"Column analysis complete: {column.name}")
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
        f"Column triage complete for {schema}.{table_name}. Column analyses: {column_analyses}"
    )

    return result.final_output


def get_high_priority_columns(results: TableAnalysisResults) -> list[tuple[str, str]]:
    """Extract high priority columns from triage results."""
    return [
        (c.column_name, c.column_type)
        for c in results.column_triage.column_classifications
        if c.classification == ColumnImportance.HIGH
    ]


def _cache_file_path(schema: str, table_name: str) -> Path:
    """Get cache file path for table analysis results."""
    return _CACHE_DIR / f"{schema}.{table_name}.analysis.json"


def _load_cached_results(schema: str, table_name: str) -> TableAnalysisResults | None:
    """Load cached analysis results if they exist."""
    cache_file = _cache_file_path(schema, table_name)
    if cache_file.exists():
        try:
            return TableAnalysisResults.model_validate_json(cache_file.read_text())
        except Exception as e:
            logger.warning(f"Failed to load cached results: {e}")
    return None


def _save_cached_results(
    schema: str, table_name: str, results: TableAnalysisResults
) -> None:
    """Save analysis results to cache."""
    cache_file = _cache_file_path(schema, table_name)
    try:
        cache_file.write_text(results.model_dump_json(indent=2))
        logger.info(f"Results cached to {cache_file}")
    except Exception as e:
        logger.warning(f"Failed to cache results: {e}")
