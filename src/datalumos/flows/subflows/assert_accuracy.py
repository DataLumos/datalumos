"""
Column accuracy validation workflow for data quality assessment.

Simple dispatcher logic: one column -> one accuracy check based on column type.
- Text columns: categorical accuracy with sampling for high cardinality
- Date columns: date accuracy validation
- Numerical columns: numerical accuracy validation
"""

import asyncio

from agents import Runner
from pydantic import BaseModel, Field

from datalumos.agents.agents.column_analyser import ColumnAnalysisOutput
from datalumos.agents.agents.date_accuracy_checker import (
    DateAccuracyCheckerAgent,
    DateAccuracyOutput,
)
from datalumos.agents.agents.numerical_accuracy_checker import (
    NumericalAccuracyCheckerAgent,
    NumericalAccuracyOutput,
)
from datalumos.agents.agents.text_accuracy_checker import (
    TextAccuracyCheckerAgent,
    TextAccuracyOutput,
)
from datalumos.agents.utils import run_agent_with_retries
from datalumos.flows.subflows.cache_utils import CacheManager
from datalumos.flows.subflows.table_profiling import TableAnalysisResults
from datalumos.logging import get_logger
from datalumos.logging_utils import (
    log_column_result,
    log_step_start,
)
from datalumos.services.postgres.connection import PostgresDB

logger = get_logger(__name__)

# Configuration
DISTINCT_VALUES_THRESHOLD = 50
SAMPLE_SIZE = 20
MAX_CONCURRENT_CHECKS = 3


class AccuracyResults(BaseModel):
    """Complete accuracy results for table columns."""

    text_accuracy: list[TextAccuracyOutput] = Field(default_factory=list)
    numerical_accuracy: list[NumericalAccuracyOutput] = Field(default_factory=list)
    date_accuracy: list[DateAccuracyOutput] = Field(default_factory=list)


# Cache manager for accuracy results
_cache_manager = CacheManager("accuracy_cache", "accuracy", AccuracyResults)


async def run_accuracy_flow(
    table_profile_results: TableAnalysisResults,
    # columns: list[str],
    schema: str,
    table_name: str,
    db: PostgresDB,
    force_refresh: bool = True,
) -> AccuracyResults:
    """Run accuracy validation with clean dispatcher logic."""
    log_step_start("Accuracy validation", f"{schema}.{table_name}")

    if not force_refresh:
        cached_results = _cache_manager.load_cached_results(schema, table_name)
        if cached_results:
            logger.info(f"Using cached accuracy results for {schema}.{table_name}")
            return cached_results

    results = await _run_all_accuracy_checks(
        table_profile_results, schema, table_name, db
    )

    _cache_manager.save_cached_results(schema, table_name, results)

    return results


async def _run_all_accuracy_checks(
    table_profile_results: TableAnalysisResults,
    schema: str,
    table_name: str,
    db: PostgresDB,
) -> AccuracyResults:
    """Run accuracy checks with clean dispatcher logic."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)

    tasks = [
        _dispatch_column_check(analysis, db, table_name, schema, semaphore)
        for analysis in table_profile_results.column_analyses
    ]

    results = await asyncio.gather(*tasks)

    return AccuracyResults(
        text_accuracy=[r for r in results if isinstance(r, TextAccuracyOutput)],
        numerical_accuracy=[
            r for r in results if isinstance(r, NumericalAccuracyOutput)
        ],
        date_accuracy=[r for r in results if isinstance(r, DateAccuracyOutput)],
    )


async def _dispatch_column_check(
    analysis: ColumnAnalysisOutput,
    db: PostgresDB,
    table_name: str,
    schema: str,
    semaphore: asyncio.Semaphore,
) -> TextAccuracyOutput | NumericalAccuracyOutput | DateAccuracyOutput | None:
    """Simple dispatcher: one column -> one accuracy check based on type."""
    column = analysis.column_name

    # Get column type
    column_info = await asyncio.to_thread(
        db.get_column_type, column, table_name, schema
    )
    column_type = column_info.lower()

    # Dispatch based on type
    if any(word in column_type for word in ["date", "time", "timestamp"]):
        return await _check_date_accuracy(
            column, analysis, db, table_name, schema, semaphore
        )
    elif any(
        word in column_type for word in ["int", "float", "numeric", "decimal", "double"]
    ):
        return await _check_numerical_accuracy(
            column, analysis, db, table_name, schema, semaphore
        )
    else:  # char/text type
        return await _check_text_accuracy(
            column, analysis, db, table_name, schema, semaphore
        )


async def _check_text_accuracy(
    column: str,
    analysis: ColumnAnalysisOutput,
    db: PostgresDB,
    table_name: str,
    schema: str,
    semaphore: asyncio.Semaphore,
) -> TextAccuracyOutput | None:
    """Check text column accuracy with sampling logic."""
    distinct_count = await asyncio.to_thread(
        db.get_count_distinct_values, column, table_name, schema
    )

    if distinct_count < DISTINCT_VALUES_THRESHOLD:
        values = await asyncio.to_thread(
            db.get_distinct_values, column, table_name, schema
        )
    else:
        values = await asyncio.to_thread(
            db.get_sample_values, column, table_name, schema, limit=SAMPLE_SIZE
        )

    async with semaphore:
        agent = TextAccuracyCheckerAgent()
        result = await run_agent_with_retries(
            fn=Runner.run,
            agent=agent,
            question=(
                f"Check accuracy of {column}. "
                f"Context: {analysis.business_definition}. "
                f"Assess the accuracy of the following distinct values: {values}. "
            ),
        )
        log_column_result(column, "text accuracy", result.final_output)
        return result.final_output


async def _check_numerical_accuracy(
    column: str,
    analysis: ColumnAnalysisOutput,
    db: PostgresDB,
    table_name: str,
    schema: str,
    semaphore: asyncio.Semaphore,
) -> NumericalAccuracyOutput | None:
    """Check numerical column accuracy."""
    sample_values = await asyncio.to_thread(
        db.get_sample_values, column, table_name, schema, limit=SAMPLE_SIZE
    )

    async with semaphore:
        agent = NumericalAccuracyCheckerAgent()
        result = await run_agent_with_retries(
            fn=Runner.run,
            agent=agent,
            question=(
                f"Check numerical accuracy of {column}. "
                f"Context: {analysis.business_definition}. "
                f"Values: {sample_values}"
            ),
        )
        log_column_result(column, "numerical accuracy", result.final_output)
        return result.final_output


async def _check_date_accuracy(
    column: str,
    analysis: ColumnAnalysisOutput,
    db: PostgresDB,
    table_name: str,
    schema: str,
    semaphore: asyncio.Semaphore,
) -> DateAccuracyOutput | None:
    """Check date column accuracy."""
    sample_values = await asyncio.to_thread(
        db.get_sample_values, column, table_name, schema, limit=SAMPLE_SIZE
    )

    async with semaphore:
        agent = DateAccuracyCheckerAgent()
        result = await run_agent_with_retries(
            fn=Runner.run,
            agent=agent,
            question=(
                f"Check date accuracy of {column}. "
                f"Context: {analysis.business_definition}. "
                f"Values: {sample_values}"
            ),
        )
        log_column_result(column, "date accuracy", result.final_output)
        return result.final_output
