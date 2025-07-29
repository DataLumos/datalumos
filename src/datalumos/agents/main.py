import asyncio
import logging
import os
from dataclasses import dataclass, field

from agents import Runner, set_default_openai_key
from agents.mcp import MCPServerStdio
from datalumos.agents.data_explorer import DataExplorerAgent, TableAnalysisOutput
from datalumos.agents.column_analyser import ColumnAnalyserAgent, ColumnAnalysisOutput
from datalumos.agents.data_validator import DataValidatorAgent, DataValidatorOutput
from datalumos.agents.triage_agent import TriageAgent, ColumnImportance
from datalumos.agents.utils import run_agent_with_retries
from datalumos.services.postgres import PostgresDB
from datalumos.logging import setup_logging, get_logger
from datalumos.core import DEFAULT_POSTGRES_CONFIG


# Configure logging
setup_logging()
logger = get_logger("datalumos")


@dataclass
class Config:
    """Minimal configuration for Data Lumos"""
    postgres_config: object
    openai_key: str | None = None

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            postgres_config=DEFAULT_POSTGRES_CONFIG,
            openai_key=os.getenv("OPENAI_API_KEY"),
        )


@dataclass
class AnalysisResults:
    """Container for all analysis results"""
    table_analysis: TableAnalysisOutput | None = None
    column_analysis: list[ColumnAnalysisOutput] = field(default_factory=list)
    validation_result: list[DataValidatorOutput] = field(default_factory=list)


async def analyze_table(table_name: str, schema: str, config: Config) -> AnalysisResults:
    """Main orchestration function - runs all three agents sequentially"""

    if config.openai_key:
        set_default_openai_key(config.openai_key)

    results = AnalysisResults()

    db = PostgresDB(config=config.postgres_config)

    postgres_mcp_params = {
        "command": "npx",
        "args": [
            "-y",
            "@modelcontextprotocol/server-postgres",
            config.postgres_config.connection_string,
            "--access-mode=restricted",
        ],
    }

    async with MCPServerStdio(params=postgres_mcp_params) as mcp_server:

        columns = db.get_column_names(table=table_name, schema=schema)
        logger.info(f"Starting analysis for {schema}.{table_name}")

        # Step 1: Explore the table
        logger.info("Step 1: Table exploration")
        explorer = DataExplorerAgent(
            mcp_servers=[mcp_server], table_name=table_name, columns=columns)
        question = f"Analyze {table_name} table in the {schema} schema"

        explorer_result = await Runner.run(explorer, question)
        results.table_analysis = explorer_result.final_output
        logger.info(f"Table exploration complete for {schema}.{table_name}")
        logger.info(explorer_result.final_output)

        # Step 2: Analyze first column (keeping it simple)
        logger.info("Step 2: Column analysis")

        columns = db.get_column_names(table=table_name, schema=schema)

        triage_agent = TriageAgent(
            mcp_servers=[mcp_server]
        )

        triage_result = await Runner.run(
            triage_agent, 
            f"Analyze and triage the columns in table {schema}.{table_name}. "
            f"Table description: {results.table_analysis.table_description}. "
            f"Columns to classify: {[c.name for c in columns]}"
        )
        logger.info(triage_result.final_output)

        high_priority_columns = [c.column_name for c in triage_result.final_output.column_classifications if c.classification == ColumnImportance.HIGH]
        logger.info(f"Based on the triage, the high priority columns are: {high_priority_columns}")
        semaphore = asyncio.Semaphore(3)
        # Launch tasks
        column_validation_tasks = [
            analyse_and_validate_column(
                column_name=col,
                table_name=table_name,
                schema=schema,
                mcp_server=mcp_server,
                results=results,
                semaphore=semaphore,
            )
            for col in high_priority_columns
        ]

        await asyncio.gather(*column_validation_tasks)
        return results


logger = logging.getLogger(__name__)


async def analyse_and_validate_column(
    column_name: str,
    table_name: str,
    schema: str,
    mcp_server: str,
    results,
    semaphore: asyncio.Semaphore,
):
    """
    End-to-end pipeline for one column:
      1. Column analysis
      2. Column validation
    Saves the artefacts into the shared `results` object.
    """
    async with semaphore:

        analyzer = ColumnAnalyserAgent(
            mcp_servers=[mcp_server],
            column_name=column_name,
            table_context=(
                results.table_analysis.table_description
                if results.table_analysis
                else ""
            ),
        )
        # TODO: move this to the column_analyser module
        question = (
            f"Analyze {column_name} column in table {table_name} "
            f"in the {schema} schema"
        )

        analyzer_result = await run_agent_with_retries(fn=lambda: Runner.run(analyzer, question))

        results.column_analysis.append(analyzer_result.final_output)

        logger.info("Column analysis complete for: %s", column_name)
        logger.info(f"\n=== COLUMN ANALYSIS ({column_name}) ===")
        logger.info(analyzer_result.final_output)

        validator = DataValidatorAgent(
            table_name=table_name,
            schema_name=schema,
            mcp_servers=[mcp_server],
            input_format=str(ColumnAnalysisOutput.describe()),
            column_name=column_name,
            table_context=(
                results.table_analysis.table_description
                if results.table_analysis
                else ""
            ),
        )

        # TODO: move this into the validator module
        validation_prompt = (
            f"Validate {column_name} column. Column analysis output: {analyzer_result.final_output}"
        )

        validation_result = await Runner.run(starting_agent=validator, input=validation_prompt)
        results.validation_result.append(validation_result.final_output)

        logger.info("Validation complete for: %s", column_name)
        logger.info(f"\n=== VALIDATION ({column_name}) ===")
        logger.info(validation_result.final_output)
