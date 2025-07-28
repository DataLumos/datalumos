import asyncio
import logging
import os
import typer
from dataclasses import dataclass, field

from agents import Runner, set_default_openai_key
from agents.mcp import MCPServerStdio
from datalumos.agents.data_explorer import DataExplorerAgent, TableAnalysisOutput
from datalumos.agents.column_analyser import ColumnAnalyserAgent, ColumnAnalysisOutput
from datalumos.agents.data_validator import DataValidatorAgent, DataValidatorOutput
from datalumos.agents.utils import run_agent_with_retries
from datalumos.services.postgres import PostgresDB
from datalumos.core import DEFAULT_POSTGRES_CONFIG


# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("datalumos")


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
        logger.info("Table exploration complete")
        print("\n=== TABLE ANALYSIS ===")
        print(results.table_analysis)

        # Step 2: Analyze first column (keeping it simple)
        logger.info("Step 2: Column analysis")

        columns = db.get_column_names(table=table_name, schema=schema)[:4]

        semaphore = asyncio.Semaphore(3)

        # Launch tasks
        column_validation_tasks = [
            analyse_and_validate_column(
                column_name=col.name,
                table_name=table_name,
                schema=schema,
                mcp_server=mcp_server,
                results=results,
                semaphore=semaphore,
            )
            for col in columns
        ]

        await asyncio.gather(*column_validation_tasks)
        return results

    logger.info("Analysis complete for %s.%s", schema, table_name)
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
        print(f"\n=== COLUMN ANALYSIS ({column_name}) ===")
        print(analyzer_result.final_output)

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
        print(f"\n=== VALIDATION ({column_name}) ===")
        print(validation_result.final_output)


def main(
    table_name: str = typer.Option(..., "--table_name",
                                   help="Name of the database table to analyze"),
    schema_name: str = typer.Option(..., "--schema_name",
                                    help="Database schema containing the table")
):
    """
    Run DataLumos QA analysis on a database table.

    This flow performs quality assurance checks on the specified table within
    the given schema, including table structure analysis, column profiling,
    and data validation.
    """
    config = Config.from_env()
    asyncio.run(analyze_table(table_name, schema_name, config))


if __name__ == "__main__":
    typer.run(main)
