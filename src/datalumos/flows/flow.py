from dataclasses import dataclass

from agents import set_default_openai_key

from datalumos.config import config
from datalumos.flows.subflows.assert_accuracy import run_accuracy_flow
from datalumos.flows.subflows.assert_validity import run_column_validation
from datalumos.flows.subflows.table_profiling import profile
from datalumos.logging import get_logger, setup_logging
from datalumos.logging_utils import log_summary
from datalumos.MCPs.postgres import postgres_mcp_server
from datalumos.services.postgres.config import DEFAULT_POSTGRES_CONFIG
from datalumos.services.postgres.connection import PostgresDB

setup_logging()
logger = get_logger("datalumos")


@dataclass
class AgentConfig:
    """Minimal configuration for Data Lumos"""

    postgres_config: object
    openai_key: str | None = None

    @classmethod
    def from_env(cls) -> "AgentConfig":
        return cls(
            postgres_config=DEFAULT_POSTGRES_CONFIG,
            openai_key=config.OPENAI_API_KEY,
        )


async def run(table_name: str, schema: str, config: AgentConfig):
    """Main orchestration function - runs all three agents sequentially"""

    if config.openai_key:
        set_default_openai_key(config.openai_key)

    db = PostgresDB(config=config.postgres_config)

    async with postgres_mcp_server(config) as mcp_server:
        table_profile_results = await profile(
            schema=schema,
            table_name=table_name,
            db=db,
            mcp_server=mcp_server,
        )

        # Log profile results
        log_summary(
            "Table Profile Results",
            {
                "Table Context": table_profile_results.table_context,
                "Columns Analyzed": table_profile_results.column_analyses,
                "High Priority Columns": [
                    c
                    for c in table_profile_results.column_triage.column_classifications
                    if c.classification.value == "HIGH"
                ],
                "Medium Priority Columns": [
                    c
                    for c in table_profile_results.column_triage.column_classifications
                    if c.classification.value == "MEDIUM"
                ],
                "Low Priority Columns": [
                    c
                    for c in table_profile_results.column_triage.column_classifications
                    if c.classification.value == "LOW"
                ],
            },
        )

        columns = db.get_column_names(table=table_name, schema=schema)

        await run_column_validation(
            table_profile_results=table_profile_results,
            columns=columns,
            schema=schema,
            table_name=table_name,
            db=db,
            mcp_server=mcp_server,
            force_refresh=True,
        )

        await run_accuracy_flow(
            table_profile_results=table_profile_results,
            schema=schema,
            table_name=table_name,
            db=db,
            force_refresh=True,
        )
