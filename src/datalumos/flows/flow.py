import asyncio
from dataclasses import dataclass

from agents import set_default_openai_key

from datalumos.flows.subflows.assert_validity import run_column_validation
from datalumos.MCPs.postgres import postgres_mcp_server
from datalumos.flows.subflows.table_profiling import profile
from datalumos.config import config
from datalumos.services.postgres.config import DEFAULT_POSTGRES_CONFIG
from datalumos.logging import get_logger, setup_logging
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
            schema=schema, table_name=table_name, db=db, mcp_server=mcp_server
        )

        columns = db.get_column_names(table=table_name, schema=schema)

        validation_results = await run_column_validation(
            table_profile_results=table_profile_results,
            columns=columns,
            schema=schema,
            table_name=table_name,
            db=db,
            mcp_server=mcp_server,
        )

        return table_profile_results, validation_results


if __name__ == "__main__":

    async def main():
        await run(
            schema="datalumos", table_name="dtdc_curier", config=AgentConfig.from_env()
        )

    asyncio.run(main())
