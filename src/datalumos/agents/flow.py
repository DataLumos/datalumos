import asyncio
from dataclasses import dataclass

from agents import set_default_openai_key
from agents.mcp import MCPServerStdio
from datalumos.agents.profile_flow import profile
from datalumos.agents.assert_validity_flow import run_column_validation

from datalumos.config import config
from datalumos.core import DEFAULT_POSTGRES_CONFIG
from datalumos.logging import get_logger, setup_logging
from datalumos.services.postgres import PostgresDB

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


async def run(
    table_name: str, schema: str, config: AgentConfig
):
    """Main orchestration function - runs all three agents sequentially"""

    if config.openai_key:
        set_default_openai_key(config.openai_key)

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

        table_profile_results = await profile(schema=schema, table_name=table_name, db=db, mcp_server=mcp_server)
        
        columns = db.get_column_names(table=table_name, schema=schema)
        
        validation_results = await run_column_validation(
            table_profile_results=table_profile_results,
            columns=columns,
            schema=schema,
            table_name=table_name,
            db=db,
            mcp_server=mcp_server
        )
        
        return table_profile_results, validation_results



if __name__ == "__main__":
    async def main():
        await run(schema="datalumos", table_name="dtdc_curier", config=AgentConfig.from_env())

    asyncio.run(main())