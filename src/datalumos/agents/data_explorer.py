from agents import Agent, WebSearchTool, Runner
from agents.mcp import MCPServerStdio
from pydantic import BaseModel

from pathlib import Path
from typing import Final
from datalumos.agents.tools import get_file_search_tool
from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config

NAME = "Data Explorer"

_CACHE_DIR: Final = Path.home() / ".datalumos" / "table_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

class DataExplorerAgent(Agent):
    """Data Explorer agent configured for table analysis and business context extraction."""

    def __init__(
        self, mcp_servers: list[MCPServerStdio], table_name: str, columns: list[str]
    ):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())

        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME).format(
                table_name=table_name, columns=columns
            ),
            output_type=TableAnalysisOutput,
            tools=tools,
            mcp_servers=mcp_servers,
            model=config.OPENAI_API_MODEL,
        )


class TableAnalysisOutput(BaseModel):
    """Structured output for table analysis results."""

    table_description: str
    business_context: str
    dataset_type: str


async def get_table_context(
    schema: str, table: str, db, mcp_server, *, force_refresh: bool = False
) -> TableAnalysisOutput:
    """
    Get the table context for a given table. If the table context is already cached, return the cached value.
    Otherwise, run the DataExplorerAgent to get the table context and save it to the cache.
    """
    if not force_refresh and (cached := load(schema, table)):
        return cached

    explorer = DataExplorerAgent(
        mcp_servers=[mcp_server],
        table_name=table,
        columns=db.get_column_names(schema=schema, table=table),
    )
    res = await Runner.run(explorer, f"Analyse {schema}.{table}")
    save(schema, table, res.final_output)
    return res.final_output


def _file(schema: str, table: str) -> Path:
    return _CACHE_DIR / f"{schema}.{table}.json"


def load(schema: str, table: str) -> TableAnalysisOutput | None:
    p = _file(schema, table)
    return TableAnalysisOutput.model_validate_json(p.read_text()) if p.exists() else None


def save(schema: str, table: str, ctx: TableAnalysisOutput) -> None:
    _file(schema, table).write_text(ctx.model_dump_json(indent=2))
