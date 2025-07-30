from pathlib import Path
from typing import Final

from agents import Agent, WebSearchTool
from agents.mcp import MCPServerStdio
from pydantic import BaseModel

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
