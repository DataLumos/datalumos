from agents import WebSearchTool
from pydantic import BaseModel
from agents import Agent
from datalumos.agents.config import MODEL
from agents.mcp import MCPServerStdio
from datalumos.agents.utils import load_agent_prompt

NAME = "Data Explorer"


class DataExplorerAgent(Agent):
    """Data Explorer agent configured for table analysis and business context extraction."""

    def __init__(self, mcp_servers: list[MCPServerStdio], table_name: str, columns: list[str]):
        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME).format(table_name=table_name, columns=columns),
            output_type=TableAnalysisOutput,
            tools=[WebSearchTool()],
            mcp_servers=mcp_servers,
            model=MODEL
        )


class TableAnalysisOutput(BaseModel):
    """Structured output for table analysis results."""
    table_description: str
    business_context: str
    dataset_type: str
