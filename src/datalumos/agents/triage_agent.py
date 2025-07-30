from enum import Enum

from agents import Agent, WebSearchTool, Runner
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.tools import get_file_search_tool
from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config

NAME = "Triage Agent"


class TriageAgent(Agent):
    """Triage Agent configured for triaging column analysis."""

    def __init__(self, mcp_servers: list[MCPServerStdio]):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())

        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME),
            output_type=TriageOutput,
            tools=tools,
            mcp_servers=mcp_servers,
            model=config.OPENAI_API_MODEL,
        )


class ColumnImportance(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ColumnClassification(BaseModel):
    """Classification results for a single column"""

    column_name: str
    column_type: str
    classification: ColumnImportance
    reasoning: str = Field(
        description="Reasoning process for the column classification"
    )


class TriageOutput(BaseModel):
    """Complete structured output for data validation results"""

    column_classifications: list[ColumnClassification] = Field(
        description="List of column classifications"
    )
