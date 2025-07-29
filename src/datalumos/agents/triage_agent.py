from enum import Enum
from pydantic import BaseModel, Field
from agents import Agent
from agents.mcp import MCPServerStdio
from agents import WebSearchTool
from datalumos.agents.config import MODEL
from datalumos.agents.utils import load_agent_prompt

NAME = "Triage Agent"


class TriageAgent(Agent):
    """Triage Agent configured for triaging column analysis."""

    def __init__(
        self,
        mcp_servers: list[MCPServerStdio]
    ):

        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME),
            output_type=TriageOutput,
            tools=[WebSearchTool()],
            mcp_servers=mcp_servers,
            model=MODEL
        )

class ColumnImportance(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ColumnClassification(BaseModel):
    """Classification results for a single column"""
    column_name: str
    classification: ColumnImportance
    reasoning: str = Field(description="Reasoning process for the column classification")

class TriageOutput(BaseModel):
    """Complete structured output for data validation results"""
    column_classifications: list[ColumnClassification] = Field(
        description="List of column classifications"
    )
