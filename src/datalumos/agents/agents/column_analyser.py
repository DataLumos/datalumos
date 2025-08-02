from typing import Literal

from agents import Agent, WebSearchTool
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config
from datalumos.tools.file_search import get_file_search_tool

NAME = "Column Analyser"


class ColumnAnalyserAgent(Agent):
    """Column Analyser agent configured for column analysis."""

    def __init__(self, mcp_servers: list[MCPServerStdio]):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())
        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME),
            output_type=ColumnAnalysisOutput,
            tools=tools,
            mcp_servers=mcp_servers,
            model=config.OPENAI_API_MODEL,
        )


class ColumnAnalysisOutput(BaseModel):
    """Structured output for column analysis results."""

    column_name: str = Field(description="Name of the column being analyzed")
    business_definition: str = Field(
        description="Business context and meaning of the column"
    )
    data_type: str = Field(..., description="Data type of the column")
    canonical_data_type: Literal[
        "string", "integer", "float", "date", "boolean", "decimal", "timestamp"
    ] = Field(..., description="Canonical data type for validation")
    technical_specification: list[str] = Field(
        description="Structured specification of what makes the column value valid"
    )
    sources: list[str] = Field(
        default_factory=list,
        description="Authoritative references used for this definition",
    )
    other_notes: str = Field(description="Additional relevant information")

    @classmethod
    def describe(cls) -> dict:
        """Return a dictionary of field names and their descriptions"""
        return {
            field_name: field.description
            for field_name, field in cls.model_fields.items()
        }
