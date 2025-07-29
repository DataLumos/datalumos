from agents import Agent
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field
from agents import WebSearchTool

from datalumos.agents.config import MODEL
from datalumos.agents.utils import load_agent_prompt
from datalumos.agents.tools import get_file_search_tool


NAME = "Column Analyser"


class ColumnAnalyserAgent(Agent):
    """Column Analyser agent configured for column analysis."""

    def __init__(self, mcp_servers: list[MCPServerStdio], column_name: str, table_context: str):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())
        
        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME).format(
                column_name=column_name, table_context=table_context),
            output_type=ColumnAnalysisOutput,
            tools=tools,
            mcp_servers=mcp_servers,
            model=MODEL
        )


class ColumnAnalysisOutput(BaseModel):
    """Structured output for column analysis results."""
    column_name: str = Field(description="Name of the column being analyzed")
    business_definition: str = Field(
        description="Business context and meaning of the column")
    technical_specification: str = Field(
        description="Technical details including data type and format")
    valid_values: str = Field(
        description="Acceptable values or ranges for this column")
    quality_rules: str = Field(
        description="Data quality rules and validation criteria")
    sources: str = Field(
        description="Relevant sources from which the technical specifications were inferred.")
    other_notes: str = Field(description="Additional relevant information")
    table_description: str = Field(
        description="Description of the parent table")

    @classmethod
    def describe(cls) -> dict:
        """Return a dictionary of field names and their descriptions"""
        return {
            field_name: field.description
            for field_name, field in cls.__fields__.items()
        }
