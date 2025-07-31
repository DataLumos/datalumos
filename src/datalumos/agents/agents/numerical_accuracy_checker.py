from agents import Agent, WebSearchTool
from pydantic import BaseModel, Field

from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config
from datalumos.tools.file_search import get_file_search_tool

NAME = "Numerical Accuracy Checker"


class NumericalAccuracyOutput(BaseModel):
    """Structured output for checking numerical accuracy of column values."""

    column_name: str = Field(..., description="Name of the column being analyzed")
    can_check_accuracy: bool = Field(
        ...,
        description="Whether the column context allows checking against "
        "real-world numerical constraints.",
    )
    out_of_range_values: list[str] = Field(
        default_factory=list,
        description="Values that fall outside expected numerical ranges.",
    )
    statistical_outliers: list[str] = Field(
        default_factory=list,
        description="Values that are statistical outliers based on domain knowledge.",
    )
    format_issues: list[str] = Field(
        default_factory=list,
        description="Values with incorrect numerical formatting or precision.",
    )
    notes: str = Field(
        default="",
        description="Additional comments, explanations, or assumptions made during "
        "the check.",
    )


class NumericalAccuracyCheckerAgent(Agent):
    """
    Agent to assess numerical accuracy of column values based on
    real-world constraints."""

    def __init__(
        self,
    ):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())

        instructions = load_agent_prompt(NAME)

        super().__init__(
            name=NAME,
            instructions=instructions,
            output_type=NumericalAccuracyOutput,
            tools=tools,
            model=config.OPENAI_API_MODEL,
        )
