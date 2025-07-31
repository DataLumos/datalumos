from agents import Agent, WebSearchTool
from pydantic import BaseModel, Field

from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config
from datalumos.tools.file_search import get_file_search_tool

NAME = "Date Accuracy Checker"


class DateAccuracyOutput(BaseModel):
    """Structured output for checking date accuracy of column values."""

    column_name: str = Field(..., description="Name of the column being analyzed")
    can_check_accuracy: bool = Field(
        ...,
        description="Whether the column context allows checking against "
        "real-world date constraints.",
    )
    invalid_dates: list[str] = Field(
        default_factory=list,
        description="Values that are not valid dates or have invalid formats.",
    )
    out_of_range_dates: list[str] = Field(
        default_factory=list,
        description="Dates that fall outside expected logical ranges.",
    )
    inconsistent_formats: list[str] = Field(
        default_factory=list,
        description="Dates with inconsistent formatting within the column.",
    )
    temporal_logic_issues: list[str] = Field(
        default_factory=list,
        description="Dates that violate temporal logic (e.g., end before start).",
    )
    notes: str = Field(
        default="",
        description="Additional comments, explanations, or assumptions made during "
        "the check.",
    )


class DateAccuracyCheckerAgent(Agent):
    """
    Agent to assess date accuracy of column values based on real-world
    constraints."""

    def __init__(
        self,
    ):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())

        instructions = load_agent_prompt(NAME)

        super().__init__(
            name=NAME,
            instructions=instructions,
            output_type=DateAccuracyOutput,
            tools=tools,
            model=config.OPENAI_API_MODEL,
        )
