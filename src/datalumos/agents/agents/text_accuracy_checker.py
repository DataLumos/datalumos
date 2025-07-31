from agents import Agent, WebSearchTool
from pydantic import BaseModel, Field

from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config
from datalumos.tools.file_search import get_file_search_tool

NAME = "Text Accuracy Checker"


class TextAccuracyOutput(BaseModel):
    """Structured output for checking categorical accuracy of column values."""

    column_name: str = Field(..., description="Name of the column being analyzed")
    can_check_accuracy: bool = Field(
        ...,
        description="Whether the column context allows checking against "
        "real-world categorical data.",
    )
    incorrect_values: list[str] = Field(
        default_factory=list,
        description="Values that do not match any real-world category.",
    )
    inconsistent_representations: list[list[str]] = Field(
        default_factory=list,
        description="Groups of values that refer to the same category but are "
        "represented inconsistently.",
    )
    notes: str = Field(
        default="",
        description="Additional comments, explanations, or assumptions made during "
        "the check.",
    )


class TextAccuracyCheckerAgent(Agent):
    """
    Agent to assess categorical accuracy of column values based on real-world
    categories."""

    def __init__(
        self,
    ):
        tools = [WebSearchTool()]
        tools.extend(get_file_search_tool())

        instructions = load_agent_prompt(NAME)

        super().__init__(
            name=NAME,
            instructions=instructions,
            output_type=TextAccuracyOutput,
            tools=tools,
            model=config.OPENAI_API_MODEL,
        )
