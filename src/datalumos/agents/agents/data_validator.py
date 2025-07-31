from enum import Enum

from agents import Agent
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field

from datalumos.agents.utils import load_agent_prompt
from datalumos.config import config
from datalumos.tools.file_search import get_file_search_tool

NAME = "Data Validator"


class DataValidatorAgent(Agent):
    """Data Validator agent configured for comprehensive data validation."""

    def __init__(
        self,
        mcp_servers: list[MCPServerStdio],
    ):
        super().__init__(
            name=NAME,
            instructions=load_agent_prompt(NAME),
            output_type=DataValidatorOutput,
            tools=get_file_search_tool(),
            mcp_servers=mcp_servers,
            model=config.OPENAI_API_MODEL,
        )


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class SampleViolation(BaseModel):
    """Individual violation example"""

    invalid_value: str = Field(description="The problematic value that violates the rule")


class ValidationResults(BaseModel):
    """Results of a specific validation rule"""

    violation_count: int = Field(description="Number of records violating this rule")
    severity: Severity = Field(description="Severity level of the violation")


class RuleValidation(BaseModel):
    """Individual validation rule and its results"""

    rule_id: str = Field(description="Sequential ID like 'R001', 'R002', etc.")
    original_requirement: str = Field(
        description="Business requirement as originally stated"
    )
    validation_rule: str = Field(
        description="Precise, testable restatement of the requirement"
    )
    sql_query: str = Field(description="SQL SELECT statement that tests this rule")
    validation_results: ValidationResults


class ColumnValidation(BaseModel):
    """Validation results for a single column"""

    column_name: str
    column_type: str = Field(description="Data type of the column")
    quality_checks: list[RuleValidation] = Field(
        default_factory=list, description="All quality rules applied to this column"
    )


class DataValidatorOutput(BaseModel):
    """Complete structured output for data validation results"""

    column_validation: ColumnValidation = Field(
        description="Validation result for individual column"
    )
