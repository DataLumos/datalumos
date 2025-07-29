from agents import Agent
from agents.mcp import MCPServerStdio
from pydantic import BaseModel, Field
from enum import Enum
from datalumos.agents.config import MODEL
from datalumos.agents.utils import load_agent_prompt
from datalumos.agents.tools import get_file_search_tool

NAME = "Data Validator"


class DataValidatorAgent(Agent):
    """Data Validator agent configured for comprehensive data validation."""

    def __init__(
        self,
        mcp_servers: list[MCPServerStdio],
        column_name: str,
        table_context: str,
        table_name: str,
        schema_name: str,
        input_format: str
    ):

        super().__init__(
            name=NAME,

            # TODO: Move this formatting in the load_agent_prompt
            instructions=load_agent_prompt(NAME).format(input_format=input_format, table_name=table_name, schema_name=schema_name,
                                                        table_context=table_context),
            output_type=DataValidatorOutput,
            tools=get_file_search_tool(),
            mcp_servers=mcp_servers,
            model=MODEL
        )


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class SampleViolation(BaseModel):
    """Individual violation example"""
    invalid_value: str = Field(
        description="The problematic value that violates the rule")


class ValidationResults(BaseModel):
    """Results of a specific validation rule"""
    violation_count: int = Field(
        description="Number of records violating this rule")
    severity: Severity = Field(description="Severity level of the violation")
    sample_violations: list[SampleViolation] = Field(
        default_factory=list,
        description="Sample values showing the violation",
        max_items=5  # Limit samples to keep output manageable
    )


class RuleValidation(BaseModel):
    """Individual validation rule and its results"""
    rule_id: str = Field(description="Sequential ID like 'R001', 'R002', etc.")
    original_requirement: str = Field(
        description="Business requirement as originally stated")
    validation_rule: str = Field(
        description="Precise, testable restatement of the requirement")
    sql_query: str = Field(
        description="SQL SELECT statement that tests this rule")
    validation_results: ValidationResults


class ColumnValidation(BaseModel):
    """Validation results for a single column"""
    column_name: str
    column_type: str = Field(description="Data type of the column")
    rules_validated: list[RuleValidation] = Field(
        default_factory=list,
        description="All validation rules applied to this column"
    )


class DataValidatorOutput(BaseModel):
    """Complete structured output for data validation results"""
    column_validation: ColumnValidation = Field(
        description="Validation result for individual column"
    )
