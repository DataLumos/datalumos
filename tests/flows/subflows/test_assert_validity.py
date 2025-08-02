"""Integration tests for column validation functionality using deepeval."""

import pytest
from deepeval import assert_test
from deepeval.metrics import GEval
from deepeval.test_case import LLMTestCase, LLMTestCaseParams

from datalumos.flows.flow import AgentConfig
from datalumos.flows.subflows.assert_validity import _validate_single_column
from datalumos.MCPs.postgres import postgres_mcp_server
from datalumos.services.langfuse.setup import setup_langfuse
from datalumos.services.postgres.connection import Column
from tests.flows.subflows.validation_cases.utils import (
    ValidationTestCase,
    load_all_test_cases,
)

setup_langfuse()


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.parametrize(
    "test_case_data", load_all_test_cases(), ids=lambda case: case.case_name
)
async def test_validate_column(test_case_data: ValidationTestCase):
    """Test column validation for all test cases."""

    # Create real column object
    column = Column(
        name=test_case_data.column_name,
        data_type=test_case_data.column_analysis.data_type,
    )

    # Use the real postgres MCP server
    agent_config = AgentConfig.from_env()
    async with postgres_mcp_server(agent_config) as mcp_server:
        # Call the actual validation function
        result = await _validate_single_column(
            column=column,
            column_analysis=test_case_data.column_analysis,
            schema=test_case_data.schema,
            table_name=test_case_data.table_name,
            mcp_server=mcp_server,
        )

    # Verify we got a result
    assert result is not None
    assert result.column_validation.column_name == test_case_data.column_name

    # Test each expected outcome using deepeval
    for outcome_name, outcome_config in test_case_data.expected_outcomes.items():
        llm_test_case = LLMTestCase(
            input=f"Validate {test_case_data.column_name} column in the table {test_case_data.schema}.{test_case_data.table_name} Column analysis output: {test_case_data.column_analysis}",
            actual_output=str(result.column_validation),
            expected_output=f"Validation addressing {outcome_name}",
        )

        # Define GEval metric
        metric = GEval(
            name=outcome_name.replace("_", " ").title(),
            evaluation_params=[LLMTestCaseParams.ACTUAL_OUTPUT],
            criteria=outcome_config["criteria"],
            evaluation_steps=outcome_config["evaluation_steps"],
            threshold=outcome_config["threshold"],
        )

        # Run the evaluation
        assert_test(llm_test_case, [metric])


@pytest.mark.asyncio
@pytest.mark.integration
async def test_validate_column_no_analysis():
    """Test validation when no column analysis is provided."""

    column = Column(name="test_column", data_type="varchar")

    agent_config = AgentConfig.from_env()
    async with postgres_mcp_server(agent_config) as mcp_server:
        result = await _validate_single_column(
            column=column,
            column_analysis=None,
            schema="test_schema",
            table_name="test_table",
            mcp_server=mcp_server,
        )

    assert result is None
