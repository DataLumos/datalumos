"""Utilities for loading and managing validation test cases."""

import importlib
from pathlib import Path
from typing import Any

from datalumos.agents.agents.column_analyser import ColumnAnalysisOutput


class ValidationTestCase:
    """A validation test case loaded from a case file."""

    def __init__(self, case_name: str, case_module):
        self.case_name = case_name
        self.column_analysis: ColumnAnalysisOutput = case_module.COLUMN_ANALYSIS
        self.column_name: str = case_module.COLUMN_NAME
        self.schema: str = case_module.SCHEMA
        self.table_name: str = case_module.TABLE_NAME
        self.expected_outcomes: dict[str, Any] = case_module.EXPECTED_OUTCOMES


def load_test_case(case_name: str) -> ValidationTestCase:
    """Load a specific test case by name."""
    try:
        module_name = f"tests.flows.subflows.validation_cases.{case_name}"
        case_module = importlib.import_module(module_name)
        return ValidationTestCase(case_name, case_module)
    except ImportError as e:
        raise ValueError(f"Test case '{case_name}' not found: {e}")


def load_all_test_cases() -> list[ValidationTestCase]:
    """Load all available test cases from the validation_cases directory."""
    cases_dir = Path(__file__).parent
    test_cases = []

    for file_path in cases_dir.glob("*_case.py"):
        case_name = file_path.stem  # Remove .py extension
        try:
            test_case = load_test_case(case_name)
            test_cases.append(test_case)
        except ValueError as e:
            print(f"Warning: Could not load test case {case_name}: {e}")

    return test_cases


def get_case_names() -> list[str]:
    """Get names of all available test cases."""
    cases_dir = Path(__file__).parent
    return [f.stem for f in cases_dir.glob("*_case.py")]
