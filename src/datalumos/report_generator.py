"""Production-ready report generator for DataLumos analysis results."""

import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from datalumos.config import config
from datalumos.flows.subflows.assert_accuracy import AccuracyResults
from datalumos.flows.subflows.assert_completeness import CompletenessResults
from datalumos.flows.subflows.assert_validity import ValidationResults
from datalumos.flows.subflows.table_profiling import TableAnalysisResults

logger = logging.getLogger(__name__)

# Mapping of canonical to PostgreSQL types
GENERIC_TO_POSTGRES: dict[str, list[str]] = {
    "string": ["text", "varchar", "character varying"],
    "integer": ["integer", "int", "int4"],
    "float": ["double precision", "float8", "real"],
    "date": ["date"],
    "boolean": ["boolean", "bool"],
    "decimal": ["numeric", "decimal"],
    "timestamp": ["timestamp", "timestamptz"],
}


def is_type_mismatch(pg_type: str, canonical_type: str) -> bool:
    """Check if PostgreSQL type doesn't match the canonical type.

    Args:
        pg_type: PostgreSQL data type
        canonical_type: Canonical data type

    Returns:
        True if types don't match, False otherwise
    """
    if not pg_type or not canonical_type:
        logger.warning(
            f"Empty type provided - pg_type: {pg_type}, canonical_type: {canonical_type}"
        )
        return False

    valid_types = GENERIC_TO_POSTGRES.get(canonical_type.lower(), [])
    return pg_type not in valid_types


def extract_high_importance_columns(table_result: TableAnalysisResults) -> list[str]:
    """Extract columns classified as high importance.

    Args:
        table_result: Table analysis results containing column classifications

    Returns:
        Sorted list of high importance column names
    """
    try:
        if not table_result or not table_result.column_triage:
            logger.warning("No column triage data available")
            return []

        if not table_result.column_triage.column_classifications:
            logger.warning("No column classifications available")
            return []

        high_importance_cols = [
            col.column_name
            for col in table_result.column_triage.column_classifications
            if col.classification == "HIGH" and col.column_name
        ]

        logger.debug(f"Found {len(high_importance_cols)} high importance columns")
        return sorted(high_importance_cols)

    except Exception as e:
        logger.error(f"Error extracting high importance columns: {e}")
        return []


def find_type_mismatches(table_result: TableAnalysisResults) -> list[dict[str, Any]]:
    """Find columns with type mismatches between actual and canonical types.

    Args:
        table_result: Table analysis results containing column analyses

    Returns:
        List of dictionaries containing mismatch details
    """
    mismatches = []

    try:
        if not table_result or not table_result.column_analyses:
            logger.warning("No column analyses available for type mismatch detection")
            return []

        for col in table_result.column_analyses:
            if not col.column_name:
                logger.warning("Column with empty name found, skipping")
                continue

            if is_type_mismatch(col.data_type, col.canonical_data_type):
                mismatches.append(
                    {
                        "name": col.column_name,
                        "business_definition": getattr(col, "business_definition", None),
                        "data_type": col.data_type,
                        "canonical_data_type": col.canonical_data_type,
                        "valid_spec": getattr(col, "technical_specification", None),
                        "sources": getattr(col, "sources", None),
                        "notes": getattr(col, "other_notes", None),
                    }
                )

        logger.debug(f"Found {len(mismatches)} type mismatches")
        return mismatches

    except Exception as e:
        logger.error(f"Error finding type mismatches: {e}")
        return []


def extract_validation_issues(
    validation_results: list[ValidationResults],
) -> list[dict[str, Any]]:
    """Extract validation issues from validation results.

    Args:
        validation_results: List of validation results to process

    Returns:
        List of dictionaries containing validation issue details
    """
    issues = []

    try:
        if not validation_results:
            logger.warning("No validation results provided")
            return []

        for val_result in validation_results:
            if not val_result or not hasattr(val_result, "column_validations"):
                logger.warning("Invalid validation result structure, skipping")
                continue

            for col_val in val_result.column_validations:
                if not col_val or not hasattr(col_val, "column_validation"):
                    logger.warning("Invalid column validation structure, skipping")
                    continue

                column_name = getattr(col_val.column_validation, "column_name", "Unknown")
                quality_checks = getattr(col_val.column_validation, "quality_checks", [])

                for rule in quality_checks:
                    if not rule or not hasattr(rule, "validation_results"):
                        continue

                    violation_count = getattr(rule.validation_results, "violation_count", 0)
                    if violation_count > 0:
                        issues.append(
                            {
                                "column": column_name,
                                "rule_id": getattr(rule, "rule_id", None),
                                "violation_count": violation_count,
                                "severity": getattr(
                                    rule.validation_results, "severity", None
                                ),
                                "original_requirement": getattr(
                                    rule, "original_requirement", None
                                ),
                                "validation_rule": getattr(rule, "validation_rule", None),
                            }
                        )

        logger.debug(f"Found {len(issues)} validation issues")
        return issues

    except Exception as e:
        logger.error(f"Error extracting validation issues: {e}")
        return []


def extract_accuracy_issues(
    accuracy_results: list[AccuracyResults],
) -> list[dict[str, Any]]:
    """Extract accuracy issues from accuracy results.

    Args:
        accuracy_results: List of accuracy results to process

    Returns:
        List of dictionaries containing accuracy issue details
    """
    issues = []

    try:
        if not accuracy_results:
            logger.warning("No accuracy results provided")
            return []

        for acc_result in accuracy_results:
            if not acc_result or not hasattr(acc_result, "text_accuracy"):
                logger.warning("Invalid accuracy result structure, skipping")
                continue

            for check in acc_result.text_accuracy:
                if not check:
                    continue

                incorrect_values = getattr(check, "incorrect_values", [])
                inconsistent_representations = getattr(
                    check, "inconsistent_representations", []
                )

                if incorrect_values or inconsistent_representations:
                    issues.append(
                        {
                            "column": getattr(check, "column_name", "Unknown"),
                            "can_check_accuracy": getattr(
                                check, "can_check_accuracy", None
                            ),
                            "invalid_values": incorrect_values,
                            "inconsistent_representations": inconsistent_representations,
                            "notes": getattr(check, "notes", None),
                        }
                    )

        logger.debug(f"Found {len(issues)} accuracy issues")
        return issues

    except Exception as e:
        logger.error(f"Error extracting accuracy issues: {e}")
        return []


def extract_completeness_issues(
    completeness_results: CompletenessResults | None, threshold: float = 95.0
) -> list[dict[str, Any]]:
    """Extract completeness issues from completeness results.

    Args:
        completeness_results: Completeness results to process
        threshold: Fill rate threshold below which to report issues

    Returns:
        List of dictionaries containing completeness issue details
    """
    try:
        if not completeness_results:
            logger.debug("No completeness results provided")
            return []

        if not hasattr(completeness_results, "column_fill_rates"):
            logger.warning("Invalid completeness results structure")
            return []

        issues = []
        for col in completeness_results.column_fill_rates:
            if not col:
                continue

            fill_rate = getattr(col, "fill_rate_percentage", 100.0)
            if fill_rate < threshold:
                issues.append(
                    {
                        "column": getattr(col, "column_name", "Unknown"),
                        "null_count": getattr(col, "null_count", None),
                        "fill_rate_percentage": fill_rate,
                    }
                )

        logger.debug(
            f"Found {len(issues)} completeness issues below {threshold}% threshold"
        )
        return issues

    except Exception as e:
        logger.error(f"Error extracting completeness issues: {e}")
        return []


def generate_llm_ready_yaml_report(
    table_profile_result: TableAnalysisResults,
    validation_results: list[ValidationResults],
    accuracy_results: list[AccuracyResults],
    completeness_results: CompletenessResults | None = None,
    table_name: str | None = None,
    output_path: Path | None = None,
) -> str:
    """Generate an LLM-ready YAML report and write to configured output path.

    Args:
        table_profile_result: Table profiling analysis results
        validation_results: List of validation results
        accuracy_results: List of accuracy results
        completeness_results: Optional completeness results
        output_path: Optional custom output path, uses config if not provided

    Returns:
        YAML string of the generated report

    Raises:
        ValueError: If required parameters are missing or invalid
        IOError: If file writing fails
    """
    logger.info("Starting report generation")

    # Validate required inputs
    if not table_profile_result:
        raise ValueError("table_profile_result is required")

    if validation_results is None:
        validation_results = []

    if accuracy_results is None:
        accuracy_results = []

    try:
        # Extract table metadata safely
        table_context = getattr(table_profile_result, "table_context", None)
        table_metadata = {
            "description": (
                getattr(table_context, "table_description", None) if table_context else None
            ),
            "business_context": (
                getattr(table_context, "business_context", None) if table_context else None
            ),
            "dataset_type": (
                getattr(table_context, "dataset_type", None) if table_context else None
            ),
        }

        # Build report structure
        report = {
            "table_report": {
                "table_metadata": table_metadata,
                "high_importance_columns": extract_high_importance_columns(
                    table_profile_result
                ),
                "columns_with_type_mismatch": find_type_mismatches(table_profile_result),
                "validation_issues": extract_validation_issues(validation_results),
                "accuracy_issues": extract_accuracy_issues(accuracy_results),
                "completeness_issues": extract_completeness_issues(completeness_results),
            }
        }

        # Determine output path
        if output_path is None:
            output_dir = Path(config.REPORT_OUTPUT_DIR)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_suffix = table_name.replace(".", "_") if table_name else "unknown"
            filename = f"report_{table_suffix}_{timestamp}.yaml"
            output_path = output_dir / filename

        logger.info(f"Writing report to: {output_path}")

        # Generate YAML string
        yaml_string = yaml.dump(report, sort_keys=False, allow_unicode=True, width=120)

        # Write file atomically using temporary file
        _write_report_file(output_path, yaml_string)

        logger.info(f"Report successfully generated at {output_path}")
        return yaml_string

    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise


def _write_report_file(output_path: Path, content: str) -> None:
    """Write report content to file atomically.

    Args:
        output_path: Path where to write the report
        content: YAML content to write

    Raises:
        IOError: If file writing fails
    """
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to temporary file first, then move to final location for atomicity
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_file.write(content)
            temp_path = Path(temp_file.name)

        # Atomic move
        temp_path.replace(output_path)
        logger.debug(f"Report file written atomically to {output_path}")

    except Exception as e:
        # Clean up temp file if it exists
        try:
            if "temp_path" in locals():
                temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise OSError(f"Failed to write report file {output_path}: {e}") from e
