from datetime import datetime
from pathlib import Path

import yaml

from datalumos import config
from datalumos.flows.subflows.assert_accuracy import AccuracyResults
from datalumos.flows.subflows.assert_completeness import CompletenessResults
from datalumos.flows.subflows.assert_validity import ValidationResults
from datalumos.flows.subflows.table_profiling import TableAnalysisResults

GENERIC_TO_POSTGRES = {
    "string": ["text", "varchar", "character varying"],
    "integer": ["integer", "int", "int4"],
    "float": ["double precision", "float8", "real"],
    "date": ["date"],
    "boolean": ["boolean", "bool"],
    "decimal": ["numeric", "decimal"],
    "timestamp": [
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
    ],
}


def generate_llm_ready_yaml_report(
    table_name: str,
    table_profile_result: TableAnalysisResults,
    validation_results: list[ValidationResults],
    accuracy_results: list[AccuracyResults],
    completeness_results: CompletenessResults | None = None,
) -> str:
    """Generate an LLM-readable YAML report from agentc validation output"""

    report = {
        "table_report": {
            "table_metadata": {
                "description": table_profile_result.table_context.table_description,
                "business_context": table_profile_result.table_context.business_context,
                "dataset_type": table_profile_result.table_context.dataset_type,
            },
            "high_importance_columns": [],
            "columns_with_type_mismatch": [],
            "validation_issues": [],
            "accuracy_issues": [],
            "completeness_issues": [],
        }
    }
    # Identify high-importance columns
    high_importance = {
        c.column_name
        for c in table_profile_result.column_triage.column_classifications
        if c.classification == "HIGH"
    }
    report["table_report"]["high_importance_columns"] = sorted(high_importance)

    for col in table_profile_result.column_analyses:
        if col.data_type != col.canonical_data_type:
            report["table_report"]["columns_with_type_mismatch"].append(
                {
                    "name": col.column_name,
                    "business_definition": col.business_definition,
                    "data_type": col.data_type,
                    "canonical_data_type": col.canonical_data_type,
                    "valid_spec": col.technical_specification,
                    "sources": col.sources,
                    "notes": col.other_notes,
                }
            )
    # Extract validation issues
    for col_val in validation_results.column_validations:
        for rule in col_val.column_validation.quality_checks:
            if rule.validation_results.violation_count > 0:
                report["table_report"]["validation_issues"].append(
                    {
                        "column": col_val.column_validation.column_name,
                        "rule_id": rule.rule_id,
                        "violation_count": rule.validation_results.violation_count,
                        "severity": rule.validation_results.severity.value,
                        "original_requirement": rule.original_requirement,
                        "failining_validation_rule": rule.validation_rule,
                        "validation_rule": rule.validation_rule,
                    }
                )

    # Extract accuracy issues
    for check in accuracy_results.text_accuracy:
        if check.incorrect_values or check.inconsistent_representations:
            report["table_report"]["accuracy_issues"].append(
                {
                    "column": check.column_name,
                    "invalid_values": check.incorrect_values,
                    "inconsistent_representations": check.inconsistent_representations,
                    "notes": check.notes,
                }
            )

    # Extract completeness issues (low fill rates)
    if completeness_results:
        for fill_rate_info in completeness_results.column_fill_rates:
            if (
                fill_rate_info.fill_rate_percentage < 95.0
            ):  # Flag columns with <95% fill rate
                report["table_report"]["completeness_issues"].append(
                    {
                        "column": fill_rate_info.column_name,
                        "null_count": fill_rate_info.null_count,
                        "fill_rate_percentage": fill_rate_info.fill_rate_percentage,
                    }
                )

    output_path = (
        Path(config.config.REPORT_OUTPUT_DIR)
        / f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
    )
    _write_report_file(
        output_path, yaml.dump(report, sort_keys=False, allow_unicode=True, width=120)
    )
    return output_path


def _write_report_file(output_path: Path, content: str) -> None:
    """Write report content to file.

    Args:
        output_path: Path where to write the report
        content: YAML content to write

    Raises:
        IOError: If file writing fails.
    """
    try:
        with open(output_path, "w") as f:
            f.write(content)

    except Exception as e:
        raise OSError(f"Failed to write report file {output_path}: {e}") from e
