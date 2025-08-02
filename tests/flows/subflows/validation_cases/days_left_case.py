"""Test case for days_left column validation."""

from datalumos.agents.agents.column_analyser import ColumnAnalysisOutput

# Test metadata
COLUMN_NAME = "days_left"
SCHEMA = "datalumos"
TABLE_NAME = "airlines_flights_data"

# Column analysis data
COLUMN_ANALYSIS = ColumnAnalysisOutput(
    column_name='days_left',
    business_definition="The 'days_left' column represents the number of days remaining until a flight departs. It is used to track the proximity of scheduled flights, aiding logistics and passenger planning.",
    data_type='character varying',
    canonical_data_type='integer',
    technical_specification=[
        'Must be a positive integer',
        'Should represent days remaining before departure'
    ],
    sources=[
        'Indian Domestic Airlines Operations Documents',
        'General Aviation Scheduling Standards'
    ],
    other_notes="Although stored as 'character varying', the values are essentially integers representing days. Ensure conversion to integer for calculations and validations."
)


# Expected outcomes for evaluation
EXPECTED_OUTCOMES = {
    "sql_validation_check": {
        "criteria": "The validation should generate a valid SQL query to check the technical specification 'Must be a positive integer' and identify exactly 2 records that don't satisfy this requirement",
        "evaluation_steps": [
            "Check if a valid SQL SELECT query is generated to test the positive integer requirement",
            "Verify the SQL query correctly identifies non-positive integer values", 
            "Confirm that exactly 2 violation records are found and reported",
            "Ensure the SQL syntax is correct and executable"
        ],
        "threshold": 0.8
    }
}