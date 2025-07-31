# DataLumos 🔍

An AI that actually understands your data before your code agents try to work with it.

## What is this

DataLumos is an agentic system that investigates your datasets and finds the issues hiding in them. 

Think of it like a flashlight for your data. It pokes around, figures out what's wrong, understands what the data *should* look like in your business context, and hands over a clean summary for you (or your code-writing AI agent) to fix things properly.

## What's broken right now

You scraped some data with BrowserUse? Great. You have a file that should be useful? Cool. But when Cursor tries to help you process it, it's flying blind. Your data is a black box.

DataLumos is the missing piece - it shows you what's broken in the data

## What makes it different

🔍 **Context-aware inspections**

Most data quality tools run generic checks. DataLumos digs into the business context first.

Got US street addresses? DataLumos knows about USPS Publication 28 rules - that "ALLEE" should be "ALY", and suffixes go after street names. Got financial data? It understands decimal precision requirements. 

**Context first, then validation.** Because not every dataset needs the same QA checks. 

🛠️ **LLM-readable issue reports**

Findings are formatted so that your LLM agent can act on them immediately. You don't need to explain anything - it already (finally) knows what to do.

📦 **Built-in rules + agentic exploration**

It doesn't just run a few checks. It thinks. Then it checks everything.

## How it works

1. **Investigate** - Figures out what your data actually represents
2. **Contextualize** - Finds the real-world rules that apply to your domain
3. **Validate** - Checks completeness, accuracy, validity, and timeliness
4. **Report** - Gives you LLM-readable insights about what's broken

No more "why is my agent hallucinating?" moments. Now you'll know it's because 30% of your postal codes are missing and half your dates are in the wrong format.

## The comprehensive bit

DataLumos doesn't just spot obvious problems. It runs through a systematic framework covering:

- **Completeness** - What's missing and why it matters
- **Accuracy** - What's wrong and how wrong it is
- **Validity** - What violates domain rules
- **Timeliness** - What's stale and what's not

## When should I use it?

- Before shipping data to prod
- Before feeding data into an LLM
- Before asking an LLM to build a data pipeline for processing raw data on which it has zero context
- After scraping or collecting data from any new source
- Anytime you want to *know* if there's something wrong with your data

---

## Installation

### Prerequisites

- Python 3.11 or higher
- PostgreSQL (optional, for database sources)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd datalumos
```

2. Install dependencies using uv (recommended):
```bash
uv sync
```

Or using pip:
```bash
pip install -e .
```

3. For development dependencies:
```bash
uv sync --group dev
```

## Usage

### Data Ingestion CLI

Use the built-in CLI tool to ingest data from various sources:

```bash
datalumos-ingest filesystem ../datalumos2/data/ --glob "items_tyler_sanitized.json" --format jsonl --dataset tyler_items_data_test --verbose
```

Additional examples:
```bash
# Load CSV files from directory
datalumos-ingest filesystem /path/to/csv/files --glob "*.csv"

# Load from S3 bucket
datalumos-ingest s3 s3://my-bucket/data/ --glob "*.csv" --key YOUR_KEY --secret YOUR_SECRET

# Load from PostgreSQL database
datalumos-ingest postgres postgresql://user:pass@host:5432/database
```

See the [connectors README](src/datalumos/connectors/README.md) for more detailed usage examples.

### Running Agents

The main agent system can be started with:

```python
from datalumos.agents import main
```

### Testing

Run tests with pytest:
```bash
pytest
```

## Running the Agentic System

You can run the DataLumos QA agentic system from the command line using the following command:

```sh
datalumos-qa --table <TABLE_NAME> --schema <SCHEMA_NAME>
```

Replace `<TABLE_NAME>` with the name of your database table and `<SCHEMA_NAME>` with the schema containing the table.

### Example

```sh
datalumos-qa --table dtdc_curier --schema datalumos
```

## Observability

DataLumos supports [Langfuse](https://langfuse.com/) for observability and tracing of AI agent interactions. To enable it, set the following environment variables:

```bash
LANGFUSE_PUBLIC_KEY=your_public_key
LANGFUSE_SECRET_KEY=your_secret_key
LANGFUSE_HOST=https://cloud.langfuse.com  # or your self-hosted instance
```

Langfuse will automatically track OpenAI API calls, agent workflows, and provide detailed tracing for debugging and monitoring your data analysis runs.

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and quality checks
5. Submit a pull request