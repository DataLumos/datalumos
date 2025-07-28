# DataLumos

An agentic system for data quality assurance and exploration using LLMs and multi-agent frameworks.

## Overview

DataLumos is a Python-based system that uses AI agents to analyze, validate, and explore data across different sources. 
It leverages LangChain, LangGraph, and MCP (Model Context Protocol) to provide intelligent data quality assessment and exploration capabilities.

## Features

- **Multi-Agent Architecture**: Specialized agents for different data QA tasks
  - Column Analyzer: Analyzes column properties and data types
  - Data Explorer: Performs exploratory data analysis
  - Data Validator: Validates data quality and integrity
- **Multiple Data Sources**: Supports PostgreSQL, S3, and filesystem sources
- **Flexible Connectors**: Extensible connector system for various data sources
- **CLI Interface**: Command-line tools for data ingestion and analysis

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

The main agent system can be accessed through:

```python
from datalumos.agents import main
```

### Docker Support

A Docker Compose configuration is provided in the `docker/` directory for easy deployment.

## Project Structure

```
src/datalumos/
├── agents/           # AI agents for data analysis
│   ├── column_analyser.py
│   ├── data_explorer.py
│   ├── data_validator.py
│   ├── main.py
│   └── prompts/      # Agent prompt templates
├── connectors/       # Data source connectors
│   ├── sources/      # Source implementations
│   │   ├── filesystem_source.py
│   │   ├── postgres_source.py
│   │   └── s3_source.py
│   └── cli.py        # Command-line interface
```

## Configuration

The system uses environment-based configuration. Key dependencies include:

- **LLM Providers**: Anthropic Claude, OpenAI
- **Data Processing**: Pandas, SQLAlchemy
- **Agent Framework**: LangChain, LangGraph
- **Data Loading**: dlt (data load tool)

## Development

### Code Quality

The project uses several tools for code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **Ruff**: Linting
- **MyPy**: Type checking

Run quality checks:
```bash
black src/
isort src/
ruff check src/
mypy src/
```

### Testing

Run tests with pytest:
```bash
pytest
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and quality checks
5. Submit a pull request