# DataLumos Connectors 🔌

Load your data into PostgreSQL before running DataLumos analysis.
DataLumos requires your data to be in a PostgreSQL database for analysis. This module provides simple commands to load data from various sources into the local PostgreSQL instance.
Built with [dlt](https://dlthub.com).

## Features

- 🚀 **One-line data loading** from multiple sources
- 🎯 **Auto-configured destination** (local Docker PostgreSQL)
- 📊 **Supports PostgreSQL, S3, and filesystem** sources
- 🔍 **Smart schema inference** and data type detection
- ⚡ **Built on dlt** for reliability and performance
- 📱 **CLI interface** for easy command-line usage

## Quick Start

### 1. Start PostgreSQL Database
```bash
cd docker/
docker-compose up -d
```

### 2. Install Package
```bash
uv venv
uv sync
```

### 3. Loading Data

3.1 From local files
```bash
datalumos-ingest filesystem /path/to/data --glob "*.csv"

# Load JSON files and define the table name in the destination db
datalumos-ingest filesystem /path/to/data --glob "*.json" --table  TABLE_NAME

# Load all files in directory
datalumos-ingest filesystem /path/to/data
```

3.2 From S3
```bash
datalumos-ingest s3 s3://bucket-name/path/ --key YOUR_ACCESS_KEY --secret YOUR_SECRET_KEY

# With file pattern
datalumos-ingest s3 s3://bucket-name/data/ --glob "*.parquet" --key YOUR_KEY --secret YOUR_SECRET
```

3.3 From PostgreSQL
```bash
datalumos-ingest postgres postgresql://user:pass@host:5432/database
# Load entire database
datalumos-ingest postgres postgresql://user:password@host:5432/database

# Load specific tables
datalumos-ingest postgres postgresql://user:pass@host:5432/db --tables customers,orders,products
```

### CLI Options
- `--glob, --pattern`: File pattern to match (default: '*')
- `--format`: File format hint (csv, json, jsonl, parquet)
- `--tables`: Comma-separated list of tables (PostgreSQL only)
- `--schema`: Database schema (PostgreSQL only, default: public)
- `--key`: AWS Access Key ID (S3 only)
- `--secret`: AWS Secret Access Key (S3 only)
- `--dataset`: Name of the schema to which the dataset will be loaded.
- `--pipeline`: Custom pipeline name
- `--table`: Name of the table to which the dataset will be loaded
- `--verbose, -v`: Enable verbose logging

### 4 Python API

```bash
from datalumos.connectors import load_data

# Load from filesystem
result = load_data(
    source_type="filesystem",
    source_config={
        "path": "/data/exports",
        "file_glob": "*.csv"
    }
)

# Load from S3
result = load_data(
    source_type="s3",
    source_config={
        "bucket_url": "s3://my-bucket/data/",
        "aws_access_key_id": "YOUR_KEY",
        "aws_secret_access_key": "YOUR_SECRET"
    }
)

# Load from PostgreSQL
result = load_data(
    source_type="postgres",
    source_config={
        "connection_string": "postgresql://user:pass@host:5432/database"
    }
)
```

## Examples

See `examples.py` for complete working examples.
