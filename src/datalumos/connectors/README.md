# Data Lumos Connectors

Simple data loading with [dlt](https://dlthub.com) - load data from various sources into your local PostgreSQL database with a single function call.

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
pip install -e .
```

### 3. Load Data (CLI)
```bash
# Load from local files
datalumos-ingest filesystem /path/to/csv/files

# Load from S3
datalumos-ingest s3 s3://my-bucket/data/ --glob "*.csv" --key YOUR_KEY --secret YOUR_SECRET

# Load from PostgreSQL
datalumos-ingest postgres postgresql://user:pass@host:5432/database
```

### 3. Load Data (Python)
```python
from datalumos.connectors import load_data

# Load from PostgreSQL
load_data(
    source_type="postgres",
    source_config={
        "connection_string": "postgresql://user:pass@host:port/database"
    }
)

# Load from S3
load_data(
    source_type="s3", 
    source_config={
        "bucket_url": "s3://my-bucket/data/",
        "file_glob": "*.csv"
    }
)

# Load from local files
load_data(
    source_type="filesystem",
    source_config={
        "path": "/path/to/data",
        "file_glob": "*.csv"
    }
)
```

## CLI Usage

```bash
# Basic usage
datalumos-ingest <source_type> <source_path> [options]

# Filesystem examples
datalumos-ingest filesystem /data/sales --glob "*.csv"
datalumos-ingest filesystem /logs --format json --dataset sales_data

# S3 examples  
datalumos-ingest s3 s3://bucket/path --glob "*.parquet" --key ACCESS_KEY --secret SECRET_KEY
datalumos-ingest s3 s3://data-lake/raw --format csv

# PostgreSQL examples
datalumos-ingest postgres postgresql://user:pass@host:5432/db
datalumos-ingest postgres "postgresql://user:pass@host/db" --tables customers,orders --schema analytics

# Advanced options
datalumos-ingest filesystem /data --dataset custom_name --pipeline monthly_load --verbose
```

### CLI Options
- `--glob, --pattern`: File pattern to match (default: '*')
- `--format`: File format hint (csv, json, jsonl, parquet)
- `--tables`: Comma-separated list of tables (PostgreSQL only)
- `--schema`: Database schema (PostgreSQL only, default: public)
- `--key`: AWS Access Key ID (S3 only)
- `--secret`: AWS Secret Access Key (S3 only)
- `--dataset`: Custom dataset name
- `--pipeline`: Custom pipeline name
- `--verbose, -v`: Enable verbose logging

## Prerequisites

1. **Docker PostgreSQL** running locally:
   ```bash
   cd docker/
   docker-compose up -d
   ```

2. **Install dependencies**:
   ```bash
   pip install dlt[postgres]
   # For S3: pip install dlt[s3]
   # For additional databases: pip install dlt[postgres,mysql,...]
   ```

## Source Configuration

### PostgreSQL
```python
{
    "connection_string": "postgresql://user:pass@host:port/database",
    "tables": ["table1", "table2"],  # Optional: specific tables
    "schema": "public"               # Optional: schema name
}
```

### S3
```python
{
    "bucket_url": "s3://bucket-name/path/",
    "file_glob": "*.csv",                    # Optional: file pattern
    "aws_access_key_id": "...",             # Optional: if not using IAM
    "aws_secret_access_key": "...",         # Optional: if not using IAM
    "file_format": "csv"                    # Optional: format hint
}
```

### Filesystem
```python
{
    "path": "/path/to/data",
    "file_glob": "*.csv",     # Optional: file pattern
    "recursive": True,        # Optional: search subdirectories
    "file_format": "csv"      # Optional: format hint
}
```

## Return Value

The `load_data()` function returns a dictionary with:

```python
{
    "status": "success",
    "pipeline_name": "datalumos_postgres_mydb",
    "dataset_name": "postgres_mydb", 
    "source_type": "postgres",
    "load_info": <dlt LoadInfo object>,
    "table_names": ["customers", "orders", ...]
}
```

## Advanced Usage

### Custom Names
```python
load_data(
    source_type="filesystem",
    source_config={"path": "/sales/data"},
    dataset_name="sales_analytics",
    pipeline_name="monthly_sales_import"
)
```

### Check Pipeline Info
```python
from datalumos.connectors import get_pipeline_info

info = get_pipeline_info("datalumos_postgres_mydb")
print(f"Last load: {info['last_load_info']}")
```

### Error Handling
```python
from datalumos.connectors import load_data, DataLoadError

try:
    result = load_data("postgres", config)
    print(f"✅ Loaded {len(result['table_names'])} tables")
except DataLoadError as e:
    print(f"❌ Load failed: {e}")
```

## Configuration

Default PostgreSQL destination (can be customized in `config.py`):
- Host: `localhost:5432`
- Database: `datalumos`
- User: `datalumos` 
- Password: `datalumos123`

## Examples

See `examples.py` for complete working examples.

## Troubleshooting

- **Connection failed**: Ensure PostgreSQL is running and accessible
- **Import errors**: Install required dependencies (`pip install dlt[postgres]`)
- **Permission denied**: Check file/database permissions
- **Schema errors**: dlt automatically handles schema inference and evolution 