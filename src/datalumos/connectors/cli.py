#!/usr/bin/env python3
"""CLI interface for Data Lumos ingestion."""

import argparse
import sys
from typing import Any

from datalumos.connectors.main import DataLoadError, load_data
from datalumos.connectors.utils import logger, setup_logging


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest data from various sources into PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ingest filesystem /path/to/csv/files
  ingest filesystem /data --glob "*.json"
  ingest s3 s3://my-bucket/data/
  ingest s3 s3://bucket/path --glob "*.csv" --key YOUR_KEY --secret YOUR_SECRET
  ingest postgres postgresql://user:pass@host:5432/database
  ingest postgres postgresql://user:pass@host/db --tables customers,orders
        """,
    )

    parser.add_argument(
        "source_type",
        choices=["filesystem", "s3", "postgres"],
        help="Type of data source to ingest from",
    )

    parser.add_argument(
        "source_path",
        help="Source path (file path, S3 URL, or PostgreSQL connection string)",
    )

    # Optional arguments
    parser.add_argument(
        "--glob",
        "--pattern",
        dest="file_glob",
        default="*",
        help="File pattern to match (default: '*')",
    )

    parser.add_argument(
        "--format",
        dest="file_format",
        choices=["csv", "json", "jsonl", "parquet"],
        help="File format hint (auto-detected if not specified)",
    )

    parser.add_argument(
        "--tables", help="Comma-separated list of tables (PostgreSQL only)"
    )

    parser.add_argument(
        "--schema",
        default="public",
        help="Database schema (when using PostgreSQL as a source only, default: public)",
    )

    parser.add_argument(
        "--key", dest="aws_access_key_id", help="AWS Access Key ID (S3 only)"
    )

    parser.add_argument(
        "--secret", dest="aws_secret_access_key", help="AWS Secret Access Key (S3 only)"
    )

    parser.add_argument(
        "--dataset",
        dest="dataset_name",
        help="Schema in local databases where the file will be loaded",
    )

    parser.add_argument(
        "--pipeline",
        dest="pipeline_name",
        help="Custom pipeline name (default: auto-generated)",
    )

    parser.add_argument(
        "--table",
        dest="table_name",
        help="Custom table name (default: filename without extension)",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    return parser.parse_args()


def build_source_config(args) -> dict[str, Any]:
    """Build source configuration from CLI arguments."""
    config = {}

    if args.source_type == "filesystem":
        config = {
            "path": args.source_path,
            "file_glob": args.file_glob,
        }
        if args.file_format:
            config["file_format"] = args.file_format

    elif args.source_type == "s3":
        config = {
            "bucket_url": args.source_path,
            "file_glob": args.file_glob,
        }
        if args.aws_access_key_id:
            config["aws_access_key_id"] = args.aws_access_key_id
        if args.aws_secret_access_key:
            config["aws_secret_access_key"] = args.aws_secret_access_key
        if args.file_format:
            config["file_format"] = args.file_format

    elif args.source_type == "postgres":
        config = {
            "connection_string": args.source_path,
            "schema": args.schema,
        }
        if args.tables:
            config["tables"] = [t.strip() for t in args.tables.split(",")]

    return config


def print_summary(result: dict[str, Any]):
    """Print a summary of the ingestion results."""
    print("\n" + "=" * 50)
    print("🎉 INGESTION COMPLETED SUCCESSFULLY!")
    print("=" * 50)

    print(f"📊 Source Type: {result['source_type']}")
    print(f"🗂️  Pipeline: {result['pipeline_name']}")
    print(f"📁 Dataset: {result['dataset_name']}")

    # Use enhanced metadata
    metadata = result.get("metadata", {})

    if result["table_names"]:
        print(f"📋 Tables Created: {len(result['table_names'])}")
        for table in result["table_names"]:
            print(f"   • {table}")

    # Display enhanced statistics
    if metadata:
        print("\n📈 Load Statistics:")
        print(f"   • Total Tables: {metadata.get('total_tables', 0)}")
        print(f"   • Total Rows: {metadata.get('total_rows', 0):,}")
        print(f"   • Total Size: {metadata.get('total_size_mb', 0):.2f} MB")

        # Show per-table details if available
        table_details = metadata.get("table_details", [])
        if table_details:
            print("\n📊 Per-Table Breakdown:")
            for table in table_details:
                name = table.get("name", "unknown")
                rows = table.get("rows", 0)
                size_mb = table.get("size_mb", 0)
                files = table.get("files_processed", 0)
                print(f"   • {name}: {rows:,} rows, {size_mb:.2f} MB ({files} files)")

    print("\n💾 Data is now available in your PostgreSQL database!")
    print("🔗 Connection: postgresql://datalumos:datalumos123@localhost:5432/datalumos")
    print(f"📊 Dataset: {result['dataset_name']}")
    print("=" * 50)


def main():
    """Main CLI entry point."""
    try:
        args = parse_arguments()

        # Set up logging level
        if args.verbose:
            setup_logging("DEBUG")

        print("🚀 Starting Data Lumos ingestion...")
        print(f"📥 Source: {args.source_type} -> {args.source_path}")

        # Build configuration
        source_config = build_source_config(args)

        # Load the data
        result = load_data(
            source_type=args.source_type,
            source_config=source_config,
            dataset_name=args.dataset_name,
            pipeline_name=args.pipeline_name,
            table_name=args.table_name,
        )

        # Save metadata (optional, don't fail if it doesn't work)
        try:
            from .main import save_load_metadata

            save_load_metadata(result["pipeline_name"], result.get("metadata", {}))
        except Exception as e:
            logger.warning(f"Could not save metadata: {e}")

        # Print results
        print_summary(result)

    except DataLoadError as e:
        print(f"\n❌ Ingestion failed: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Ingestion interrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
