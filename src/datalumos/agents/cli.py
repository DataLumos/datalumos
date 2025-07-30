#!/usr/bin/env python3
"""CLI interface for Data Lumos QA analysis."""

import argparse
import asyncio
import sys

from datalumos.agents.flow import AgentConfig, run
from datalumos.logging import setup_logging


def parse_arguments():
    """Parse command line arguments for QA analysis."""
    parser = argparse.ArgumentParser(
        description="Run Data Lumos QA analysis on a database table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  analyze --table customers --schema public
        """,
    )

    parser.add_argument(
        "--table", dest="table_name", required=True, help="Table to analyze (required)"
    )

    parser.add_argument(
        "--schema",
        dest="schema_name",
        required=False,
        default="datalumos",
        help="Schema containing the table. If not provided, the default schema is used.",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    return parser.parse_args()


def main():
    """Main CLI entry point for QA analysis."""
    try:
        args = parse_arguments()

        if args.verbose:
            setup_logging("DEBUG")

        print("🔍 Starting Data Lumos QA analysis...")
        print(f"📋 Table: {args.schema_name}.{args.table_name}")

        config = AgentConfig.from_env()
        table_profile_results, validation_results = asyncio.run(
            run(schema=args.schema_name, table_name=args.table_name, config=config)
        )
        print(table_profile_results)
        print(validation_results)

        print("✅ Analysis completed successfully.")

    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
