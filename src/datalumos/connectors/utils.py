"""Utility functions for Data Lumos connectors."""

import logging
from typing import Any
from urllib.parse import urlparse


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Set up logging for the connector system."""
    logger = logging.getLogger("datalumos.connectors")

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, level.upper()))

    return logger


def validate_source_type(source_type: str) -> None:
    """Validate that the source type is supported."""
    supported_types = {"postgres", "s3", "filesystem"}
    if source_type not in supported_types:
        raise ValueError(
            f"Unsupported source type '{source_type}'. "
            f"Supported types: {', '.join(supported_types)}"
        )


def validate_postgres_config(config: dict[str, Any]) -> None:
    """Validate PostgreSQL source configuration."""
    required_fields = {"connection_string"}
    missing_fields = required_fields - set(config.keys())

    if missing_fields:
        raise ValueError(
            f"Missing required fields for PostgreSQL source: {', '.join(missing_fields)}"
        )

    # Basic connection string validation
    if not config["connection_string"].startswith(("postgresql://", "postgres://")):
        raise ValueError("Invalid PostgreSQL connection string format")


def validate_s3_config(config: dict[str, Any]) -> None:
    """Validate S3 source configuration."""
    required_fields = {"bucket_url"}
    missing_fields = required_fields - set(config.keys())

    if missing_fields:
        raise ValueError(
            f"Missing required fields for S3 source: {', '.join(missing_fields)}"
        )

    # Validate S3 URL format
    if not config["bucket_url"].startswith("s3://"):
        raise ValueError("S3 bucket_url must start with 's3://'")


def validate_filesystem_config(config: dict[str, Any]) -> None:
    """Validate filesystem source configuration."""
    required_fields = {"path"}
    missing_fields = required_fields - set(config.keys())

    if missing_fields:
        raise ValueError(
            f"Missing required fields for filesystem source: {', '.join(missing_fields)}"
        )


def sanitize_table_name(name: str) -> str:
    """Sanitize table name for PostgreSQL compatibility."""
    # Replace non-alphanumeric characters with underscores
    sanitized = "".join(c if c.isalnum() else "_" for c in name.lower())

    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = f"t_{sanitized}"

    return sanitized or "unknown_table"


def extract_source_name(config: dict[str, Any], source_type: str) -> str | None:
    """Extract a meaningful source name from configuration."""
    if source_type == "postgres":
        # Extract database name from connection string
        try:
            parsed = urlparse(config["connection_string"])
            return parsed.path.lstrip("/") if parsed.path else None
        except Exception:
            return None

    elif source_type == "s3":
        # Extract bucket name from S3 URL
        try:
            parsed = urlparse(config["bucket_url"])
            return parsed.netloc
        except Exception:
            return None

    elif source_type == "filesystem":
        # Extract directory name from path
        import os

        try:
            return os.path.basename(config["path"].rstrip("/"))
        except Exception:
            return None

    return None


# Create a default logger instance
logger = setup_logging()
