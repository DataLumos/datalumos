"""Configuration management for Data Lumos connectors."""

import os
from typing import Dict, Any, Optional
from datalumos.core import DEFAULT_POSTGRES_CONFIG


class ConnectorConfig:
    """Central configuration for the connector system."""

    def __init__(self):
        self.postgres = DEFAULT_POSTGRES_CONFIG

    def get_destination_config(self) -> Dict[str, Any]:
        """Get dlt destination configuration for local PostgreSQL."""
        return {
            "destination": "postgres",
            "credentials": self.postgres.get_dlt_credentials()
        }

    def get_dataset_name(self, source_type: str, source_name: Optional[str] = None) -> str:
        """Generate a dataset name based on source type and optional source name."""
        if source_name:
            return f"{source_type}_{source_name}"
        return source_type

    def get_pipeline_name(self, source_type: str, source_name: Optional[str] = None) -> str:
        """Generate a pipeline name based on source type and optional source name."""
        base_name = f"datalumos_{source_type}"
        if source_name:
            base_name += f"_{source_name}"
        return base_name


# Global configuration instance
config = ConnectorConfig()
