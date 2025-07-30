"""Data Lumos Connectors - Simple data loading with dlt."""

from datalumos.connectors.config import config
from datalumos.connectors.main import DataLoadError, get_pipeline_info, load_data

__all__ = ["DataLoadError", "config", "get_pipeline_info", "load_data"]
