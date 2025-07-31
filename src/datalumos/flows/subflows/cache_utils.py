"""
Shared caching utilities for subflow operations.

Provides reusable caching functionality for table profiling, accuracy validation,
and validity checking operations.
"""

from pathlib import Path
from typing import Generic, TypeVar

from pydantic import BaseModel

from datalumos.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


class CacheManager(Generic[T]):
    """Generic cache manager for Pydantic models."""

    def __init__(self, cache_subdir: str, cache_suffix: str, result_type: type[T]):
        """
        Initialize cache manager.

        Args:
            cache_subdir: Subdirectory name under ~/.datalumos/
            cache_suffix: File suffix for cache files (e.g., 'analysis', 'accuracy')
            result_type: Pydantic model type for validation
        """
        self.cache_dir = Path.home() / ".datalumos" / cache_subdir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_suffix = cache_suffix
        self.result_type = result_type

    def _cache_file_path(self, schema: str, table_name: str) -> Path:
        """Get cache file path for results."""
        return self.cache_dir / f"{schema}.{table_name}.{self.cache_suffix}.json"

    def load_cached_results(self, schema: str, table_name: str) -> T | None:
        """Load cached results if they exist."""
        cache_file = self._cache_file_path(schema, table_name)
        if cache_file.exists():
            try:
                return self.result_type.model_validate_json(cache_file.read_text())
            except Exception as e:
                logger.warning(f"Failed to load cached {self.cache_suffix} results: {e}")
        return None

    def save_cached_results(self, schema: str, table_name: str, results: T) -> None:
        """Save results to cache."""
        cache_file = self._cache_file_path(schema, table_name)
        try:
            cache_file.write_text(results.model_dump_json(indent=2))
            logger.info(f"{self.cache_suffix.title()} results cached to {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to cache {self.cache_suffix} results: {e}")
