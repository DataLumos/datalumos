"""Centralized PostgreSQL configuration for Data Lumos."""

from dataclasses import dataclass
from typing import Any

from datalumos.config import config


@dataclass
class PostgreSQLConfig:
    """Configuration for PostgreSQL connections with environment variable support."""

    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "public"

    @classmethod
    def from_env(cls) -> "PostgreSQLConfig":
        """Create configuration from environment variables with fallback to defaults."""
        return cls(
            host=config.DL_DB_HOST,
            port=config.DL_DB_PORT,
            database=config.DL_DB_NAME,
            username=config.DL_DB_USER,
            password=config.DL_DB_PASSWORD,
            schema=config.DL_DB_SCHEMA,
        )

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def connection_params(self) -> dict[str, Any]:
        """Get connection parameters as dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
        }

    def get_dlt_credentials(self) -> dict[str, Any]:
        """Get credentials in dlt format."""
        return {
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }


# Global default configuration instance
DEFAULT_POSTGRES_CONFIG = PostgreSQLConfig.from_env()
