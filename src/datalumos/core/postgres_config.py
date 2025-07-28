"""Centralized PostgreSQL configuration for Data Lumos."""

import os
from typing import Dict, Any
from dataclasses import dataclass


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
    def from_env(cls, prefix: str = "POSTGRES") -> "PostgreSQLConfig":
        """Create configuration from environment variables with fallback to defaults."""
        return cls(
            host=os.getenv(f"{prefix}_HOST", os.getenv("DL_DB_HOST", "localhost")),
            port=int(os.getenv(f"{prefix}_PORT", os.getenv("DL_DB_PORT", "5432"))),
            database=os.getenv(f"{prefix}_DB", os.getenv("DL_DB_NAME", "datalumos")),
            username=os.getenv(f"{prefix}_USER", os.getenv("DL_DB_USER", "datalumos")),
            password=os.getenv(f"{prefix}_PASSWORD", os.getenv("DL_DB_PASSWORD", "datalumos123")),
            schema=os.getenv(f"{prefix}_SCHEMA", "public")
        )

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password
        }

    def get_dlt_credentials(self) -> Dict[str, Any]:
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