"""Centralized PostgreSQL configuration for Data Lumos."""

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from datalumos.config import config


@dataclass
class PostgreSQLConfig:
    """Configuration for PostgreSQL connections with environment variable support."""

    host: str
    port: int | None
    database: str
    username: str
    password: str
    schema: str = "public"
    sslmode: str = ""
    channel_binding: str = ""

    @classmethod
    def from_env(cls) -> "PostgreSQLConfig":
        """Create configuration from environment variables with fallback to defaults."""
        return cls(
            host=config.DL_DB_HOST,
            port=cls._parse_port(config.DL_DB_PORT),
            database=config.DL_DB_NAME,
            username=config.DL_DB_USER,
            password=config.DL_DB_PASSWORD,
            schema=config.DL_DB_SCHEMA,
            sslmode=config.DL_DB_SSLMODE,
            channel_binding=config.DL_DB_CHANNELBINDING,
        )

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string with optional port and SSL parameters."""
        base_url = self._get_base_url()
        query_params = self._get_connection_query_params()

        if query_params:
            query_string = urlencode(query_params)
            return f"{base_url}?{query_string}"

        return base_url

    @property
    def connection_params(self) -> dict[str, Any]:
        """Get connection parameters as dictionary."""
        params = self._get_base_connection_params()
        self._add_optional_params(params)
        return params

    def get_dlt_credentials(self) -> str | dict[str, Any]:
        """Get credentials in dlt format."""
        # For pooled connections, use connection string to avoid search_path issues
        if self._requires_connection_string():
            return self.connection_string

        # For standard connections, use individual parameters
        credentials = self._get_base_connection_params()
        self._add_optional_params(credentials)
        return credentials

    @staticmethod
    def _parse_port(port_str: str) -> int | None:
        """Parse port string to integer, returning None if empty or invalid."""
        if not port_str:
            return None
        try:
            return int(port_str)
        except ValueError:
            return None

    def _get_base_url(self) -> str:
        """Generate base PostgreSQL URL with optional port."""
        if self.port:
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql://{self.username}:{self.password}@{self.host}/{self.database}"

    def _get_connection_query_params(self) -> dict[str, str]:
        """Get connection query parameters for SSL settings."""
        params = {}
        if self.sslmode:
            params["sslmode"] = self.sslmode
        if self.channel_binding:
            params["channel_binding"] = self.channel_binding
        return params

    def _get_base_connection_params(self) -> dict[str, Any]:
        """Get base connection parameters."""
        return {
            "host": self.host,
            "database": self.database,
            "user": self.username,
            "password": self.password,
        }

    def _add_optional_params(self, params: dict[str, Any]) -> None:
        """Add optional parameters (port, SSL) to params dictionary."""
        if self.port:
            params["port"] = self.port
        if self.sslmode:
            params["sslmode"] = self.sslmode
        if self.channel_binding:
            params["channel_binding"] = self.channel_binding

    def _requires_connection_string(self) -> bool:
        """Check if this connection requires connection string format for compatibility."""
        # Pooled connections (like Neon, PgBouncer, etc.) often have issues with search_path
        pooled_indicators = ["neon.tech", "pooler", "pgbouncer", "pool"]
        return any(indicator in self.host.lower() for indicator in pooled_indicators)


# Global default configuration instance
DEFAULT_POSTGRES_CONFIG = PostgreSQLConfig.from_env()
