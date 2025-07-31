"""Centralized configuration for DataLumos with environment variable support."""
import base64
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Centralized configuration class that loads all environment variables."""

    # OpenAI Configuration
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY")
    OPENAI_API_MODEL: str = os.getenv("OPENAI_API_MODEL", "gpt-4o")

    # PostgreSQL Configuration
    DL_DB_HOST: str = os.getenv("DL_DB_HOST", "localhost")
    DL_DB_PORT: int = int(os.getenv("DL_DB_PORT", "5432"))
    DL_DB_NAME: str = os.getenv("DL_DB_NAME", "datalumos")
    DL_DB_USER: str = os.getenv("DL_DB_USER", "datalumos")
    DL_DB_PASSWORD: str = os.getenv("DL_DB_PASSWORD", "datalumos123")
    DL_DB_SCHEMA: str = os.getenv("DL_DB_SCHEMA", "public")

    # Logging
    LOGLEVEL: str = os.getenv("LOGLEVEL", "INFO")

    # Observability
    LANGFUSE_PUBLIC_KEY: str = os.getenv("LANGFUSE_PUBLIC_KEY", "")
    LANGFUSE_SECRET_KEY: str = os.getenv("LANGFUSE_SECRET_KEY", "")
    LANGFUSE_HOST: str = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")

    LANGFUSE_AUTH = base64.b64encode(
        f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()
    ).decode()
    OTEL_EXPORTER_OTLP_ENDPOINT = f"{LANGFUSE_HOST}/api/public/otel"
    OTEL_EXPORTER_OTLP_HEADERS = f"Authorization=Basic {LANGFUSE_AUTH}"

# Global config instance
config = Config()

# Configure OpenTelemetry endpoint & headers
os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = config.OTEL_EXPORTER_OTLP_ENDPOINT
os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = config.OTEL_EXPORTER_OTLP_HEADERS

