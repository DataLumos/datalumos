"""Centralized configuration for DataLumos with environment variable support."""

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


# Global config instance
config = Config()
