import logging

from rich.logging import RichHandler

from datalumos.config import config


def setup_logging():
    logging.basicConfig(
        level=config.LOGLEVEL,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    # Suppress OpenAI HTTP request logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance"""
    return logging.getLogger(name)


def setup_output_logger() -> logging.Logger:
    """Set up a separate logger for formatted analysis outputs"""
    output_logger = logging.getLogger("datalumos.output")
    output_handler = logging.StreamHandler()
    output_handler.setFormatter(logging.Formatter("%(message)s"))
    output_logger.addHandler(output_handler)
    output_logger.propagate = False
    return output_logger
