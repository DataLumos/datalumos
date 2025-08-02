import logfire
import nest_asyncio
from langfuse import Langfuse

from datalumos.config import config
from datalumos.logging import get_logger

logger = get_logger(__name__)


def setup_langfuse():
    langfuse = Langfuse(
        public_key=config.LANGFUSE_PUBLIC_KEY,
        secret_key=config.LANGFUSE_SECRET_KEY,
        host=config.LANGFUSE_HOST,
    )

    # Verify connection
    try:
        langfuse.auth_check()
        logger.info("Langfuse client is authenticated and ready!")

        nest_asyncio.apply()

        # Configure logfire instrumentation.
        configure_kwargs = {
            "service_name": "DataLumos flows",
            "send_to_logfire": False,
        }

        # Only disable console logging if LOGFIRE_LOG_TO_CONSOLE is False
        if not config.LOGFIRE_LOG_TO_CONSOLE:
            configure_kwargs["console"] = False

        logfire.configure(**configure_kwargs)

        # This method automatically patches the OpenAI Agents SDK to send logs via OTLP to Langfuse.
        logfire.instrument_openai_agents()
    except Exception:
        logger.warning(
            "Langfuse authentication failed. If you wish to use Langfuse for "
            "observability, please check your credentials and host."
        )

    return langfuse
