from langfuse import Langfuse
import nest_asyncio
import logfire

from datalumos.config import config
from datalumos.logging import get_logger

logger = get_logger(__name__)


def setup_langfuse():
    langfuse = Langfuse(
        public_key=config.LANGFUSE_PUBLIC_KEY,
        secret_key=config.LANGFUSE_SECRET_KEY,
        host=config.LANGFUSE_HOST
    )

    # Verify connection
    try:
        langfuse.auth_check()
        logger.info("Langfuse client is authenticated and ready!")

        nest_asyncio.apply()

        # Configure logfire instrumentation.
        logfire.configure(
            service_name='DataLumos flows',
            send_to_logfire=False,
            console=False,
        )
        # This method automatically patches the OpenAI Agents SDK to send logs via OTLP to Langfuse.
        logfire.instrument_openai_agents()
    except Exception as e:
        logger.warning(
            "Langfuse authentication failed. If you wish to use Langfuse for "
            "observability, please check your credentials and host."
        )

    return langfuse
