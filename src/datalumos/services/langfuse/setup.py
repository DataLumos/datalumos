from langfuse import get_client
import nest_asyncio
import logfire

from datalumos.logging import get_logger

logger = get_logger("langfuse")


def setup_langfuse():
    langfuse = get_client()

    # Verify connection
    try:
        langfuse.auth_check()
        logger.info("Langfuse client is authenticated and ready!")
    except Exception as e:
        logger.warning(
            f"Authentication failed. Please check your credentials and host. {e}"
        )

    nest_asyncio.apply()

    # Configure logfire instrumentation.
    logfire.configure(
        service_name='DataLumos flows',

        send_to_logfire=False,
    )
    # This method automatically patches the OpenAI Agents SDK to send logs via OTLP to Langfuse.
    logfire.instrument_openai_agents()
    return langfuse
