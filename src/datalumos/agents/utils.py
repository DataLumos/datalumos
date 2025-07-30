import asyncio
import os
from agents import Agent
from typing import Any, Awaitable, Callable

PROMPT_DIR = os.path.join(os.path.dirname(__file__), 'prompts')


def load_agent_prompt(agent_name: str) -> str:
    """
    Loads the prompt for a given agent based on its name.
    The function looks for a Markdown file in the 'prompts' directory whose filename matches the agent name (case-insensitive, spaces replaced with underscores, and .md extension).
    Example: agent_name='Data Explorer' -> prompts/data_explorer.md
    """
    filename = agent_name.lower().replace(' ', '_') + '.md'
    prompt_path = os.path.join(PROMPT_DIR, filename)
    if not os.path.exists(prompt_path):
        raise FileNotFoundError(
            f"Prompt file not found for agent '{agent_name}' at {prompt_path}")
    with open(prompt_path, 'r') as f:
        return f.read()


async def run_agent_with_retries(
    fn: Callable[[Agent, str], Awaitable[Any]], 
    agent: Agent,
    question: str,
    attempts: int = 3, 
    last_error_message: str | None = None,
    base_delay: float = 1.0, 
    backoff: float = 2.0,
    raise_on_failure: bool = False
) -> Any:
    """
    Run *fn* with retry, skipping retry for cancellation / fatal errors.
    Immediately re-raises: ``asyncio.CancelledError``, ``KeyboardInterrupt`` and
    ``GuardrailViolationError`` (when available) so cooperative cancellation &
    policy violations are respected.
    
    It also adds the previous error to the question if it is provided.
    If raise_on_failure is False, logs and returns None on failure instead of raising.
    """
    import logging
    logger = logging.getLogger("datalumos.agents.utils")
    delay = base_delay
    question = f"{question}\nPrevious error: {last_error_message}" if last_error_message else question
    last_exception = None
    for i in range(1, attempts + 1):
        try:
            result = await fn(agent, question)
            return result
        except (asyncio.CancelledError, KeyboardInterrupt):  # no retry
            raise
        except Exception as e:  # noqa: BLE001
            last_error_message = str(e)
            last_exception = e
            if i == attempts:
                break
            await asyncio.sleep(delay)
            delay *= backoff
    if raise_on_failure:
        raise last_exception
    else:
        logger.error(f"Agent call failed after {attempts} attempts for question: '{question}'. Last error: {last_error_message}")
        return None
