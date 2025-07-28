import asyncio
import os
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
    fn: Callable[[], Awaitable[Any]], *, attempts: int = 3, base_delay: float = 1.0, backoff: float = 2.0
) -> Any:
    """Run *fn* with retry, skipping retry for cancellation / fatal errors.
    Immediately re-raises: ``asyncio.CancelledError``, ``KeyboardInterrupt`` and
    ``GuardrailViolationError`` (when available) so cooperative cancellation &
    policy violations are respected.
    """
    delay = base_delay
    last_exc: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            result = await fn()
            return result
        except (asyncio.CancelledError, KeyboardInterrupt):  # no retry
            raise
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if i == attempts:
                break
            await asyncio.sleep(delay)
            delay *= backoff
    assert last_exc is not None
    raise
