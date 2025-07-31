"""
Enhanced logging utilities with colors for subflow result reporting.
Simple, maintainable approach that doesn't depend on specific model classes.
"""

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty

from datalumos.logging import get_logger

console = Console()
logger = get_logger(__name__)


def log_step_start(step_name: str, context: str = "") -> None:
    """Log the start of a processing step."""
    context_text = f" for {context}" if context else ""
    console.print(f"\n[bold cyan]🔍 {step_name}[/bold cyan]{context_text}")


def log_step_complete(step_name: str, count: int | None = None) -> None:
    """Log completion of a processing step."""
    count_text = f" ({count} items)" if count is not None else ""
    console.print(f"[green]✅ {step_name} complete[/green]{count_text}")


def log_column_result(column_name: str, result_type: str, result: Any) -> None:
    """Log individual column result in a clean, readable format."""
    console.print(
        f"\n[bold blue]📊 Column: {column_name}[/bold blue] [dim]({result_type})[/dim]"
    )

    # Pretty print the result object with nice formatting
    console.print(
        Panel(
            Pretty(result, max_length=10, expand_all=True),
            border_style="blue",
            padding=(0, 1),
        )
    )


def log_summary(title: str, items_dict: dict) -> None:
    """Log a summary of results by type."""
    console.print(f"\n[bold magenta]📋 {title}[/bold magenta]")

    for item_type, items in items_dict.items():
        count = len(items) if isinstance(items, list) else items
        console.print(f"  [cyan]•[/cyan] {item_type}: [white]{count}[/white]")
