"""
Simple CLI interface for DataLumos knowledge management.
"""

import sys

import typer
from rich.console import Console
from rich.table import Table

from datalumos.knowledge.manager import KnowledgeManager
from datalumos.logging import setup_logging, get_logger

app = typer.Typer(
    name="datalumos-knowledge",
    help="DataLumos Knowledge Management - Add, list, and delete documents"
)
console = Console()
logger = get_logger(__name__)


@app.command()
def add(
    source: str = typer.Argument(..., help="File path or URL"),
    source_type: str = typer.Option("filesystem", "--type", "-t", help="Source type: filesystem or web")
) -> None:
    """Add a document to the DataLumos vector store."""
    try:
        setup_logging()
        manager = KnowledgeManager()
        
        # Get or create the default vector store
        vector_store_id = manager.get_or_create_default_store()
        
        console.print(f"Adding document: {source}")
        
        file_id = manager.upload_and_add_document(
            vector_store_id=vector_store_id,
            source=source,
            source_type=source_type
        )
        
        console.print(f"✓ Document added successfully! File ID: {file_id}")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@app.command()
def list() -> None:
    """List all documents in the DataLumos vector store."""
    try:
        setup_logging()
        manager = KnowledgeManager()
        
        vector_store_id = manager.get_default_store_id()
        if not vector_store_id:
            console.print("No DataLumos vector store found. Add a document first.")
            return
        
        files = manager.get_vector_store_files(vector_store_id)
        
        if not files:
            console.print("No documents found in DataLumos vector store.")
            return
        
        table = Table(title="DataLumos Knowledge Base Documents")
        table.add_column("File ID", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Created", style="blue")
        
        for file in files:
            table.add_row(
                file["id"],
                file["status"],
                str(file["created_at"])
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@app.command()
def delete(
    file_id: str = typer.Argument(..., help="File ID to delete"),
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt")
) -> None:
    """Delete a document from the DataLumos vector store."""
    try:
        setup_logging()
        
        if not confirm:
            confirm_delete = typer.confirm(f"Are you sure you want to delete file {file_id}?")
            if not confirm_delete:
                console.print("Operation cancelled.")
                return
        
        manager = KnowledgeManager()
        success = manager.delete_file(file_id)
        
        if success:
            console.print(f"✓ Document deleted successfully! File ID: {file_id}")
        else:
            console.print(f"[red]Failed to delete document {file_id}[/red]")
            sys.exit(1)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@app.command()
def delete_store(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt")
) -> None:
    """Delete the entire default DataLumos vector store and all its documents."""
    try:
        setup_logging()
        manager = KnowledgeManager()
        vector_store_id = manager.get_default_store_id()
        if not vector_store_id:
            console.print("No DataLumos vector store found to delete.")
            return
        if not confirm:
            confirm_delete = typer.confirm(
                f"Are you sure you want to delete the entire DataLumos vector store (ID: {vector_store_id})? This will remove all documents and cannot be undone."
            )
            if not confirm_delete:
                console.print("Operation cancelled.")
                return
        success = manager.delete_vector_store(vector_store_id)
        if success:
            console.print(f"✓ Vector store deleted successfully! Store ID: {vector_store_id}")
        else:
            console.print(f"[red]Failed to delete vector store {vector_store_id}[/red]")
            sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()