"""
Tools utilities for DataLumos agents.
"""

from typing import Any
from datalumos.knowledge.manager import KnowledgeManager
from datalumos.logging import get_logger
from agents.tool import FileSearchTool

logger = get_logger(__name__)


def get_file_search_tool(openai_api_key: str | None = None) -> list[FileSearchTool | None]:
    """
    Get file_search tools for agents if the datalumos vector store exists and has files.
    
    Args:
        openai_api_key: OpenAI API key. If not provided, uses OPENAI_API_KEY env var.
        
    Returns:
        List containing file_search tool configuration or empty list
    """
    try:
        manager = KnowledgeManager(openai_api_key=openai_api_key)
        vector_store_id = manager.get_knowledgebase_id_if_available()
        if vector_store_id:
            return [FileSearchTool(vector_store_ids=[vector_store_id])]
        else:
            return []
    except Exception as e:
        logger.error(f"Failed to initialize knowledge manager: {e}")
        return []