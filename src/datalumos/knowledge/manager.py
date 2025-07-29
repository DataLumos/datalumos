"""
Knowledge Manager for DataLumos vector store operations.

Provides high-level interface for managing vector stores, uploading documents,
and integrating with OpenAI's File Search functionality.
"""

import os
from typing import Any
from openai import OpenAI

from datalumos.knowledge.uploader import FileUploader
from datalumos.logging import get_logger

logger = get_logger(__name__)


class KnowledgeManager:
    """
    High-level manager for DataLumos knowledge and vector store operations.
    
    Provides functionality to:
    - Create and manage vector stores
    - Upload documents from filesystem and web
    - Associate files with vector stores
    - Search and retrieve relevant knowledge
    """
    
    def __init__(self, openai_api_key: str | None = None) -> None:
        """
        Initialize the Knowledge Manager.
        
        Args:
            openai_api_key: OpenAI API key. If not provided, will use OPENAI_API_KEY env var.
        """
        api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key is required. Provide it or set OPENAI_API_KEY env var.")
            
        self.client = OpenAI(api_key=api_key)
        self.uploader = FileUploader(self.client)
        self.default_store_name = "datalumos"
        
    def create_vector_store(
        self, 
        name: str, 
        file_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None
    ) -> str:
        """
        Create a new vector store.
        
        Args:
            name: Name for the vector store
            file_ids: Optional list of file IDs to add to the vector store
            metadata: Optional metadata to associate with the vector store
            
        Returns:
            Vector store ID
        """
        logger.info(f"Creating vector store: {name}")
        
        vector_store_params = {"name": name}
        if file_ids:
            vector_store_params["file_ids"] = file_ids
        if metadata:
            vector_store_params["metadata"] = metadata
            
        vector_store = self.client.vector_stores.create(**vector_store_params)
        
        logger.info(f"Created vector store: {vector_store.id}")
        return vector_store.id
    
    def add_file_to_vector_store(self, vector_store_id: str, file_id: str) -> None:
        """
        Add a file to an existing vector store.
        
        Args:
            vector_store_id: ID of the vector store
            file_id: ID of the file to add
        """
        logger.info(f"Adding file {file_id} to vector store {vector_store_id}")
        
        self.client.vector_stores.files.create(
            vector_store_id=vector_store_id,
            file_id=file_id
        )
        
        logger.info(f"Successfully added file to vector store")
    
    def upload_and_add_document(
        self, 
        vector_store_id: str, 
        source: str,
        source_type: str = "filesystem",
        metadata: dict[str, Any] | None = None
    ) -> str:
        """
        Upload a document and add it to a vector store.
        
        Args:
            vector_store_id: ID of the vector store to add the document to
            source: File path (for filesystem) or URL (for web)
            source_type: Either "filesystem" or "web"
            metadata: Optional metadata for the file
            
        Returns:
            File ID of the uploaded document
            
        Raises:
            ValueError: If source_type is invalid
        """
        if source_type == "filesystem":
            file_id = self.uploader.upload_from_filesystem(source, metadata=metadata)
        elif source_type == "web":
            file_id = self.uploader.upload_from_web(source, metadata=metadata)
        else:
            raise ValueError(f"Invalid source_type: {source_type}. Must be 'filesystem' or 'web'")
            
        self.add_file_to_vector_store(vector_store_id, file_id)
        return file_id
    
    def create_knowledge_base(
        self, 
        name: str, 
        documents: list[dict[str, str]],
        metadata: dict[str, Any] | None = None
    ) -> str:
        """
        Create a complete knowledge base with multiple documents.
        
        Args:
            name: Name for the knowledge base (vector store)
            documents: List of document dictionaries with 'source' and 'type' keys
                      Example: [{"source": "/path/to/file.pdf", "type": "filesystem"},
                               {"source": "https://example.com/doc.pdf", "type": "web"}]
            metadata: Optional metadata for the vector store
            
        Returns:
            Vector store ID
        """
        logger.info(f"Creating knowledge base '{name}' with {len(documents)} documents")
        
        # Upload all files first
        file_ids = []
        for doc in documents:
            source = doc["source"]
            doc_type = doc.get("type", "filesystem")
            
            try:
                if doc_type == "filesystem":
                    file_id = self.uploader.upload_from_filesystem(source)
                elif doc_type == "web":
                    file_id = self.uploader.upload_from_web(source)
                else:
                    logger.warning(f"Skipping document with invalid type: {doc_type}")
                    continue
                    
                file_ids.append(file_id)
                
            except Exception as e:
                logger.error(f"Failed to upload document {source}: {e}")
                continue
        
        if not file_ids:
            raise ValueError("No documents were successfully uploaded")
            
        # Create vector store with all uploaded files
        vector_store_id = self.create_vector_store(name, file_ids, metadata)
        
        logger.info(f"Successfully created knowledge base with {len(file_ids)} documents")
        return vector_store_id
    
    def list_vector_stores(self) -> list[dict[str, Any]]:
        """
        List all vector stores.
        
        Returns:
            List of vector store information dictionaries
        """
        vector_stores = self.client.vector_stores.list()
        
        stores_info = []
        for store in vector_stores.data:
            stores_info.append({
                "id": store.id,
                "name": store.name,
                "created_at": store.created_at,
                "file_counts": store.file_counts,
                "metadata": store.metadata
            })
            
        return stores_info
    
    def get_vector_store_files(self, vector_store_id: str) -> list[dict[str, Any]]:
        """
        Get information about files in a vector store.
        
        Args:
            vector_store_id: ID of the vector store
            
        Returns:
            List of file information dictionaries
        """
        files = self.client.vector_stores.files.list(vector_store_id=vector_store_id)
        
        files_info = []
        for file in files.data:
            files_info.append({
                "id": file.id,
                "created_at": file.created_at,
                "vector_store_id": file.vector_store_id,
                "status": file.status
            })
            
        return files_info
    
    def delete_vector_store(self, vector_store_id: str) -> bool:
        """
        Delete a vector store.
        
        Args:
            vector_store_id: ID of the vector store to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            self.client.vector_stores.delete(vector_store_id)
            logger.info(f"Deleted vector store: {vector_store_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete vector store {vector_store_id}: {e}")
            return False
    
    def get_default_store_id(self) -> str | None:
        """
        Get the ID of the default DataLumos vector store.
        
        Returns:
            Vector store ID if found, None otherwise
        """
        stores = self.list_vector_stores()
        for store in stores:
            if store["name"] == self.default_store_name:
                return store["id"]
        return None
    
    def get_or_create_default_store(self) -> str:
        """
        Get or create the default DataLumos vector store.
        
        Returns:
            Vector store ID
        """
        store_id = self.get_default_store_id()
        if store_id:
            logger.info(f"Using existing default vector store: {store_id}")
            return store_id
        
        logger.info("Creating default DataLumos vector store")
        return self.create_vector_store(self.default_store_name)
    
    def delete_file(self, file_id: str) -> bool:
        """
        Delete a file from OpenAI.
        
        Args:
            file_id: ID of the file to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            self.client.files.delete(file_id)
            logger.info(f"Deleted file: {file_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file {file_id}: {e}")
            return False
    
    def get_knowledgebase_id_if_available(self) -> str | None:
        """
        Get file_search tools for agents if the datalumos vector store exists and has files.
        
        Returns:
            List containing file_search tool configuration or empty list
        """
        try:
            store_id = self.get_default_store_id()
            if not store_id:
                logger.debug("No datalumos vector store found")
                return []
            files = self.get_vector_store_files(store_id)
            if not files:
                logger.debug("Datalumos vector store has no files")
                return []
            
            logger.info(f"Knowledge base available with {len(files)} documents")
            return store_id
            
        except Exception as e:
            logger.error(f"Failed to get knowledge tools: {e}")
            return []