"""
File upload functionality for DataLumos knowledge management.

Handles uploading files from filesystem and web URLs to OpenAI's File API.
"""

import os
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from openai import OpenAI

from datalumos.logging import get_logger

logger = get_logger(__name__)


class FileUploader:
    """Handles file uploads from filesystem and web sources to OpenAI File API."""

    def __init__(self, openai_client: OpenAI) -> None:
        """Initialize the file uploader with OpenAI client."""
        self.client = openai_client

    def upload_from_filesystem(
        self,
        file_path: str | Path,
        purpose: str = "assistants",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Upload a file from the filesystem to OpenAI File API.

        Args:
            file_path: Path to the file to upload
            purpose: Purpose for the file upload (default: "assistants")
            metadata: Optional metadata to associate with the file

        Returns:
            File ID from OpenAI API

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file is not supported
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if not self._is_supported_file_type(file_path):
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

        logger.info(f"Uploading file from filesystem: {file_path}")

        with open(file_path, "rb") as file:
            uploaded_file = self.client.files.create(file=file, purpose=purpose)

        logger.info(f"Successfully uploaded file: {uploaded_file.id}")
        return uploaded_file.id

    def upload_from_web(
        self,
        url: str,
        purpose: str = "assistants",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Download a file from a web URL and upload it to OpenAI File API.

        Args:
            url: URL of the file to download and upload
            purpose: Purpose for the file upload (default: "assistants")
            metadata: Optional metadata to associate with the file

        Returns:
            File ID from OpenAI API

        Raises:
            requests.RequestException: If download fails
            ValueError: If the file is not supported
        """
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL: {url}")

        logger.info(f"Downloading file from web: {url}")

        # Download the file
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # Determine file extension from URL or content-type
        file_extension = self._get_file_extension(
            url, response.headers.get("content-type")
        )

        if not self._is_supported_file_extension(file_extension):
            raise ValueError(f"Unsupported file type: {file_extension}")

        # Create temporary file
        with tempfile.NamedTemporaryFile(
            suffix=file_extension, delete=False
        ) as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
            temp_file_path = temp_file.name

        try:
            # Upload the temporary file
            with open(temp_file_path, "rb") as file:
                uploaded_file = self.client.files.create(file=file, purpose=purpose)

            logger.info(f"Successfully uploaded file from web: {uploaded_file.id}")
            return uploaded_file.id

        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)

    def _is_supported_file_type(self, file_path: Path) -> bool:
        """Check if the file type is supported by OpenAI File API."""
        return self._is_supported_file_extension(file_path.suffix.lower())

    def _is_supported_file_extension(self, extension: str) -> bool:
        """Check if the file extension is supported by OpenAI File API."""
        supported_extensions = {
            ".pdf",
            ".txt",
            ".md",
            ".doc",
            ".docx",
            ".html",
            ".json",
            ".csv",
            ".tsv",
        }
        return extension.lower() in supported_extensions

    def _get_file_extension(self, url: str, content_type: str | None) -> str:
        """Determine file extension from URL or content type."""
        # First try to get extension from URL
        parsed_url = urlparse(url)
        path = Path(parsed_url.path)
        if path.suffix:
            return path.suffix.lower()

        # Fallback to content type mapping
        content_type_map = {
            "application/pdf": ".pdf",
            "text/plain": ".txt",
            "text/markdown": ".md",
            "text/html": ".html",
            "application/json": ".json",
            "text/csv": ".csv",
            "application/msword": ".doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        }

        if content_type:
            return content_type_map.get(content_type.split(";")[0].strip(), ".txt")

        return ".txt"  # Default fallback
