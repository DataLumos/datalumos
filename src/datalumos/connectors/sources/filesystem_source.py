"""Filesystem source connector for Data Lumos."""

import os
import json
import csv
import dlt
from typing import Dict, Any, Optional, List
from pathlib import Path
from dlt.sources.filesystem import filesystem
from datalumos.connectors.utils import logger, sanitize_table_name


def create_filesystem_source(config: Dict[str, Any], table_name: Optional[str] = None):
    """
    Create a filesystem source for dlt pipeline.

    Args:
        config: Configuration dictionary containing:
            - path: Local filesystem path
            - file_glob: Optional file pattern (defaults to '*')
            - file_format: Optional file format hint ('csv', 'json', 'parquet', etc.)
            - recursive: Optional boolean to search recursively (defaults to True)
            - table_name: Optional custom table name (defaults to auto-generated)

    Returns:
        dlt source object for filesystem
    """
    path = config["path"]
    file_glob = config.get("file_glob", "*")
    file_format = config.get("file_format")
    recursive = config.get("recursive", True)

    logger.info(f"Creating filesystem source for path: {path}")
    logger.info(f"Using file pattern: {file_glob}")
    logger.info(f"Recursive search: {recursive}")

    # Ensure path exists
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")

    # Find matching files
    matching_files = _find_matching_files(path, file_glob, recursive)

    logger.info(f"Found {len(matching_files)} matching files")

    if not matching_files:
        logger.warning("No matching files found")

        @dlt.resource(table_name="no_files_found")
        def empty_source():
            yield {"message": "No files matched the pattern", "pattern": file_glob, "path": path}

        return empty_source

    # Detect format from first file if not specified
    if not file_format and matching_files:
        file_format = _detect_format_from_extension(matching_files[0])
        logger.info(f"Auto-detected file format: {file_format}")

    # Determine table name
    if table_name:
        final_table_name = sanitize_table_name(table_name)
        logger.info(f"Using user-defined table name: {final_table_name}")
    else:
        # Use first file's name without extension as default
        first_file_name = Path(matching_files[0]).stem if matching_files else "data"
        final_table_name = sanitize_table_name(first_file_name)
        logger.info(f"Using filename-based table name: {final_table_name}")

    @dlt.resource(table_name=final_table_name)
    def load_file_content():
        """Load actual content from files."""
        total_records = 0
        skipped_records = 0

        for file_path in matching_files:
            logger.info(f"Processing file: {file_path}")

            try:
                file_records = 0
                file_skipped = 0

                if file_format == 'json':
                    for record in _process_json_file(file_path):
                        file_records += 1
                        yield record
                elif file_format == 'jsonl':
                    for record in _process_jsonl_file(file_path):
                        file_records += 1
                        yield record
                elif file_format == 'csv':
                    for record in _process_csv_file(file_path):
                        file_records += 1
                        yield record
                else:
                    logger.warning(f"Unsupported file format: {file_format}")
                    continue

                total_records += file_records
                logger.info(
                    f"Completed {file_path}: {file_records} records processed")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue

        logger.info(
            f"Processing complete: {total_records} total records processed")

    return load_file_content


def list_files(config: Dict[str, Any], max_files: int = 100) -> List[str]:
    """
    List files in a filesystem path.

    Args:
        config: Filesystem configuration dictionary
        max_files: Maximum number of files to list

    Returns:
        List of file paths
    """
    try:
        path = Path(config["path"])
        file_glob = config.get("file_glob", "*")
        recursive = config.get("recursive", True)

        if not path.exists():
            logger.error(f"Path does not exist: {path}")
            return []

        files = []
        if recursive:
            # Search recursively
            for file_path in path.rglob(file_glob):
                if file_path.is_file():
                    files.append(str(file_path))
                    if len(files) >= max_files:
                        break
        else:
            # Search only in current directory
            for file_path in path.glob(file_glob):
                if file_path.is_file():
                    files.append(str(file_path))
                    if len(files) >= max_files:
                        break

        return files

    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return []


def validate_filesystem_access(config: Dict[str, Any]) -> bool:
    """
    Test filesystem access and permissions.

    Args:
        config: Filesystem configuration dictionary

    Returns:
        True if access successful, False otherwise
    """
    try:
        path = Path(config["path"])

        # Check if path exists
        if not path.exists():
            logger.error(f"Path does not exist: {path}")
            return False

        # Check if path is readable
        if not os.access(path, os.R_OK):
            logger.error(f"Path is not readable: {path}")
            return False

        # Try to list contents
        if path.is_dir():
            list(path.iterdir())
        elif path.is_file():
            # If it's a file, check if we can read it
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                f.read(1)  # Read just one character to test

        return True

    except Exception as e:
        logger.error(f"Filesystem access test failed: {e}")
        return False


def detect_file_format(config: Dict[str, Any]) -> Optional[str]:
    """
    Attempt to detect file format from filesystem files.

    Args:
        config: Filesystem configuration dictionary

    Returns:
        Detected file format or None
    """
    files = list_files(config, max_files=10)

    if not files:
        return None

    # Analyze file extensions
    extensions = {}
    for file_path in files:
        path = Path(file_path)
        if path.suffix:
            ext = path.suffix.lstrip('.').lower()
            extensions[ext] = extensions.get(ext, 0) + 1

    if not extensions:
        return None

    # Return the most common extension
    most_common_ext = max(extensions, key=extensions.get)

    # Map to known formats
    format_mapping = {
        "csv": "csv",
        "json": "json",
        "jsonl": "jsonl",
        "parquet": "parquet",
        "txt": "csv",  # Assume CSV for txt files
        "tsv": "csv",  # Tab-separated values
    }

    return format_mapping.get(most_common_ext)


def get_file_stats(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get statistics about files in the filesystem path.

    Args:
        config: Filesystem configuration dictionary

    Returns:
        Dictionary with file statistics
    """
    try:
        path = Path(config["path"])
        file_glob = config.get("file_glob", "*")
        recursive = config.get("recursive", True)

        if not path.exists():
            return {"error": f"Path does not exist: {path}"}

        stats = {
            "total_files": 0,
            "total_size_bytes": 0,
            "file_types": {},
            "largest_file": None,
            "largest_file_size": 0,
        }

        files = list_files(config, max_files=1000)  # Get more files for stats

        for file_path in files:
            file_obj = Path(file_path)
            if file_obj.exists() and file_obj.is_file():
                stats["total_files"] += 1

                # File size
                size = file_obj.stat().st_size
                stats["total_size_bytes"] += size

                # Track largest file
                if size > stats["largest_file_size"]:
                    stats["largest_file_size"] = size
                    stats["largest_file"] = str(file_obj)

                # File type
                ext = file_obj.suffix.lstrip('.').lower(
                ) if file_obj.suffix else "no_extension"
                stats["file_types"][ext] = stats["file_types"].get(ext, 0) + 1

        # Convert bytes to human-readable format
        stats["total_size_mb"] = round(
            stats["total_size_bytes"] / (1024 * 1024), 2)
        stats["largest_file_size_mb"] = round(
            stats["largest_file_size"] / (1024 * 1024), 2)

        return stats

    except Exception as e:
        return {"error": f"Error getting file stats: {e}"}


# Private helper functions

def _find_matching_files(path: str, file_glob: str, recursive: bool) -> List[Path]:
    """Find files matching the pattern."""
    base_path = Path(path)
    matching_files = []

    if recursive:
        matching_files = list(base_path.rglob(file_glob))
    else:
        matching_files = list(base_path.glob(file_glob))

    # Filter to only files
    return [f for f in matching_files if f.is_file()]


def _detect_format_from_extension(file_path: Path) -> str | None:
    """Detect file format from extension."""
    ext = file_path.suffix.lower().lstrip('.')
    format_map = {
        'json': 'json',
        'jsonl': 'jsonl',
        'csv': 'csv',
        'tsv': 'csv',
        'txt': 'csv'
    }
    if ext in format_map:
        return format_map[ext]
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _process_json_file(file_path: Path):
    """Process a JSON file and yield records."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

        # Handle different JSON structures
        if isinstance(data, list):
            # JSON array - yield each item
            for i, item in enumerate(data):
                try:
                    if isinstance(item, dict):
                        sanitized_item = _sanitize_data(item)
                        sanitized_item['_file_source'] = str(file_path)
                        sanitized_item['_record_index'] = i
                        yield sanitized_item
                    else:
                        yield {
                            'value': _sanitize_data(item),
                            '_file_source': str(file_path),
                            '_record_index': i
                        }
                except Exception as e:
                    logger.warning(f"Skipped record {i} in {file_path}: {e}")
                    continue
        elif isinstance(data, dict):
            # Single JSON object
            try:
                sanitized_data = _sanitize_data(data)
                sanitized_data['_file_source'] = str(file_path)
                yield sanitized_data
            except Exception as e:
                logger.warning(f"Skipped single object in {file_path}: {e}")
        else:
            # Primitive value
            try:
                yield {
                    'value': _sanitize_data(data),
                    '_file_source': str(file_path)
                }
            except Exception as e:
                logger.warning(f"Skipped primitive value in {file_path}: {e}")


def _process_jsonl_file(file_path: Path):
    """Process a JSONL file and yield records."""
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            line = line.strip()
            if line:
                try:
                    data = json.loads(line)
                    sanitized_data = _sanitize_data(data)
                    if isinstance(sanitized_data, dict):
                        sanitized_data['_file_source'] = str(file_path)
                        sanitized_data['_line_number'] = line_num + 1
                    yield sanitized_data
                except Exception as e:
                    logger.warning(
                        f"Skipped line {line_num + 1} in {file_path}: {e}")
                    continue


def _process_csv_file(file_path: Path):
    """Process a CSV file and yield records."""
    with open(file_path, 'r', encoding='utf-8') as f:
        # Try to detect delimiter
        sample = f.read(1024)
        f.seek(0)

        delimiter = ','
        if '\t' in sample and sample.count('\t') > sample.count(','):
            delimiter = '\t'

        reader = csv.DictReader(f, delimiter=delimiter)
        for row_num, row in enumerate(reader):
            try:
                sanitized_row = _sanitize_data(dict(row))
                sanitized_row['_file_source'] = str(file_path)
                sanitized_row['_row_number'] = row_num + 1
                yield sanitized_row
            except Exception as e:
                logger.warning(
                    f"Skipped row {row_num + 1} in {file_path}: {e}")
                continue


def _sanitize_data(data):
    """Sanitize data to prevent SQL injection and encoding issues."""
    if isinstance(data, dict):
        return {_sanitize_key(key): _sanitize_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_sanitize_data(item) for item in data]
    elif isinstance(data, str):
        return _sanitize_string(data)
    else:
        return data


def _sanitize_key(key):
    """Sanitize dictionary keys to be SQL-safe."""
    if isinstance(key, str):
        # Replace spaces and special characters in column names
        sanitized = ''.join(c if c.isalnum() else '_' for c in key.lower())
        # Ensure it starts with a letter
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
        return sanitized or 'unknown_column'
    return str(key)


def _sanitize_string(data: str) -> str:
    """Clean up problematic characters that cause SQL issues."""
    cleaned = data

    # Remove null bytes and control characters
    cleaned = ''.join(char for char in cleaned if ord(char)
                      >= 32 or char in '\n\t')

    # Replace problematic escape sequences
    cleaned = cleaned.replace('\x00', '')  # Null bytes
    cleaned = cleaned.replace('\\', '/')   # Backslashes cause escape issues
    cleaned = cleaned.replace("'", "''")   # Escape single quotes for SQL
    cleaned = cleaned.replace('"', '""')   # Escape double quotes
    cleaned = cleaned.replace('\r\n', '\n').replace(
        '\r', '\n')  # Normalize line endings

    # Remove any remaining problematic characters
    cleaned = cleaned.replace('\b', '').replace('\f', '').replace('\v', '')

    # Limit very long strings to prevent memory issues
    if len(cleaned) > 16000:  # Be more conservative with size
        cleaned = cleaned[:16000] + "... [TRUNCATED]"

    return cleaned
