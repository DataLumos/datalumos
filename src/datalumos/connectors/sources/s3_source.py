"""S3 source connector for Data Lumos."""

from typing import Dict, Any, Optional, List
from dlt.sources.filesystem import filesystem
from datalumos.connectors.utils import logger, sanitize_table_name


def create_s3_source(config: Dict[str, Any], table_name: Optional[str] = None):
    """
    Create an S3 source for dlt pipeline.
    
    Args:
        config: Configuration dictionary containing:
            - bucket_url: S3 bucket URL (s3://bucket-name/path/)
            - file_glob: Optional file pattern (defaults to '*')
            - aws_access_key_id: Optional AWS access key
            - aws_secret_access_key: Optional AWS secret key
            - aws_session_token: Optional AWS session token
            - file_format: Optional file format hint ('csv', 'json', 'parquet', etc.)
    
    Returns:
        dlt source object for S3
    """
    bucket_url = config["bucket_url"]
    file_glob = config.get("file_glob", "*")
    file_format = config.get("file_format")
    
    logger.info(f"Creating S3 source for bucket: {bucket_url}")
    logger.info(f"Using file pattern: {file_glob}")
    
    # Prepare credentials if provided
    credentials = {}
    if config.get("aws_access_key_id"):
        credentials["aws_access_key_id"] = config["aws_access_key_id"]
    if config.get("aws_secret_access_key"):
        credentials["aws_secret_access_key"] = config["aws_secret_access_key"]
    if config.get("aws_session_token"):
        credentials["aws_session_token"] = config["aws_session_token"]
    
    # Create the filesystem source for S3
    source_kwargs = {
        "bucket_url": bucket_url,
        "file_glob": file_glob,
    }
    
    if credentials:
        source_kwargs["credentials"] = credentials
    
    if file_format:
        logger.info(f"Using specified file format: {file_format}")
        source_kwargs["file_format"] = file_format
    
    source = filesystem(**source_kwargs)
    
    # Add metadata for naming
    source._source_type = "s3"
    source._bucket_url = bucket_url
    
    return source


def list_s3_objects(config: Dict[str, Any], max_objects: int = 100) -> List[str]:
    """
    List objects in an S3 bucket.
    
    Args:
        config: S3 configuration dictionary
        max_objects: Maximum number of objects to list
    
    Returns:
        List of object keys
    """
    try:
        import boto3
        from urllib.parse import urlparse
        
        # Parse S3 URL
        parsed = urlparse(config["bucket_url"])
        bucket_name = parsed.netloc
        prefix = parsed.path.lstrip("/")
        
        # Create S3 client
        client_kwargs = {}
        if config.get("aws_access_key_id"):
            client_kwargs["aws_access_key_id"] = config["aws_access_key_id"]
        if config.get("aws_secret_access_key"):
            client_kwargs["aws_secret_access_key"] = config["aws_secret_access_key"]
        if config.get("aws_session_token"):
            client_kwargs["aws_session_token"] = config["aws_session_token"]
        
        s3_client = boto3.client("s3", **client_kwargs)
        
        # List objects
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=max_objects
        )
        
        objects = []
        if "Contents" in response:
            for obj in response["Contents"]:
                objects.append(obj["Key"])
        
        return objects
        
    except ImportError:
        logger.warning("boto3 not available. Cannot list S3 objects directly.")
        return []
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")
        return []


def validate_s3_access(config: Dict[str, Any]) -> bool:
    """
    Test S3 access and permissions.
    
    Args:
        config: S3 configuration dictionary
    
    Returns:
        True if access successful, False otherwise
    """
    try:
        import boto3
        from urllib.parse import urlparse
        
        # Parse S3 URL
        parsed = urlparse(config["bucket_url"])
        bucket_name = parsed.netloc
        
        # Create S3 client
        client_kwargs = {}
        if config.get("aws_access_key_id"):
            client_kwargs["aws_access_key_id"] = config["aws_access_key_id"]
        if config.get("aws_secret_access_key"):
            client_kwargs["aws_secret_access_key"] = config["aws_secret_access_key"]
        if config.get("aws_session_token"):
            client_kwargs["aws_session_token"] = config["aws_session_token"]
        
        s3_client = boto3.client("s3", **client_kwargs)
        
        # Test access by checking bucket location
        s3_client.get_bucket_location(Bucket=bucket_name)
        return True
        
    except ImportError:
        logger.warning("boto3 not available. Cannot test S3 connection.")
        return False
    except Exception as e:
        logger.error(f"S3 access test failed: {e}")
        return False


def detect_file_format(config: Dict[str, Any]) -> Optional[str]:
    """
    Attempt to detect file format from S3 objects.
    
    Args:
        config: S3 configuration dictionary
    
    Returns:
        Detected file format or None
    """
    objects = list_s3_objects(config, max_objects=10)
    
    if not objects:
        return None
    
    # Analyze file extensions
    extensions = {}
    for obj_key in objects:
        if "." in obj_key:
            ext = obj_key.split(".")[-1].lower()
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
    }
    
    return format_mapping.get(most_common_ext) 