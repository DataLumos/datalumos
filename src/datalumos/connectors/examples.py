"""Usage examples for Data Lumos connectors."""

from datalumos.connectors import load_data


def example_postgres_load():
    """Example: Load data from PostgreSQL database."""
    try:
        result = load_data(
            source_type="postgres",
            source_config={
                "connection_string": "postgresql://user:password@localhost:5432/source_db",
                "tables": ["customers", "orders"],  # Optional: specify tables
                "schema": "public",  # Optional: specify schema
            },
        )

        print("✅ Successfully loaded from PostgreSQL!")
        print(f"   Pipeline: {result['pipeline_name']}")
        print(f"   Dataset: {result['dataset_name']}")
        print(f"   Tables: {', '.join(result['table_names'])}")

        return result

    except Exception as e:
        print(f"❌ PostgreSQL load failed: {e}")
        return None


def example_s3_load():
    """Example: Load data from S3 bucket."""
    try:
        result = load_data(
            source_type="s3",
            source_config={
                "bucket_url": "s3://my-data-bucket/csv-files/",
                "file_glob": "*.csv",
                "aws_access_key_id": "your_access_key",  # Optional if using IAM
                "aws_secret_access_key": "your_secret_key",  # Optional if using IAM
            },
        )

        print("✅ Successfully loaded from S3!")
        print(f"   Pipeline: {result['pipeline_name']}")
        print(f"   Dataset: {result['dataset_name']}")
        print(f"   Tables: {', '.join(result['table_names'])}")

        return result

    except Exception as e:
        print(f"❌ S3 load failed: {e}")
        return None


def example_filesystem_load():
    """Example: Load data from local filesystem."""
    try:
        result = load_data(
            source_type="filesystem",
            source_config={
                "path": "/path/to/data/files",
                "file_glob": "*.csv",
                "recursive": True,  # Optional: search subdirectories
            },
        )

        print("✅ Successfully loaded from filesystem!")
        print(f"   Pipeline: {result['pipeline_name']}")
        print(f"   Dataset: {result['dataset_name']}")
        print(f"   Tables: {', '.join(result['table_names'])}")

        return result

    except Exception as e:
        print(f"❌ Filesystem load failed: {e}")
        return None


def example_custom_names():
    """Example: Load data with custom pipeline and dataset names."""
    try:
        result = load_data(
            source_type="filesystem",
            source_config={"path": "/path/to/sales/data", "file_glob": "sales_*.csv"},
            dataset_name="sales_analytics",
            pipeline_name="monthly_sales_import",
        )

        print("✅ Successfully loaded with custom names!")
        print(f"   Pipeline: {result['pipeline_name']}")
        print(f"   Dataset: {result['dataset_name']}")

        return result

    except Exception as e:
        print(f"❌ Custom load failed: {e}")
        return None


def example_check_pipeline_info():
    """Example: Check information about an existing pipeline."""
    from datalumos.connectors import get_pipeline_info

    info = get_pipeline_info("datalumos_postgres_mydb")

    if info:
        print("📊 Pipeline Info:")
        print(f"   Name: {info['name']}")
        print(f"   Destination: {info['destination']}")
        print(f"   Dataset: {info['dataset_name']}")
        print(f"   Last Load: {info['last_load_info']}")
    else:
        print("❓ Pipeline not found or no previous loads")


if __name__ == "__main__":
    print("🚀 Data Lumos Connector Examples")
    print("=" * 40)

    # Run examples (commented out to avoid actual execution)
    # Uncomment the ones you want to test:

    # example_postgres_load()
    # example_s3_load()
    # example_filesystem_load()
    # example_custom_names()
    # example_check_pipeline_info()

    print("\n💡 Tip: Uncomment the examples you want to run!")
    print("📚 Make sure to update the connection details for your data sources.")
