"""
Example: Azure Storage Operations

This example demonstrates how to use Azure Blob Storage and
Data Lake Storage Gen2 utilities.
"""

from utils.azure_storage import AzureBlobStorageClient, AzureDataLakeGen2Client
from utils.config import get_config_manager
from utils.logging import configure_logging, get_logger, OperationLogger
import os

# Configure logging
configure_logging(log_level="INFO")
logger = get_logger(__name__)

# Load configuration
config = get_config_manager(config_file="config.yml", env_file=".env")


def blob_storage_example():
    """Example of Azure Blob Storage operations."""
    
    logger.info("=== Azure Blob Storage Example ===")
    
    # Initialize Blob Storage client
    account_url = config.get_storage_account_url()
    blob_client = AzureBlobStorageClient(account_url=account_url)
    
    # Example 1: Create a container
    container_name = "test-container"
    logger.info(f"Creating container: {container_name}")
    try:
        blob_client.create_container(container_name)
    except Exception as e:
        logger.info(f"Container may already exist: {e}")
    
    # Example 2: Upload a file
    logger.info("\n=== Uploading files ===")
    test_file = "/tmp/test_file.txt"
    with open(test_file, "w") as f:
        f.write("Hello from Azure Sandbox!")
    
    with OperationLogger(logger, "file_upload", file=test_file):
        blob_client.upload_file(
            container_name=container_name,
            file_path=test_file,
            blob_name="test_file.txt"
        )
    
    # Example 3: List blobs
    logger.info("\n=== Listing blobs ===")
    blobs = blob_client.list_blobs(container_name)
    for blob in blobs:
        logger.info(f"Blob: {blob}")
    
    # Example 4: Download a file
    logger.info("\n=== Downloading file ===")
    download_path = "/tmp/downloaded_file.txt"
    with OperationLogger(logger, "file_download"):
        blob_client.download_file(
            container_name=container_name,
            blob_name="test_file.txt",
            file_path=download_path
        )
    
    with open(download_path, "r") as f:
        content = f.read()
        logger.info(f"Downloaded content: {content}")
    
    # Example 5: Delete a blob
    logger.info("\n=== Deleting blob ===")
    blob_client.delete_blob(container_name, "test_file.txt")
    logger.info("Blob deleted")
    
    # Cleanup
    os.remove(test_file)
    os.remove(download_path)


def data_lake_example():
    """Example of Azure Data Lake Storage Gen2 operations."""
    
    logger.info("\n\n=== Azure Data Lake Storage Gen2 Example ===")
    
    # Initialize Data Lake client
    account_url = config.get_datalake_account_url()
    datalake_client = AzureDataLakeGen2Client(account_url=account_url)
    
    # Example 1: Create a file system
    file_system_name = "test-filesystem"
    logger.info(f"Creating file system: {file_system_name}")
    try:
        datalake_client.create_file_system(file_system_name)
    except Exception as e:
        logger.info(f"File system may already exist: {e}")
    
    # Example 2: Create a directory
    logger.info("\n=== Creating directory ===")
    directory_name = "test-directory"
    datalake_client.create_directory(file_system_name, directory_name)
    
    # Example 3: Upload a file
    logger.info("\n=== Uploading file to Data Lake ===")
    test_file = "/tmp/datalake_test.txt"
    with open(test_file, "w") as f:
        f.write("Hello from Data Lake Gen2!")
    
    with OperationLogger(logger, "datalake_upload", file=test_file):
        datalake_client.upload_file(
            file_system_name=file_system_name,
            file_path=f"{directory_name}/test_file.txt",
            local_file_path=test_file
        )
    
    # Example 4: List paths
    logger.info("\n=== Listing paths ===")
    paths = datalake_client.list_paths(file_system_name)
    for path in paths:
        logger.info(f"Path: {path}")
    
    # Example 5: Download a file
    logger.info("\n=== Downloading file from Data Lake ===")
    download_path = "/tmp/datalake_downloaded.txt"
    with OperationLogger(logger, "datalake_download"):
        datalake_client.download_file(
            file_system_name=file_system_name,
            file_path=f"{directory_name}/test_file.txt",
            local_file_path=download_path
        )
    
    with open(download_path, "r") as f:
        content = f.read()
        logger.info(f"Downloaded content: {content}")
    
    # Example 6: Delete a file
    logger.info("\n=== Deleting file ===")
    datalake_client.delete_file(file_system_name, f"{directory_name}/test_file.txt")
    logger.info("File deleted")
    
    # Cleanup
    os.remove(test_file)
    os.remove(download_path)


def main():
    """Main example function."""
    blob_storage_example()
    data_lake_example()


if __name__ == "__main__":
    main()
