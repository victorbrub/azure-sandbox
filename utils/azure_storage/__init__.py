"""Azure Storage Utilities

Provides utilities for Azure Blob Storage and Azure Data Lake Storage Gen2
operations including file uploads, downloads, and container management.
"""

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeDirectoryClient
from typing import List, Optional, Dict, Any, BinaryIO
import logging
import os

logger = logging.getLogger(__name__)


class AzureBlobStorageClient:
    """Client for interacting with Azure Blob Storage."""
    
    def __init__(self, account_url: str, credential: Optional[Any] = None):
        """
        Initialize Azure Blob Storage client.
        
        Args:
            account_url: Storage account URL (e.g., https://<account>.blob.core.windows.net)
            credential: Optional credential (defaults to DefaultAzureCredential)
        """
        self.account_url = account_url
        self.credential = credential or DefaultAzureCredential()
        self.service_client = BlobServiceClient(
            account_url=account_url,
            credential=self.credential
        )
        logger.info(f"Initialized Blob Storage client for: {account_url}")
    
    def create_container(self, container_name: str, public_access: Optional[str] = None) -> ContainerClient:
        """
        Create a new container.
        
        Args:
            container_name: Name of the container
            public_access: Public access level (None, 'blob', 'container')
            
        Returns:
            Container client
        """
        try:
            container_client = self.service_client.create_container(
                name=container_name,
                public_access=public_access
            )
            logger.info(f"Created container: {container_name}")
            return container_client
        except Exception as e:
            logger.error(f"Failed to create container: {e}")
            raise
    
    def delete_container(self, container_name: str) -> None:
        """
        Delete a container.
        
        Args:
            container_name: Name of the container to delete
        """
        try:
            self.service_client.delete_container(container_name)
            logger.info(f"Deleted container: {container_name}")
        except Exception as e:
            logger.error(f"Failed to delete container: {e}")
            raise
    
    def list_containers(self) -> List[str]:
        """
        List all containers in the storage account.
        
        Returns:
            List of container names
        """
        try:
            containers = [c.name for c in self.service_client.list_containers()]
            logger.info(f"Retrieved {len(containers)} containers")
            return containers
        except Exception as e:
            logger.error(f"Failed to list containers: {e}")
            raise
    
    def upload_blob(self, container_name: str, blob_name: str, 
                   data: Any, overwrite: bool = True) -> None:
        """
        Upload a blob to a container.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob
            data: Data to upload (bytes, str, or file-like object)
            overwrite: Whether to overwrite if blob exists
        """
        try:
            blob_client = self.service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            blob_client.upload_blob(data, overwrite=overwrite)
            logger.info(f"Uploaded blob: {blob_name} to container: {container_name}")
        except Exception as e:
            logger.error(f"Failed to upload blob: {e}")
            raise
    
    def upload_file(self, container_name: str, file_path: str, 
                   blob_name: Optional[str] = None) -> None:
        """
        Upload a file from local path to blob storage.
        
        Args:
            container_name: Name of the container
            file_path: Local file path
            blob_name: Name for the blob (defaults to filename)
        """
        try:
            blob_name = blob_name or os.path.basename(file_path)
            with open(file_path, "rb") as data:
                self.upload_blob(container_name, blob_name, data)
            logger.info(f"Uploaded file: {file_path} as blob: {blob_name}")
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise
    
    def download_blob(self, container_name: str, blob_name: str) -> bytes:
        """
        Download a blob's content.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob
            
        Returns:
            Blob content as bytes
        """
        try:
            blob_client = self.service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            data = blob_client.download_blob().readall()
            logger.info(f"Downloaded blob: {blob_name} from container: {container_name}")
            return data
        except Exception as e:
            logger.error(f"Failed to download blob: {e}")
            raise
    
    def download_file(self, container_name: str, blob_name: str, file_path: str) -> None:
        """
        Download a blob to a local file.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob
            file_path: Local file path to save to
        """
        try:
            data = self.download_blob(container_name, blob_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(data)
            logger.info(f"Downloaded blob: {blob_name} to file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise
    
    def delete_blob(self, container_name: str, blob_name: str) -> None:
        """
        Delete a blob.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob to delete
        """
        try:
            blob_client = self.service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            blob_client.delete_blob()
            logger.info(f"Deleted blob: {blob_name} from container: {container_name}")
        except Exception as e:
            logger.error(f"Failed to delete blob: {e}")
            raise
    
    def list_blobs(self, container_name: str, prefix: Optional[str] = None) -> List[str]:
        """
        List blobs in a container.
        
        Args:
            container_name: Name of the container
            prefix: Optional prefix filter
            
        Returns:
            List of blob names
        """
        try:
            container_client = self.service_client.get_container_client(container_name)
            blobs = [b.name for b in container_client.list_blobs(name_starts_with=prefix)]
            logger.info(f"Retrieved {len(blobs)} blobs from container: {container_name}")
            return blobs
        except Exception as e:
            logger.error(f"Failed to list blobs: {e}")
            raise


class AzureDataLakeGen2Client:
    """Client for interacting with Azure Data Lake Storage Gen2."""
    
    def __init__(self, account_url: str, credential: Optional[Any] = None):
        """
        Initialize Azure Data Lake Storage Gen2 client.
        
        Args:
            account_url: Storage account URL (e.g., https://<account>.dfs.core.windows.net)
            credential: Optional credential (defaults to DefaultAzureCredential)
        """
        self.account_url = account_url
        self.credential = credential or DefaultAzureCredential()
        self.service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=self.credential
        )
        logger.info(f"Initialized Data Lake Gen2 client for: {account_url}")
    
    def create_file_system(self, file_system_name: str) -> FileSystemClient:
        """
        Create a new file system (similar to container).
        
        Args:
            file_system_name: Name of the file system
            
        Returns:
            File system client
        """
        try:
            file_system_client = self.service_client.create_file_system(file_system_name)
            logger.info(f"Created file system: {file_system_name}")
            return file_system_client
        except Exception as e:
            logger.error(f"Failed to create file system: {e}")
            raise
    
    def delete_file_system(self, file_system_name: str) -> None:
        """
        Delete a file system.
        
        Args:
            file_system_name: Name of the file system to delete
        """
        try:
            self.service_client.delete_file_system(file_system_name)
            logger.info(f"Deleted file system: {file_system_name}")
        except Exception as e:
            logger.error(f"Failed to delete file system: {e}")
            raise
    
    def create_directory(self, file_system_name: str, directory_name: str) -> DataLakeDirectoryClient:
        """
        Create a directory in a file system.
        
        Args:
            file_system_name: Name of the file system
            directory_name: Name of the directory
            
        Returns:
            Directory client
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system_name)
            directory_client = file_system_client.create_directory(directory_name)
            logger.info(f"Created directory: {directory_name} in file system: {file_system_name}")
            return directory_client
        except Exception as e:
            logger.error(f"Failed to create directory: {e}")
            raise
    
    def upload_file(self, file_system_name: str, file_path: str, 
                   local_file_path: str, overwrite: bool = True) -> None:
        """
        Upload a file to Data Lake Gen2.
        
        Args:
            file_system_name: Name of the file system
            file_path: Path in Data Lake where file will be stored
            local_file_path: Local file path
            overwrite: Whether to overwrite if file exists
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system_name)
            file_client = file_system_client.get_file_client(file_path)
            
            with open(local_file_path, "rb") as data:
                file_client.upload_data(data, overwrite=overwrite)
            
            logger.info(f"Uploaded file: {local_file_path} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise
    
    def download_file(self, file_system_name: str, file_path: str, 
                     local_file_path: str) -> None:
        """
        Download a file from Data Lake Gen2.
        
        Args:
            file_system_name: Name of the file system
            file_path: Path in Data Lake
            local_file_path: Local file path to save to
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system_name)
            file_client = file_system_client.get_file_client(file_path)
            
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            with open(local_file_path, "wb") as f:
                download = file_client.download_file()
                f.write(download.readall())
            
            logger.info(f"Downloaded file: {file_path} to {local_file_path}")
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise
    
    def delete_file(self, file_system_name: str, file_path: str) -> None:
        """
        Delete a file from Data Lake Gen2.
        
        Args:
            file_system_name: Name of the file system
            file_path: Path to the file to delete
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system_name)
            file_client = file_system_client.get_file_client(file_path)
            file_client.delete_file()
            logger.info(f"Deleted file: {file_path} from file system: {file_system_name}")
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            raise
    
    def list_paths(self, file_system_name: str, path: Optional[str] = None) -> List[str]:
        """
        List paths in a file system.
        
        Args:
            file_system_name: Name of the file system
            path: Optional path prefix to filter
            
        Returns:
            List of path names
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system_name)
            paths = [p.name for p in file_system_client.get_paths(path=path)]
            logger.info(f"Retrieved {len(paths)} paths from file system: {file_system_name}")
            return paths
        except Exception as e:
            logger.error(f"Failed to list paths: {e}")
            raise
