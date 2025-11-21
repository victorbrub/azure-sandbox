"""
Complete Example: End-to-End Data Pipeline

This example demonstrates an end-to-end data engineering pipeline
using multiple Azure services together with PowerBI integration.
"""

from utils.azure_data_factory import AzureDataFactoryClient
from utils.azure_databricks import AzureDatabricksClient
from utils.azure_storage import AzureBlobStorageClient, AzureDataLakeGen2Client
from utils.azure_sql import AzureSQLClient
from utils.powerbi import PowerBIClient
from utils.config import get_config_manager
from utils.logging import configure_logging, get_logger, OperationLogger
import time
from datetime import datetime

# Configure logging
configure_logging(log_level="INFO", log_file="logs/pipeline.log")
logger = get_logger(__name__)

# Load configuration
config = get_config_manager(config_file="config.yml", env_file=".env")


class DataPipeline:
    """End-to-end data pipeline orchestrator."""
    
    def __init__(self):
        """Initialize all Azure service clients."""
        logger.info("Initializing data pipeline...")
        
        # Initialize clients (add error handling for missing configs)
        try:
            self.init_adf_client()
        except Exception as e:
            logger.warning(f"ADF client not initialized: {e}")
            self.adf_client = None
        
        try:
            self.init_databricks_client()
        except Exception as e:
            logger.warning(f"Databricks client not initialized: {e}")
            self.databricks_client = None
        
        try:
            self.init_storage_clients()
        except Exception as e:
            logger.warning(f"Storage clients not initialized: {e}")
            self.blob_client = None
            self.datalake_client = None
        
        try:
            self.init_sql_client()
        except Exception as e:
            logger.warning(f"SQL client not initialized: {e}")
            self.sql_client = None
        
        try:
            self.init_powerbi_client()
        except Exception as e:
            logger.warning(f"PowerBI client not initialized: {e}")
            self.powerbi_client = None
    
    def init_adf_client(self):
        """Initialize Azure Data Factory client."""
        self.adf_client = AzureDataFactoryClient(
            subscription_id=config.get("azure.subscription_id", required=True),
            resource_group=config.get("azure.data_factory.resource_group", required=True),
            factory_name=config.get("azure.data_factory.factory_name", required=True)
        )
        logger.info("ADF client initialized")
    
    def init_databricks_client(self):
        """Initialize Databricks client."""
        self.databricks_client = AzureDatabricksClient(
            workspace_url=config.get("azure.databricks.workspace_url", required=True),
            token=config.get("azure.databricks.token")
        )
        logger.info("Databricks client initialized")
    
    def init_storage_clients(self):
        """Initialize storage clients."""
        self.blob_client = AzureBlobStorageClient(
            account_url=config.get_storage_account_url()
        )
        self.datalake_client = AzureDataLakeGen2Client(
            account_url=config.get_datalake_account_url()
        )
        logger.info("Storage clients initialized")
    
    def init_sql_client(self):
        """Initialize SQL Database client."""
        sql_params = config.get_sql_connection_params()
        self.sql_client = AzureSQLClient(
            server=sql_params["server"],
            database=sql_params["database"],
            username=sql_params.get("username"),
            password=sql_params.get("password"),
            use_azure_ad=not sql_params.get("username")
        )
        logger.info("SQL Database client initialized")
    
    def init_powerbi_client(self):
        """Initialize PowerBI client."""
        creds = config.get_powerbi_credentials()
        self.powerbi_client = PowerBIClient(
            client_id=creds["client_id"],
            client_secret=creds.get("client_secret"),
            tenant_id=creds.get("tenant_id"),
            username=creds.get("username"),
            password=creds.get("password")
        )
        logger.info("PowerBI client initialized")
    
    def run_ingestion_pipeline(self, pipeline_name: str, date: str) -> str:
        """
        Run data ingestion pipeline in Azure Data Factory.
        
        Args:
            pipeline_name: Name of the ADF pipeline
            date: Date parameter for the pipeline
            
        Returns:
            Pipeline run ID
        """
        if not self.adf_client:
            logger.warning("ADF client not available, skipping ingestion")
            return None
        
        with OperationLogger(logger, "adf_ingestion", pipeline=pipeline_name, date=date):
            run_id = self.adf_client.create_pipeline_run(
                pipeline_name=pipeline_name,
                parameters={"date": date}
            )
            
            # Monitor pipeline
            while True:
                run_info = self.adf_client.get_pipeline_run(run_id)
                status = run_info.status
                
                if status in ["Succeeded", "Failed", "Cancelled"]:
                    break
                
                time.sleep(10)
            
            if status == "Succeeded":
                logger.info(f"Ingestion pipeline completed successfully")
            else:
                raise Exception(f"Ingestion pipeline failed: {status}")
            
            return run_id
    
    def process_with_databricks(self, notebook_path: str, cluster_id: str, 
                               parameters: dict) -> dict:
        """
        Process data using Databricks notebook.
        
        Args:
            notebook_path: Path to the processing notebook
            cluster_id: Cluster ID to run on
            parameters: Notebook parameters
            
        Returns:
            Execution results
        """
        if not self.databricks_client:
            logger.warning("Databricks client not available, skipping processing")
            return None
        
        with OperationLogger(logger, "databricks_processing", notebook=notebook_path):
            result = self.databricks_client.execute_notebook(
                notebook_path=notebook_path,
                cluster_id=cluster_id,
                parameters=parameters,
                timeout_seconds=1800  # 30 minutes
            )
            
            if result["status"] == "SUCCESS":
                logger.info("Data processing completed successfully")
            else:
                raise Exception(f"Processing failed: {result['status']}")
            
            return result
    
    def load_to_sql(self, data: list, table_name: str):
        """
        Load processed data to Azure SQL Database.
        
        Args:
            data: List of dictionaries representing rows
            table_name: Target table name
        """
        if not self.sql_client:
            logger.warning("SQL client not available, skipping load")
            return
        
        with OperationLogger(logger, "sql_load", table=table_name, rows=len(data)):
            self.sql_client.bulk_insert(table_name, data)
            logger.info(f"Loaded {len(data)} rows to {table_name}")
    
    def refresh_powerbi_dataset(self, dataset_id: str):
        """
        Refresh PowerBI dataset after data load.
        
        Args:
            dataset_id: PowerBI dataset ID
        """
        if not self.powerbi_client:
            logger.warning("PowerBI client not available, skipping refresh")
            return
        
        with OperationLogger(logger, "powerbi_refresh", dataset=dataset_id):
            self.powerbi_client.refresh_dataset(
                dataset_id=dataset_id,
                notify_option="MailOnFailure"
            )
            logger.info("PowerBI dataset refresh triggered")
    
    def run_complete_pipeline(self):
        """Run the complete end-to-end pipeline."""
        logger.info("=" * 80)
        logger.info("Starting complete data pipeline")
        logger.info("=" * 80)
        
        try:
            # Step 1: Data Ingestion via ADF
            logger.info("\n[Step 1] Running data ingestion...")
            date = datetime.now().strftime("%Y-%m-%d")
            adf_run_id = self.run_ingestion_pipeline(
                pipeline_name="IngestionPipeline",
                date=date
            )
            
            # Step 2: Data Processing via Databricks
            logger.info("\n[Step 2] Processing data with Databricks...")
            if self.databricks_client:
                clusters = self.databricks_client.list_clusters()
                if clusters:
                    cluster_id = clusters[0].cluster_id
                    processing_result = self.process_with_databricks(
                        notebook_path="/Shared/ProcessingNotebook",
                        cluster_id=cluster_id,
                        parameters={"date": date, "source": "ingestion"}
                    )
            
            # Step 3: Load to SQL Database
            logger.info("\n[Step 3] Loading data to SQL Database...")
            sample_data = [
                {"id": 1, "name": "Sample1", "value": 100, "date": date},
                {"id": 2, "name": "Sample2", "value": 200, "date": date},
            ]
            self.load_to_sql(sample_data, "ProcessedData")
            
            # Step 4: Refresh PowerBI Dataset
            logger.info("\n[Step 4] Refreshing PowerBI dashboard...")
            dataset_id = config.get("powerbi.dataset_id")
            if dataset_id:
                self.refresh_powerbi_dataset(dataset_id)
            
            logger.info("\n" + "=" * 80)
            logger.info("Pipeline completed successfully!")
            logger.info("=" * 80)
            
            return {
                "status": "success",
                "adf_run_id": adf_run_id,
                "date": date
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            if self.sql_client:
                self.sql_client.close()


def main():
    """Main function to run the pipeline."""
    pipeline = DataPipeline()
    
    # Run the complete pipeline
    result = pipeline.run_complete_pipeline()
    
    logger.info(f"\nPipeline result: {result}")


if __name__ == "__main__":
    main()
