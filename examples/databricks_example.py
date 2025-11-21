"""
Example: Azure Databricks Operations

This example demonstrates how to use the Azure Databricks utilities
to manage clusters, jobs, and execute notebooks.
"""

from utils.azure_databricks import AzureDatabricksClient
from utils.config import get_config_manager
from utils.logging import configure_logging, get_logger, OperationLogger
import time

# Configure logging
configure_logging(log_level="INFO")
logger = get_logger(__name__)

# Load configuration
config = get_config_manager(config_file="config.yml", env_file=".env")


def main():
    """Main example function."""
    
    # Initialize Databricks client
    workspace_url = config.get("azure.databricks.workspace_url", required=True)
    token = config.get("azure.databricks.token")
    
    databricks_client = AzureDatabricksClient(
        workspace_url=workspace_url,
        token=token
    )
    
    # Example 1: List all clusters
    logger.info("=== Listing all clusters ===")
    clusters = databricks_client.list_clusters()
    for cluster in clusters:
        logger.info(f"Cluster: {cluster.cluster_name} (ID: {cluster.cluster_id})")
    
    # Example 2: Create and start a cluster
    logger.info("\n=== Creating a new cluster ===")
    cluster_name = "test-cluster"
    
    with OperationLogger(logger, "cluster_creation", cluster=cluster_name):
        cluster_id = databricks_client.create_cluster(
            cluster_name=cluster_name,
            spark_version="13.3.x-scala2.12",
            node_type_id="Standard_DS3_v2",
            num_workers=2,
            autotermination_minutes=30
        )
        
        logger.info(f"Created cluster: {cluster_id}")
        
        # Wait for cluster to start
        while True:
            status = databricks_client.get_cluster_status(cluster_id)
            logger.info(f"Cluster status: {status}")
            
            if status == "RUNNING":
                break
            elif status in ["ERROR", "TERMINATED"]:
                raise Exception(f"Cluster failed to start: {status}")
            
            time.sleep(30)
        
        logger.info("Cluster is running!")
    
    # Example 3: Create and run a job
    logger.info("\n=== Creating and running a job ===")
    job_name = "test-job"
    
    task_config = {
        "task_key": "main_task",
        "notebook_task": {
            "notebook_path": "/Users/your-email@domain.com/YourNotebook",
            "base_parameters": {
                "param1": "value1"
            }
        },
        "existing_cluster_id": cluster_id
    }
    
    with OperationLogger(logger, "job_execution", job=job_name):
        # Create job
        job_id = databricks_client.create_job(job_name, task_config)
        logger.info(f"Created job: {job_id}")
        
        # Run job
        run_id = databricks_client.run_job(job_id)
        logger.info(f"Started job run: {run_id}")
        
        # Monitor job run
        while True:
            status = databricks_client.get_run_status(run_id)
            logger.info(f"Job status: {status}")
            
            if status in ["SUCCESS", "FAILED", "CANCELED", "TIMEOUT"]:
                break
            
            time.sleep(10)
        
        if status == "SUCCESS":
            logger.info("Job completed successfully!")
        else:
            logger.error(f"Job failed with status: {status}")
    
    # Example 4: Execute a notebook directly
    logger.info("\n=== Executing a notebook ===")
    notebook_path = "/Users/your-email@domain.com/YourNotebook"
    
    with OperationLogger(logger, "notebook_execution", notebook=notebook_path):
        result = databricks_client.execute_notebook(
            notebook_path=notebook_path,
            cluster_id=cluster_id,
            parameters={"date": "2024-01-01"},
            timeout_seconds=600
        )
        
        logger.info(f"Notebook execution result: {result['status']}")
    
    # Cleanup: Terminate the cluster
    logger.info("\n=== Terminating cluster ===")
    databricks_client.terminate_cluster(cluster_id)
    logger.info("Cluster terminated")


if __name__ == "__main__":
    main()
