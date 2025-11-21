"""
Example: Azure Data Factory Operations

This example demonstrates how to use the Azure Data Factory utilities
to manage pipelines, triggers, and monitor pipeline runs.
"""

from utils.azure_data_factory import AzureDataFactoryClient
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
    
    # Initialize ADF client
    subscription_id = config.get("azure.subscription_id", required=True)
    resource_group = config.get("azure.data_factory.resource_group", required=True)
    factory_name = config.get("azure.data_factory.factory_name", required=True)
    
    adf_client = AzureDataFactoryClient(
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name
    )
    
    # Example 1: List all pipelines
    logger.info("=== Listing all pipelines ===")
    pipelines = adf_client.list_pipelines()
    for pipeline in pipelines:
        logger.info(f"Pipeline: {pipeline.name}")
    
    # Example 2: Run a pipeline with parameters
    logger.info("\n=== Running a pipeline ===")
    pipeline_name = "YourPipelineName"  # Replace with your pipeline name
    
    with OperationLogger(logger, "pipeline_execution", pipeline=pipeline_name):
        # Start pipeline run
        run_id = adf_client.create_pipeline_run(
            pipeline_name=pipeline_name,
            parameters={
                "param1": "value1",
                "param2": "value2"
            }
        )
        
        logger.info(f"Started pipeline run: {run_id}")
        
        # Monitor pipeline run
        while True:
            run_info = adf_client.get_pipeline_run(run_id)
            status = run_info.status
            logger.info(f"Pipeline status: {status}")
            
            if status in ["Succeeded", "Failed", "Cancelled"]:
                break
            
            time.sleep(10)  # Wait 10 seconds before checking again
        
        if status == "Succeeded":
            logger.info("Pipeline completed successfully!")
            
            # Get activity runs
            activity_runs = adf_client.query_activity_runs(run_id)
            logger.info(f"Activity runs: {len(activity_runs)}")
            for activity in activity_runs:
                logger.info(f"  - {activity.activity_name}: {activity.status}")
        else:
            logger.error(f"Pipeline failed with status: {status}")
    
    # Example 3: Manage triggers
    logger.info("\n=== Managing triggers ===")
    trigger_name = "YourTriggerName"  # Replace with your trigger name
    
    try:
        # Start trigger
        adf_client.start_trigger(trigger_name)
        logger.info(f"Started trigger: {trigger_name}")
        
        # Wait a bit
        time.sleep(5)
        
        # Stop trigger
        adf_client.stop_trigger(trigger_name)
        logger.info(f"Stopped trigger: {trigger_name}")
    except Exception as e:
        logger.error(f"Trigger operation failed: {e}")


if __name__ == "__main__":
    main()
