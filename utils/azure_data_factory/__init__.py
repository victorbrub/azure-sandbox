"""Azure Data Factory Utilities

Provides utilities for managing Azure Data Factory pipelines, triggers,
linked services, datasets, and monitoring pipeline runs.
"""

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class AzureDataFactoryClient:
    """Client for interacting with Azure Data Factory."""
    
    def __init__(self, subscription_id: str, resource_group: str, factory_name: str):
        """
        Initialize Azure Data Factory client.
        
        Args:
            subscription_id: Azure subscription ID
            resource_group: Resource group name
            factory_name: Data Factory name
        """
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.credential = DefaultAzureCredential()
        self.client = DataFactoryManagementClient(self.credential, subscription_id)
        logger.info(f"Initialized ADF client for factory: {factory_name}")
    
    def create_pipeline_run(self, pipeline_name: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Create and start a pipeline run.
        
        Args:
            pipeline_name: Name of the pipeline to run
            parameters: Optional parameters to pass to the pipeline
            
        Returns:
            Run ID of the started pipeline
        """
        try:
            run_response = self.client.pipelines.create_run(
                self.resource_group,
                self.factory_name,
                pipeline_name,
                parameters=parameters or {}
            )
            run_id = run_response.run_id
            logger.info(f"Started pipeline run: {run_id} for pipeline: {pipeline_name}")
            return run_id
        except Exception as e:
            logger.error(f"Failed to start pipeline run: {e}")
            raise
    
    def get_pipeline_run(self, run_id: str) -> Any:
        """
        Get the status and details of a pipeline run.
        
        Args:
            run_id: Run ID of the pipeline
            
        Returns:
            Pipeline run object with status and details
        """
        try:
            run = self.client.pipeline_runs.get(
                self.resource_group,
                self.factory_name,
                run_id
            )
            logger.info(f"Retrieved pipeline run: {run_id}, status: {run.status}")
            return run
        except Exception as e:
            logger.error(f"Failed to get pipeline run: {e}")
            raise
    
    def cancel_pipeline_run(self, run_id: str) -> None:
        """
        Cancel a running pipeline.
        
        Args:
            run_id: Run ID of the pipeline to cancel
        """
        try:
            self.client.pipeline_runs.cancel(
                self.resource_group,
                self.factory_name,
                run_id
            )
            logger.info(f"Cancelled pipeline run: {run_id}")
        except Exception as e:
            logger.error(f"Failed to cancel pipeline run: {e}")
            raise
    
    def list_pipelines(self) -> List[Any]:
        """
        List all pipelines in the Data Factory.
        
        Returns:
            List of pipeline resources
        """
        try:
            pipelines = list(self.client.pipelines.list_by_factory(
                self.resource_group,
                self.factory_name
            ))
            logger.info(f"Retrieved {len(pipelines)} pipelines")
            return pipelines
        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            raise
    
    def get_pipeline(self, pipeline_name: str) -> Any:
        """
        Get a specific pipeline by name.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Pipeline resource
        """
        try:
            pipeline = self.client.pipelines.get(
                self.resource_group,
                self.factory_name,
                pipeline_name
            )
            logger.info(f"Retrieved pipeline: {pipeline_name}")
            return pipeline
        except Exception as e:
            logger.error(f"Failed to get pipeline: {e}")
            raise
    
    def create_trigger(self, trigger_name: str, trigger_spec: Dict[str, Any]) -> Any:
        """
        Create a new trigger in the Data Factory.
        
        Args:
            trigger_name: Name of the trigger
            trigger_spec: Trigger specification dictionary
            
        Returns:
            Created trigger resource
        """
        try:
            trigger = self.client.triggers.create_or_update(
                self.resource_group,
                self.factory_name,
                trigger_name,
                trigger_spec
            )
            logger.info(f"Created trigger: {trigger_name}")
            return trigger
        except Exception as e:
            logger.error(f"Failed to create trigger: {e}")
            raise
    
    def start_trigger(self, trigger_name: str) -> None:
        """
        Start a trigger.
        
        Args:
            trigger_name: Name of the trigger to start
        """
        try:
            self.client.triggers.begin_start(
                self.resource_group,
                self.factory_name,
                trigger_name
            ).result()
            logger.info(f"Started trigger: {trigger_name}")
        except Exception as e:
            logger.error(f"Failed to start trigger: {e}")
            raise
    
    def stop_trigger(self, trigger_name: str) -> None:
        """
        Stop a trigger.
        
        Args:
            trigger_name: Name of the trigger to stop
        """
        try:
            self.client.triggers.begin_stop(
                self.resource_group,
                self.factory_name,
                trigger_name
            ).result()
            logger.info(f"Stopped trigger: {trigger_name}")
        except Exception as e:
            logger.error(f"Failed to stop trigger: {e}")
            raise
    
    def query_activity_runs(self, run_id: str, filter_parameters: Optional[Dict[str, Any]] = None) -> List[Any]:
        """
        Query activity runs for a pipeline run.
        
        Args:
            run_id: Pipeline run ID
            filter_parameters: Optional filter parameters
            
        Returns:
            List of activity runs
        """
        try:
            from datetime import datetime, timedelta
            filter_params = filter_parameters or {
                "lastUpdatedAfter": (datetime.now() - timedelta(days=1)).isoformat(),
                "lastUpdatedBefore": datetime.now().isoformat()
            }
            
            activity_runs = self.client.activity_runs.query_by_pipeline_run(
                self.resource_group,
                self.factory_name,
                run_id,
                filter_params
            )
            logger.info(f"Retrieved activity runs for pipeline run: {run_id}")
            return activity_runs.value
        except Exception as e:
            logger.error(f"Failed to query activity runs: {e}")
            raise
