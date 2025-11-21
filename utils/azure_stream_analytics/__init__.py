"""Azure Stream Analytics Utilities

Provides utilities for managing Azure Stream Analytics jobs,
including starting, stopping, and monitoring streaming jobs.
"""

from azure.identity import DefaultAzureCredential
from azure.mgmt.streamanalytics import StreamAnalyticsManagementClient
from azure.mgmt.streamanalytics.models import *
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class AzureStreamAnalyticsClient:
    """Client for interacting with Azure Stream Analytics."""
    
    def __init__(self, subscription_id: str, resource_group: str):
        """
        Initialize Azure Stream Analytics client.
        
        Args:
            subscription_id: Azure subscription ID
            resource_group: Resource group name
        """
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.credential = DefaultAzureCredential()
        self.client = StreamAnalyticsManagementClient(self.credential, subscription_id)
        logger.info(f"Initialized Stream Analytics client for resource group: {resource_group}")
    
    def create_job(self, job_name: str, location: str, 
                  query: str, inputs: List[Dict[str, Any]], 
                  outputs: List[Dict[str, Any]]) -> Any:
        """
        Create a new Stream Analytics job.
        
        Args:
            job_name: Name of the job
            location: Azure region location
            query: Stream Analytics query
            inputs: List of input configurations
            outputs: List of output configurations
            
        Returns:
            Created job resource
        """
        try:
            job = StreamingJob(
                location=location,
                sku=Sku(name="Standard"),
                transformation=Transformation(
                    name="Transformation",
                    query=query,
                    streaming_units=1
                )
            )
            
            result = self.client.streaming_jobs.begin_create_or_replace(
                self.resource_group,
                job_name,
                job
            ).result()
            
            logger.info(f"Created Stream Analytics job: {job_name}")
            return result
        except Exception as e:
            logger.error(f"Failed to create job: {e}")
            raise
    
    def start_job(self, job_name: str, output_start_mode: str = "JobStartTime") -> None:
        """
        Start a Stream Analytics job.
        
        Args:
            job_name: Name of the job to start
            output_start_mode: Output start mode (JobStartTime, CustomTime, LastOutputEventTime)
        """
        try:
            self.client.streaming_jobs.begin_start(
                self.resource_group,
                job_name,
                start_job_parameters=StartStreamingJobParameters(
                    output_start_mode=output_start_mode
                )
            ).result()
            logger.info(f"Started Stream Analytics job: {job_name}")
        except Exception as e:
            logger.error(f"Failed to start job: {e}")
            raise
    
    def stop_job(self, job_name: str) -> None:
        """
        Stop a Stream Analytics job.
        
        Args:
            job_name: Name of the job to stop
        """
        try:
            self.client.streaming_jobs.begin_stop(
                self.resource_group,
                job_name
            ).result()
            logger.info(f"Stopped Stream Analytics job: {job_name}")
        except Exception as e:
            logger.error(f"Failed to stop job: {e}")
            raise
    
    def get_job(self, job_name: str) -> Any:
        """
        Get details of a Stream Analytics job.
        
        Args:
            job_name: Name of the job
            
        Returns:
            Job resource with details
        """
        try:
            job = self.client.streaming_jobs.get(
                self.resource_group,
                job_name
            )
            logger.info(f"Retrieved job: {job_name}, state: {job.job_state}")
            return job
        except Exception as e:
            logger.error(f"Failed to get job: {e}")
            raise
    
    def get_job_state(self, job_name: str) -> str:
        """
        Get the current state of a Stream Analytics job.
        
        Args:
            job_name: Name of the job
            
        Returns:
            Job state (Created, Starting, Running, Stopping, Stopped, etc.)
        """
        try:
            job = self.get_job(job_name)
            state = job.job_state if job.job_state else "Unknown"
            logger.info(f"Job {job_name} state: {state}")
            return state
        except Exception as e:
            logger.error(f"Failed to get job state: {e}")
            raise
    
    def list_jobs(self) -> List[Any]:
        """
        List all Stream Analytics jobs in the resource group.
        
        Returns:
            List of job resources
        """
        try:
            jobs = list(self.client.streaming_jobs.list_by_resource_group(
                self.resource_group
            ))
            logger.info(f"Retrieved {len(jobs)} Stream Analytics jobs")
            return jobs
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            raise
    
    def delete_job(self, job_name: str) -> None:
        """
        Delete a Stream Analytics job.
        
        Args:
            job_name: Name of the job to delete
        """
        try:
            self.client.streaming_jobs.begin_delete(
                self.resource_group,
                job_name
            ).result()
            logger.info(f"Deleted Stream Analytics job: {job_name}")
        except Exception as e:
            logger.error(f"Failed to delete job: {e}")
            raise
    
    def scale_job(self, job_name: str, streaming_units: int) -> None:
        """
        Scale a Stream Analytics job by changing streaming units.
        
        Args:
            job_name: Name of the job
            streaming_units: Number of streaming units (1, 3, 6, 12, etc.)
        """
        try:
            job = self.get_job(job_name)
            job.transformation.streaming_units = streaming_units
            
            self.client.streaming_jobs.begin_create_or_replace(
                self.resource_group,
                job_name,
                job
            ).result()
            
            logger.info(f"Scaled job {job_name} to {streaming_units} streaming units")
        except Exception as e:
            logger.error(f"Failed to scale job: {e}")
            raise
    
    def create_input(self, job_name: str, input_name: str, 
                    input_config: Dict[str, Any]) -> Any:
        """
        Create an input for a Stream Analytics job.
        
        Args:
            job_name: Name of the job
            input_name: Name of the input
            input_config: Input configuration dictionary
            
        Returns:
            Created input resource
        """
        try:
            input_obj = Input.from_dict(input_config)
            result = self.client.inputs.create_or_replace(
                self.resource_group,
                job_name,
                input_name,
                input_obj
            )
            logger.info(f"Created input: {input_name} for job: {job_name}")
            return result
        except Exception as e:
            logger.error(f"Failed to create input: {e}")
            raise
    
    def create_output(self, job_name: str, output_name: str,
                     output_config: Dict[str, Any]) -> Any:
        """
        Create an output for a Stream Analytics job.
        
        Args:
            job_name: Name of the job
            output_name: Name of the output
            output_config: Output configuration dictionary
            
        Returns:
            Created output resource
        """
        try:
            output_obj = Output.from_dict(output_config)
            result = self.client.outputs.create_or_replace(
                self.resource_group,
                job_name,
                output_name,
                output_obj
            )
            logger.info(f"Created output: {output_name} for job: {job_name}")
            return result
        except Exception as e:
            logger.error(f"Failed to create output: {e}")
            raise
    
    def test_input(self, job_name: str, input_name: str) -> bool:
        """
        Test an input connection.
        
        Args:
            job_name: Name of the job
            input_name: Name of the input
            
        Returns:
            True if test successful, False otherwise
        """
        try:
            result = self.client.inputs.test(
                self.resource_group,
                job_name,
                input_name
            )
            success = result.status == "TestSucceeded"
            logger.info(f"Input test for {input_name}: {'Success' if success else 'Failed'}")
            return success
        except Exception as e:
            logger.error(f"Failed to test input: {e}")
            raise
    
    def test_output(self, job_name: str, output_name: str) -> bool:
        """
        Test an output connection.
        
        Args:
            job_name: Name of the job
            output_name: Name of the output
            
        Returns:
            True if test successful, False otherwise
        """
        try:
            result = self.client.outputs.test(
                self.resource_group,
                job_name,
                output_name
            )
            success = result.status == "TestSucceeded"
            logger.info(f"Output test for {output_name}: {'Success' if success else 'Failed'}")
            return success
        except Exception as e:
            logger.error(f"Failed to test output: {e}")
            raise
