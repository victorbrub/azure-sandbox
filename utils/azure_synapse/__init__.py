"""Azure Synapse Analytics Utilities

Provides utilities for managing Azure Synapse Analytics workspaces,
SQL pools, Spark pools, and pipeline operations.
"""

from azure.identity import DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient
from azure.mgmt.synapse import SynapseManagementClient
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class AzureSynapseClient:
    """Client for interacting with Azure Synapse Analytics."""
    
    def __init__(self, subscription_id: str, resource_group: str, 
                 workspace_name: str, synapse_endpoint: str):
        """
        Initialize Azure Synapse client.
        
        Args:
            subscription_id: Azure subscription ID
            resource_group: Resource group name
            workspace_name: Synapse workspace name
            synapse_endpoint: Synapse workspace endpoint URL
        """
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name
        self.synapse_endpoint = synapse_endpoint
        self.credential = DefaultAzureCredential()
        
        # Management client for workspace operations
        self.mgmt_client = SynapseManagementClient(self.credential, subscription_id)
        
        # Artifacts client for pipelines, notebooks, etc.
        self.artifacts_client = ArtifactsClient(
            credential=self.credential,
            endpoint=synapse_endpoint
        )
        
        logger.info(f"Initialized Synapse client for workspace: {workspace_name}")
    
    def create_sql_pool(self, sql_pool_name: str, sku_name: str = "DW100c") -> Any:
        """
        Create a SQL pool (dedicated SQL pool).
        
        Args:
            sql_pool_name: Name of the SQL pool
            sku_name: SKU name for the pool (e.g., DW100c, DW200c)
            
        Returns:
            SQL pool resource
        """
        try:
            from azure.mgmt.synapse.models import SqlPool, Sku
            
            sql_pool = SqlPool(
                location=self.get_workspace_location(),
                sku=Sku(name=sku_name)
            )
            
            result = self.mgmt_client.sql_pools.begin_create(
                self.resource_group,
                self.workspace_name,
                sql_pool_name,
                sql_pool
            ).result()
            
            logger.info(f"Created SQL pool: {sql_pool_name}")
            return result
        except Exception as e:
            logger.error(f"Failed to create SQL pool: {e}")
            raise
    
    def pause_sql_pool(self, sql_pool_name: str) -> None:
        """
        Pause a SQL pool to save costs.
        
        Args:
            sql_pool_name: Name of the SQL pool to pause
        """
        try:
            self.mgmt_client.sql_pools.begin_pause(
                self.resource_group,
                self.workspace_name,
                sql_pool_name
            ).result()
            logger.info(f"Paused SQL pool: {sql_pool_name}")
        except Exception as e:
            logger.error(f"Failed to pause SQL pool: {e}")
            raise
    
    def resume_sql_pool(self, sql_pool_name: str) -> None:
        """
        Resume a paused SQL pool.
        
        Args:
            sql_pool_name: Name of the SQL pool to resume
        """
        try:
            self.mgmt_client.sql_pools.begin_resume(
                self.resource_group,
                self.workspace_name,
                sql_pool_name
            ).result()
            logger.info(f"Resumed SQL pool: {sql_pool_name}")
        except Exception as e:
            logger.error(f"Failed to resume SQL pool: {e}")
            raise
    
    def create_spark_pool(self, spark_pool_name: str, node_size: str = "Small",
                         node_count: int = 3, auto_scale_enabled: bool = True) -> Any:
        """
        Create a Spark pool.
        
        Args:
            spark_pool_name: Name of the Spark pool
            node_size: Size of nodes (Small, Medium, Large)
            node_count: Number of nodes
            auto_scale_enabled: Enable auto-scaling
            
        Returns:
            Spark pool resource
        """
        try:
            from azure.mgmt.synapse.models import BigDataPoolResourceInfo, AutoScaleProperties
            
            spark_pool = BigDataPoolResourceInfo(
                location=self.get_workspace_location(),
                node_size=node_size,
                node_count=node_count,
                auto_scale=AutoScaleProperties(
                    enabled=auto_scale_enabled,
                    min_node_count=node_count,
                    max_node_count=node_count * 2
                ) if auto_scale_enabled else None
            )
            
            result = self.mgmt_client.big_data_pools.begin_create_or_update(
                self.resource_group,
                self.workspace_name,
                spark_pool_name,
                spark_pool
            ).result()
            
            logger.info(f"Created Spark pool: {spark_pool_name}")
            return result
        except Exception as e:
            logger.error(f"Failed to create Spark pool: {e}")
            raise
    
    def list_sql_pools(self) -> List[Any]:
        """
        List all SQL pools in the workspace.
        
        Returns:
            List of SQL pool resources
        """
        try:
            pools = list(self.mgmt_client.sql_pools.list_by_workspace(
                self.resource_group,
                self.workspace_name
            ))
            logger.info(f"Retrieved {len(pools)} SQL pools")
            return pools
        except Exception as e:
            logger.error(f"Failed to list SQL pools: {e}")
            raise
    
    def list_spark_pools(self) -> List[Any]:
        """
        List all Spark pools in the workspace.
        
        Returns:
            List of Spark pool resources
        """
        try:
            pools = list(self.mgmt_client.big_data_pools.list_by_workspace(
                self.resource_group,
                self.workspace_name
            ))
            logger.info(f"Retrieved {len(pools)} Spark pools")
            return pools
        except Exception as e:
            logger.error(f"Failed to list Spark pools: {e}")
            raise
    
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
            run_response = self.artifacts_client.pipeline_run.run_pipeline(
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
            run = self.artifacts_client.pipeline_run.get_pipeline_run(run_id)
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
            self.artifacts_client.pipeline_run.cancel_pipeline_run(run_id)
            logger.info(f"Cancelled pipeline run: {run_id}")
        except Exception as e:
            logger.error(f"Failed to cancel pipeline run: {e}")
            raise
    
    def list_pipelines(self) -> List[Any]:
        """
        List all pipelines in the Synapse workspace.
        
        Returns:
            List of pipeline resources
        """
        try:
            pipelines = list(self.artifacts_client.pipeline.get_pipelines_by_workspace())
            logger.info(f"Retrieved {len(pipelines)} pipelines")
            return pipelines
        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            raise
    
    def get_workspace_location(self) -> str:
        """
        Get the location of the Synapse workspace.
        
        Returns:
            Azure region location
        """
        try:
            workspace = self.mgmt_client.workspaces.get(
                self.resource_group,
                self.workspace_name
            )
            return workspace.location
        except Exception as e:
            logger.error(f"Failed to get workspace location: {e}")
            raise
