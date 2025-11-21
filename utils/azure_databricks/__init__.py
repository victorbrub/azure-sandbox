"""Azure Databricks Utilities

Provides utilities for managing Azure Databricks clusters, jobs, 
workspaces, and notebook operations.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute
from typing import Dict, List, Optional, Any
import logging
import time

logger = logging.getLogger(__name__)


class AzureDatabricksClient:
    """Client for interacting with Azure Databricks."""
    
    def __init__(self, workspace_url: str, token: Optional[str] = None):
        """
        Initialize Azure Databricks client.
        
        Args:
            workspace_url: Databricks workspace URL
            token: Optional personal access token (can use Azure AD auth instead)
        """
        self.workspace_url = workspace_url
        if token:
            self.client = WorkspaceClient(host=workspace_url, token=token)
        else:
            # Use Azure AD authentication
            self.client = WorkspaceClient(host=workspace_url)
        logger.info(f"Initialized Databricks client for workspace: {workspace_url}")
    
    def create_cluster(self, cluster_name: str, spark_version: str, 
                      node_type_id: str, num_workers: int,
                      autotermination_minutes: int = 30) -> str:
        """
        Create a new Databricks cluster.
        
        Args:
            cluster_name: Name of the cluster
            spark_version: Spark version to use
            node_type_id: Azure VM type for nodes
            num_workers: Number of worker nodes
            autotermination_minutes: Minutes of inactivity before auto-termination
            
        Returns:
            Cluster ID
        """
        try:
            cluster = self.client.clusters.create(
                cluster_name=cluster_name,
                spark_version=spark_version,
                node_type_id=node_type_id,
                num_workers=num_workers,
                autotermination_minutes=autotermination_minutes
            )
            logger.info(f"Created cluster: {cluster_name}, ID: {cluster.cluster_id}")
            return cluster.cluster_id
        except Exception as e:
            logger.error(f"Failed to create cluster: {e}")
            raise
    
    def start_cluster(self, cluster_id: str) -> None:
        """
        Start a stopped cluster.
        
        Args:
            cluster_id: ID of the cluster to start
        """
        try:
            self.client.clusters.start(cluster_id=cluster_id)
            logger.info(f"Started cluster: {cluster_id}")
        except Exception as e:
            logger.error(f"Failed to start cluster: {e}")
            raise
    
    def terminate_cluster(self, cluster_id: str) -> None:
        """
        Terminate a running cluster.
        
        Args:
            cluster_id: ID of the cluster to terminate
        """
        try:
            self.client.clusters.delete(cluster_id=cluster_id)
            logger.info(f"Terminated cluster: {cluster_id}")
        except Exception as e:
            logger.error(f"Failed to terminate cluster: {e}")
            raise
    
    def get_cluster_status(self, cluster_id: str) -> str:
        """
        Get the status of a cluster.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            Cluster state (PENDING, RUNNING, TERMINATED, etc.)
        """
        try:
            cluster = self.client.clusters.get(cluster_id=cluster_id)
            status = cluster.state.value if cluster.state else "UNKNOWN"
            logger.info(f"Cluster {cluster_id} status: {status}")
            return status
        except Exception as e:
            logger.error(f"Failed to get cluster status: {e}")
            raise
    
    def list_clusters(self) -> List[Any]:
        """
        List all clusters in the workspace.
        
        Returns:
            List of cluster information
        """
        try:
            clusters = list(self.client.clusters.list())
            logger.info(f"Retrieved {len(clusters)} clusters")
            return clusters
        except Exception as e:
            logger.error(f"Failed to list clusters: {e}")
            raise
    
    def create_job(self, job_name: str, task_config: Dict[str, Any]) -> int:
        """
        Create a new job in Databricks.
        
        Args:
            job_name: Name of the job
            task_config: Task configuration dictionary
            
        Returns:
            Job ID
        """
        try:
            job = self.client.jobs.create(
                name=job_name,
                tasks=[jobs.Task.from_dict(task_config)]
            )
            logger.info(f"Created job: {job_name}, ID: {job.job_id}")
            return job.job_id
        except Exception as e:
            logger.error(f"Failed to create job: {e}")
            raise
    
    def run_job(self, job_id: int, parameters: Optional[Dict[str, str]] = None) -> int:
        """
        Run a job by ID.
        
        Args:
            job_id: ID of the job to run
            parameters: Optional parameters to pass to the job
            
        Returns:
            Run ID
        """
        try:
            run = self.client.jobs.run_now(
                job_id=job_id,
                notebook_params=parameters or {}
            )
            logger.info(f"Started job run: {run.run_id} for job: {job_id}")
            return run.run_id
        except Exception as e:
            logger.error(f"Failed to run job: {e}")
            raise
    
    def get_run_status(self, run_id: int) -> str:
        """
        Get the status of a job run.
        
        Args:
            run_id: Run ID
            
        Returns:
            Run state (PENDING, RUNNING, SUCCESS, FAILED, etc.)
        """
        try:
            run = self.client.jobs.get_run(run_id=run_id)
            status = run.state.life_cycle_state.value if run.state else "UNKNOWN"
            logger.info(f"Run {run_id} status: {status}")
            return status
        except Exception as e:
            logger.error(f"Failed to get run status: {e}")
            raise
    
    def cancel_run(self, run_id: int) -> None:
        """
        Cancel a running job.
        
        Args:
            run_id: Run ID to cancel
        """
        try:
            self.client.jobs.cancel_run(run_id=run_id)
            logger.info(f"Cancelled run: {run_id}")
        except Exception as e:
            logger.error(f"Failed to cancel run: {e}")
            raise
    
    def upload_notebook(self, notebook_path: str, content: str, language: str = "PYTHON") -> None:
        """
        Upload a notebook to the workspace.
        
        Args:
            notebook_path: Path in workspace where notebook will be stored
            content: Notebook content (base64 encoded if needed)
            language: Programming language (PYTHON, SQL, SCALA, R)
        """
        try:
            self.client.workspace.import_(
                path=notebook_path,
                content=content.encode(),
                language=language,
                format="SOURCE"
            )
            logger.info(f"Uploaded notebook to: {notebook_path}")
        except Exception as e:
            logger.error(f"Failed to upload notebook: {e}")
            raise
    
    def execute_notebook(self, notebook_path: str, cluster_id: str, 
                        parameters: Optional[Dict[str, str]] = None,
                        timeout_seconds: int = 3600) -> Dict[str, Any]:
        """
        Execute a notebook and wait for completion.
        
        Args:
            notebook_path: Path to the notebook in workspace
            cluster_id: Cluster ID to run the notebook on
            parameters: Optional parameters to pass to the notebook
            timeout_seconds: Maximum time to wait for completion
            
        Returns:
            Execution results
        """
        try:
            from databricks.sdk.service.jobs import NotebookTask, RunSubmitTaskSettings
            
            run = self.client.jobs.submit(
                run_name=f"Execute {notebook_path}",
                tasks=[
                    RunSubmitTaskSettings(
                        task_key="notebook_task",
                        notebook_task=NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters=parameters or {}
                        ),
                        existing_cluster_id=cluster_id
                    )
                ]
            )
            
            run_id = run.run_id
            logger.info(f"Submitted notebook execution: {run_id}")
            
            # Wait for completion
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                status = self.get_run_status(run_id)
                if status in ["SUCCESS", "FAILED", "CANCELED", "TIMEOUT"]:
                    break
                time.sleep(10)
            
            run_details = self.client.jobs.get_run(run_id=run_id)
            logger.info(f"Notebook execution completed with status: {status}")
            return {"run_id": run_id, "status": status, "details": run_details}
        except Exception as e:
            logger.error(f"Failed to execute notebook: {e}")
            raise
