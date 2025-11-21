"""PowerBI API Utilities

Provides utilities for interacting with PowerBI REST API including
dataset refresh, activity events, reports, dashboards, and workspace management.
"""

import requests
import msal
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class PowerBIClient:
    """Client for interacting with PowerBI REST API."""
    
    def __init__(self, client_id: str, client_secret: Optional[str] = None,
                 tenant_id: Optional[str] = None, username: Optional[str] = None,
                 password: Optional[str] = None):
        """
        Initialize PowerBI API client.
        
        Args:
            client_id: Azure AD application (client) ID
            client_secret: Application secret (for service principal auth)
            tenant_id: Azure AD tenant ID
            username: User's username (for user auth)
            password: User's password (for user auth)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.username = username
        self.base_url = "https://api.powerbi.com/v1.0/myorg"
        
        # Get access token
        self.access_token = self._get_access_token()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        logger.info("Initialized PowerBI client")
    
    def _get_access_token(self) -> str:
        """
        Get access token for PowerBI API using MSAL.
        
        Returns:
            Access token string
        """
        try:
            authority = f"https://login.microsoftonline.com/{self.tenant_id or 'common'}"
            scope = ["https://analysis.windows.net/powerbi/api/.default"]
            
            if self.client_secret:
                # Service principal authentication
                app = msal.ConfidentialClientApplication(
                    self.client_id,
                    authority=authority,
                    client_credential=self.client_secret
                )
                result = app.acquire_token_for_client(scopes=scope)
            elif self.username and self.password:
                # User authentication
                app = msal.PublicClientApplication(
                    self.client_id,
                    authority=authority
                )
                result = app.acquire_token_by_username_password(
                    self.username,
                    self.password,
                    scopes=scope
                )
            else:
                # Interactive authentication
                app = msal.PublicClientApplication(
                    self.client_id,
                    authority=authority
                )
                result = app.acquire_token_interactive(scopes=scope)
            
            if "access_token" in result:
                logger.info("Successfully acquired access token")
                return result["access_token"]
            else:
                error = result.get("error_description", result.get("error"))
                raise Exception(f"Failed to acquire token: {error}")
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            raise
    
    def _make_request(self, method: str, endpoint: str, 
                     data: Optional[Dict[str, Any]] = None,
                     params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make an API request to PowerBI.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (without base URL)
            data: Optional request body
            params: Optional query parameters
            
        Returns:
            Response JSON data
        """
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params
            )
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    # Dataset Operations
    
    def refresh_dataset(self, dataset_id: str, notify_option: str = "NoNotification") -> str:
        """
        Trigger a refresh for a dataset.
        
        Args:
            dataset_id: ID of the dataset to refresh
            notify_option: Notification option (NoNotification, MailOnFailure, MailOnCompletion)
            
        Returns:
            Request ID for tracking the refresh
        """
        try:
            data = {"notifyOption": notify_option}
            response = self._make_request(
                "POST",
                f"datasets/{dataset_id}/refreshes",
                data=data
            )
            logger.info(f"Triggered refresh for dataset: {dataset_id}")
            return response.get("requestId", "")
        except Exception as e:
            logger.error(f"Failed to refresh dataset: {e}")
            raise
    
    def get_refresh_history(self, dataset_id: str, top: int = 10) -> List[Dict[str, Any]]:
        """
        Get refresh history for a dataset.
        
        Args:
            dataset_id: ID of the dataset
            top: Number of refresh records to return
            
        Returns:
            List of refresh history records
        """
        try:
            response = self._make_request(
                "GET",
                f"datasets/{dataset_id}/refreshes",
                params={"$top": top}
            )
            refreshes = response.get("value", [])
            logger.info(f"Retrieved {len(refreshes)} refresh records for dataset: {dataset_id}")
            return refreshes
        except Exception as e:
            logger.error(f"Failed to get refresh history: {e}")
            raise
    
    def cancel_refresh(self, dataset_id: str, refresh_id: str) -> None:
        """
        Cancel an in-progress dataset refresh.
        
        Args:
            dataset_id: ID of the dataset
            refresh_id: ID of the refresh to cancel
        """
        try:
            self._make_request(
                "DELETE",
                f"datasets/{dataset_id}/refreshes/{refresh_id}"
            )
            logger.info(f"Cancelled refresh {refresh_id} for dataset: {dataset_id}")
        except Exception as e:
            logger.error(f"Failed to cancel refresh: {e}")
            raise
    
    def get_datasets(self, group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all datasets in a workspace or personal workspace.
        
        Args:
            group_id: Optional workspace (group) ID
            
        Returns:
            List of datasets
        """
        try:
            endpoint = f"groups/{group_id}/datasets" if group_id else "datasets"
            response = self._make_request("GET", endpoint)
            datasets = response.get("value", [])
            logger.info(f"Retrieved {len(datasets)} datasets")
            return datasets
        except Exception as e:
            logger.error(f"Failed to get datasets: {e}")
            raise
    
    def get_dataset(self, dataset_id: str, group_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get details of a specific dataset.
        
        Args:
            dataset_id: ID of the dataset
            group_id: Optional workspace (group) ID
            
        Returns:
            Dataset details
        """
        try:
            endpoint = f"groups/{group_id}/datasets/{dataset_id}" if group_id else f"datasets/{dataset_id}"
            dataset = self._make_request("GET", endpoint)
            logger.info(f"Retrieved dataset: {dataset_id}")
            return dataset
        except Exception as e:
            logger.error(f"Failed to get dataset: {e}")
            raise
    
    # Activity Events (Admin API)
    
    def get_activity_events(self, start_datetime: str, end_datetime: str) -> List[Dict[str, Any]]:
        """
        Get activity events for the organization.
        Requires PowerBI Admin permissions.
        
        Args:
            start_datetime: Start datetime in ISO 8601 format (UTC)
            end_datetime: End datetime in ISO 8601 format (UTC)
            
        Returns:
            List of activity events
        """
        try:
            params = {
                "startDateTime": f"'{start_datetime}'",
                "endDateTime": f"'{end_datetime}'"
            }
            
            # Admin API uses different base URL
            url = f"https://api.powerbi.com/v1.0/myorg/admin/activityevents"
            response = requests.get(
                url=url,
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            events = data.get("activityEventEntities", [])
            logger.info(f"Retrieved {len(events)} activity events")
            
            # Handle pagination if continuation token exists
            continuation_token = data.get("continuationToken")
            while continuation_token:
                params["continuationToken"] = continuation_token
                response = requests.get(url=url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                events.extend(data.get("activityEventEntities", []))
                continuation_token = data.get("continuationToken")
            
            logger.info(f"Total retrieved {len(events)} activity events")
            return events
        except Exception as e:
            logger.error(f"Failed to get activity events: {e}")
            raise
    
    # Report Operations
    
    def get_reports(self, group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all reports in a workspace or personal workspace.
        
        Args:
            group_id: Optional workspace (group) ID
            
        Returns:
            List of reports
        """
        try:
            endpoint = f"groups/{group_id}/reports" if group_id else "reports"
            response = self._make_request("GET", endpoint)
            reports = response.get("value", [])
            logger.info(f"Retrieved {len(reports)} reports")
            return reports
        except Exception as e:
            logger.error(f"Failed to get reports: {e}")
            raise
    
    def get_report(self, report_id: str, group_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get details of a specific report.
        
        Args:
            report_id: ID of the report
            group_id: Optional workspace (group) ID
            
        Returns:
            Report details
        """
        try:
            endpoint = f"groups/{group_id}/reports/{report_id}" if group_id else f"reports/{report_id}"
            report = self._make_request("GET", endpoint)
            logger.info(f"Retrieved report: {report_id}")
            return report
        except Exception as e:
            logger.error(f"Failed to get report: {e}")
            raise
    
    def clone_report(self, report_id: str, name: str, 
                    target_workspace_id: Optional[str] = None,
                    target_model_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Clone a report to a workspace.
        
        Args:
            report_id: ID of the report to clone
            name: Name for the cloned report
            target_workspace_id: Optional target workspace ID
            target_model_id: Optional target dataset ID
            
        Returns:
            Cloned report details
        """
        try:
            data = {"name": name}
            if target_workspace_id:
                data["targetWorkspaceId"] = target_workspace_id
            if target_model_id:
                data["targetModelId"] = target_model_id
            
            response = self._make_request(
                "POST",
                f"reports/{report_id}/Clone",
                data=data
            )
            logger.info(f"Cloned report: {report_id} to {name}")
            return response
        except Exception as e:
            logger.error(f"Failed to clone report: {e}")
            raise
    
    def export_report(self, report_id: str, format: str = "PDF",
                     group_id: Optional[str] = None) -> bytes:
        """
        Export a report to file.
        
        Args:
            report_id: ID of the report
            format: Export format (PDF, PPTX, PNG)
            group_id: Optional workspace (group) ID
            
        Returns:
            Report file content as bytes
        """
        try:
            endpoint = f"groups/{group_id}/reports/{report_id}/Export" if group_id else f"reports/{report_id}/Export"
            
            # Start export
            data = {"format": format}
            export_response = self._make_request("POST", endpoint, data=data)
            export_id = export_response.get("id")
            
            # Poll for completion
            import time
            max_attempts = 60
            for _ in range(max_attempts):
                status_endpoint = f"{endpoint}/{export_id}"
                status = self._make_request("GET", status_endpoint)
                
                if status.get("status") == "Succeeded":
                    # Download file
                    file_endpoint = f"{status_endpoint}/file"
                    url = f"{self.base_url}/{file_endpoint}"
                    response = requests.get(url=url, headers=self.headers)
                    response.raise_for_status()
                    logger.info(f"Exported report: {report_id} to {format}")
                    return response.content
                elif status.get("status") == "Failed":
                    raise Exception(f"Export failed: {status.get('error')}")
                
                time.sleep(5)
            
            raise Exception("Export timeout")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
            raise
    
    # Dashboard Operations
    
    def get_dashboards(self, group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all dashboards in a workspace or personal workspace.
        
        Args:
            group_id: Optional workspace (group) ID
            
        Returns:
            List of dashboards
        """
        try:
            endpoint = f"groups/{group_id}/dashboards" if group_id else "dashboards"
            response = self._make_request("GET", endpoint)
            dashboards = response.get("value", [])
            logger.info(f"Retrieved {len(dashboards)} dashboards")
            return dashboards
        except Exception as e:
            logger.error(f"Failed to get dashboards: {e}")
            raise
    
    def get_dashboard(self, dashboard_id: str, group_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get details of a specific dashboard.
        
        Args:
            dashboard_id: ID of the dashboard
            group_id: Optional workspace (group) ID
            
        Returns:
            Dashboard details
        """
        try:
            endpoint = f"groups/{group_id}/dashboards/{dashboard_id}" if group_id else f"dashboards/{dashboard_id}"
            dashboard = self._make_request("GET", endpoint)
            logger.info(f"Retrieved dashboard: {dashboard_id}")
            return dashboard
        except Exception as e:
            logger.error(f"Failed to get dashboard: {e}")
            raise
    
    def get_dashboard_tiles(self, dashboard_id: str, 
                           group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get tiles from a dashboard.
        
        Args:
            dashboard_id: ID of the dashboard
            group_id: Optional workspace (group) ID
            
        Returns:
            List of tiles
        """
        try:
            endpoint = f"groups/{group_id}/dashboards/{dashboard_id}/tiles" if group_id else f"dashboards/{dashboard_id}/tiles"
            response = self._make_request("GET", endpoint)
            tiles = response.get("value", [])
            logger.info(f"Retrieved {len(tiles)} tiles from dashboard: {dashboard_id}")
            return tiles
        except Exception as e:
            logger.error(f"Failed to get dashboard tiles: {e}")
            raise
    
    # Workspace (Group) Operations
    
    def get_workspaces(self) -> List[Dict[str, Any]]:
        """
        Get all workspaces (groups) the user has access to.
        
        Returns:
            List of workspaces
        """
        try:
            response = self._make_request("GET", "groups")
            workspaces = response.get("value", [])
            logger.info(f"Retrieved {len(workspaces)} workspaces")
            return workspaces
        except Exception as e:
            logger.error(f"Failed to get workspaces: {e}")
            raise
    
    def create_workspace(self, name: str) -> Dict[str, Any]:
        """
        Create a new workspace.
        
        Args:
            name: Name of the workspace
            
        Returns:
            Created workspace details
        """
        try:
            data = {"name": name}
            workspace = self._make_request("POST", "groups", data=data)
            logger.info(f"Created workspace: {name}")
            return workspace
        except Exception as e:
            logger.error(f"Failed to create workspace: {e}")
            raise
    
    def delete_workspace(self, group_id: str) -> None:
        """
        Delete a workspace.
        
        Args:
            group_id: ID of the workspace to delete
        """
        try:
            self._make_request("DELETE", f"groups/{group_id}")
            logger.info(f"Deleted workspace: {group_id}")
        except Exception as e:
            logger.error(f"Failed to delete workspace: {e}")
            raise
