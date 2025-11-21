"""
Example: PowerBI API Operations

This example demonstrates how to use the PowerBI API utilities
to refresh datasets, get activity events, manage reports, and dashboards.
"""

from utils.powerbi import PowerBIClient
from utils.config import get_config_manager
from utils.logging import configure_logging, get_logger, OperationLogger
from datetime import datetime, timedelta
import time

# Configure logging
configure_logging(log_level="INFO")
logger = get_logger(__name__)

# Load configuration
config = get_config_manager(config_file="config.yml", env_file=".env")


def main():
    """Main example function."""
    
    # Initialize PowerBI client
    creds = config.get_powerbi_credentials()
    
    powerbi_client = PowerBIClient(
        client_id=creds["client_id"],
        client_secret=creds.get("client_secret"),
        tenant_id=creds.get("tenant_id"),
        username=creds.get("username"),
        password=creds.get("password")
    )
    
    # Example 1: List all workspaces
    logger.info("=== Listing all workspaces ===")
    workspaces = powerbi_client.get_workspaces()
    for workspace in workspaces:
        logger.info(f"Workspace: {workspace['name']} (ID: {workspace['id']})")
    
    # Get workspace ID (use first workspace or from config)
    workspace_id = config.get("powerbi.workspace_id") or (workspaces[0]["id"] if workspaces else None)
    
    if not workspace_id:
        logger.error("No workspace found. Please configure a workspace.")
        return
    
    # Example 2: List datasets and trigger refresh
    logger.info("\n=== Managing datasets ===")
    datasets = powerbi_client.get_datasets(group_id=workspace_id)
    
    for dataset in datasets:
        logger.info(f"Dataset: {dataset['name']} (ID: {dataset['id']})")
    
    if datasets:
        dataset_id = datasets[0]["id"]
        
        # Trigger dataset refresh
        logger.info(f"\n=== Triggering refresh for dataset: {dataset_id} ===")
        with OperationLogger(logger, "dataset_refresh", dataset_id=dataset_id):
            request_id = powerbi_client.refresh_dataset(
                dataset_id=dataset_id,
                notify_option="MailOnFailure"
            )
            logger.info(f"Refresh triggered with request ID: {request_id}")
            
            # Wait a bit and check refresh history
            time.sleep(5)
            
            refresh_history = powerbi_client.get_refresh_history(dataset_id, top=5)
            logger.info("Recent refresh history:")
            for refresh in refresh_history:
                status = refresh.get("status", "Unknown")
                start_time = refresh.get("startTime", "N/A")
                end_time = refresh.get("endTime", "N/A")
                logger.info(f"  - Status: {status}, Start: {start_time}, End: {end_time}")
    
    # Example 3: Get activity events (requires admin permissions)
    logger.info("\n=== Getting activity events ===")
    try:
        # Get events from the last 24 hours
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=1)
        
        with OperationLogger(logger, "get_activity_events"):
            events = powerbi_client.get_activity_events(
                start_datetime=start_time.isoformat() + "Z",
                end_datetime=end_time.isoformat() + "Z"
            )
            
            logger.info(f"Retrieved {len(events)} activity events")
            
            # Group events by activity type
            event_types = {}
            for event in events:
                activity = event.get("Activity", "Unknown")
                event_types[activity] = event_types.get(activity, 0) + 1
            
            logger.info("Activity breakdown:")
            for activity, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  - {activity}: {count}")
    except Exception as e:
        logger.warning(f"Could not retrieve activity events (admin permissions required): {e}")
    
    # Example 4: List and manage reports
    logger.info("\n=== Managing reports ===")
    reports = powerbi_client.get_reports(group_id=workspace_id)
    
    for report in reports:
        logger.info(f"Report: {report['name']} (ID: {report['id']})")
    
    if reports:
        report_id = reports[0]["id"]
        
        # Get report details
        report_details = powerbi_client.get_report(report_id, group_id=workspace_id)
        logger.info(f"\nReport details: {report_details['name']}")
        logger.info(f"  - Web URL: {report_details.get('webUrl', 'N/A')}")
        logger.info(f"  - Embed URL: {report_details.get('embedUrl', 'N/A')}")
        
        # Clone report (optional)
        # cloned_report = powerbi_client.clone_report(
        #     report_id=report_id,
        #     name=f"Clone of {report_details['name']}",
        #     target_workspace_id=workspace_id
        # )
        # logger.info(f"Cloned report: {cloned_report['id']}")
    
    # Example 5: List and manage dashboards
    logger.info("\n=== Managing dashboards ===")
    dashboards = powerbi_client.get_dashboards(group_id=workspace_id)
    
    for dashboard in dashboards:
        logger.info(f"Dashboard: {dashboard['displayName']} (ID: {dashboard['id']})")
    
    if dashboards:
        dashboard_id = dashboards[0]["id"]
        
        # Get dashboard tiles
        tiles = powerbi_client.get_dashboard_tiles(dashboard_id, group_id=workspace_id)
        logger.info(f"\nDashboard has {len(tiles)} tiles:")
        for tile in tiles:
            logger.info(f"  - {tile.get('title', 'Untitled')} (ID: {tile['id']})")
    
    # Example 6: Export a report (optional)
    # logger.info("\n=== Exporting report ===")
    # if reports:
    #     report_id = reports[0]["id"]
    #     with OperationLogger(logger, "report_export", report_id=report_id):
    #         pdf_content = powerbi_client.export_report(
    #             report_id=report_id,
    #             format="PDF",
    #             group_id=workspace_id
    #         )
    #         
    #         # Save to file
    #         with open(f"report_{report_id}.pdf", "wb") as f:
    #             f.write(pdf_content)
    #         logger.info(f"Report exported to report_{report_id}.pdf")


if __name__ == "__main__":
    main()
