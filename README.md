# Azure Sandbox - Data Engineering Utilities

A comprehensive collection of utilities for Azure Data Engineering services, designed for testing and running data engineering projects before moving to a complete production environment. This repository provides easy-to-use Python utilities for all major Azure data services and PowerBI API integration.

## Features

### Azure Data Engineering Services

- **Azure Data Factory**: Pipeline management, trigger operations, monitoring, and activity tracking
- **Azure Databricks**: Cluster management, job execution, notebook operations, and workspace management
- **Azure Synapse Analytics**: SQL/Spark pool management, pipeline operations, and workspace control
- **Azure Blob Storage**: Container management, file upload/download, and blob operations
- **Azure Data Lake Storage Gen2**: File system operations, directory management, and hierarchical storage
- **Azure SQL Database**: Connection management, query execution, stored procedures, and bulk operations
- **Azure Event Hub**: Message producer/consumer, event streaming, and checkpoint management
- **Azure Stream Analytics**: Job management, input/output configuration, and monitoring

### PowerBI API Integration

- **Dataset Management**: Trigger refresh, monitor refresh history, cancel operations
- **Activity Events**: Retrieve organization activity logs (requires admin permissions)
- **Report Operations**: List, get, clone, and export reports
- **Dashboard Management**: Retrieve dashboards, get tiles, and manage content
- **Workspace Operations**: Create, list, and manage workspaces

### Additional Utilities

- **Configuration Management**: Environment variable and config file support (YAML/JSON)
- **Secrets Management**: Secure handling of credentials and secrets
- **Structured Logging**: Advanced logging with context, timing, and operation tracking

## Installation

### Prerequisites

- Python 3.8 or higher
- Azure subscription with appropriate permissions
- Azure services configured (Data Factory, Databricks, Synapse, etc.)
- PowerBI workspace (for PowerBI operations)

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Configuration

### Option 1: Environment Variables

Copy the example environment file and fill in your values:

```bash
cp .env.example .env
# Edit .env with your Azure credentials and service details
```

### Option 2: Configuration File

Copy the example config file and fill in your values:

```bash
cp config.example.yml config.yml
# Edit config.yml with your Azure credentials and service details
```

### Configuration Keys

#### Azure Core
- `AZURE_SUBSCRIPTION_ID`: Your Azure subscription ID
- `AZURE_TENANT_ID`: Your Azure AD tenant ID
- `AZURE_CLIENT_ID`: Service principal client ID (optional)
- `AZURE_CLIENT_SECRET`: Service principal client secret (optional)

#### Azure Data Factory
- `AZURE_DATA_FACTORY_RESOURCE_GROUP`: Resource group name
- `AZURE_DATA_FACTORY_NAME`: Data Factory name

#### Azure Databricks
- `AZURE_DATABRICKS_WORKSPACE_URL`: Databricks workspace URL
- `AZURE_DATABRICKS_TOKEN`: Personal access token (optional)

#### Azure Synapse
- `AZURE_SYNAPSE_RESOURCE_GROUP`: Resource group name
- `AZURE_SYNAPSE_WORKSPACE_NAME`: Synapse workspace name
- `AZURE_SYNAPSE_ENDPOINT`: Synapse workspace endpoint URL

#### Azure Storage
- `AZURE_STORAGE_ACCOUNT_NAME`: Storage account name
- `AZURE_STORAGE_CONNECTION_STRING`: Connection string (optional)

#### Azure SQL Database
- `AZURE_SQL_SERVER`: SQL Server FQDN
- `AZURE_SQL_DATABASE`: Database name
- `AZURE_SQL_USERNAME`: Username (optional if using Azure AD)
- `AZURE_SQL_PASSWORD`: Password (optional if using Azure AD)

#### Azure Event Hub
- `AZURE_EVENTHUB_NAMESPACE`: Event Hub namespace
- `AZURE_EVENTHUB_NAME`: Event Hub name
- `AZURE_EVENTHUB_CONNECTION_STRING`: Connection string (optional)

#### PowerBI
- `POWERBI_CLIENT_ID`: PowerBI application client ID
- `POWERBI_CLIENT_SECRET`: Application secret (for service principal)
- `POWERBI_TENANT_ID`: Azure AD tenant ID
- `POWERBI_USERNAME`: Username (for user authentication)
- `POWERBI_PASSWORD`: Password (for user authentication)

## Quick Start

### Azure Data Factory Example

```python
from utils.azure_data_factory import AzureDataFactoryClient
from utils.config import get_config_manager

# Load configuration
config = get_config_manager(env_file=".env")

# Initialize client
adf_client = AzureDataFactoryClient(
    subscription_id=config.get("azure.subscription_id"),
    resource_group=config.get("azure.data_factory.resource_group"),
    factory_name=config.get("azure.data_factory.factory_name")
)

# Run a pipeline
run_id = adf_client.create_pipeline_run(
    pipeline_name="MyPipeline",
    parameters={"date": "2024-01-01"}
)

# Monitor pipeline run
run_info = adf_client.get_pipeline_run(run_id)
print(f"Pipeline status: {run_info.status}")
```

### PowerBI API Example

```python
from utils.powerbi import PowerBIClient
from utils.config import get_config_manager

# Load configuration
config = get_config_manager(env_file=".env")
creds = config.get_powerbi_credentials()

# Initialize client
powerbi_client = PowerBIClient(
    client_id=creds["client_id"],
    client_secret=creds["client_secret"],
    tenant_id=creds["tenant_id"]
)

# Trigger dataset refresh
powerbi_client.refresh_dataset(
    dataset_id="your-dataset-id",
    notify_option="MailOnFailure"
)

# Get activity events
from datetime import datetime, timedelta
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=1)

events = powerbi_client.get_activity_events(
    start_datetime=start_time.isoformat() + "Z",
    end_datetime=end_time.isoformat() + "Z"
)
print(f"Retrieved {len(events)} activity events")
```

### Azure Storage Example

```python
from utils.azure_storage import AzureBlobStorageClient
from utils.config import get_config_manager

# Load configuration
config = get_config_manager(env_file=".env")

# Initialize client
blob_client = AzureBlobStorageClient(
    account_url=config.get_storage_account_url()
)

# Upload a file
blob_client.upload_file(
    container_name="my-container",
    file_path="/path/to/local/file.txt",
    blob_name="file.txt"
)

# Download a file
blob_client.download_file(
    container_name="my-container",
    blob_name="file.txt",
    file_path="/path/to/download/file.txt"
)
```

## Examples

Comprehensive examples are available in the `examples/` directory:

- `adf_example.py`: Azure Data Factory operations
- `databricks_example.py`: Azure Databricks cluster and job management
- `powerbi_example.py`: PowerBI API operations
- `storage_example.py`: Azure Storage and Data Lake operations

Run an example:

```bash
python examples/adf_example.py
```

## Project Structure

```
azure-sandbox/
├── utils/
│   ├── azure_data_factory/    # ADF utilities
│   ├── azure_databricks/       # Databricks utilities
│   ├── azure_synapse/          # Synapse utilities
│   ├── azure_storage/          # Blob Storage & Data Lake utilities
│   ├── azure_sql/              # SQL Database utilities
│   ├── azure_eventhub/         # Event Hub utilities
│   ├── azure_stream_analytics/ # Stream Analytics utilities
│   ├── powerbi/                # PowerBI API utilities
│   ├── config/                 # Configuration management
│   └── logging/                # Logging utilities
├── examples/                   # Usage examples
├── tests/                      # Unit tests
├── requirements.txt            # Python dependencies
├── config.example.yml          # Example config file
├── .env.example                # Example environment file
└── README.md                   # This file
```

## Authentication

This toolkit supports multiple authentication methods:

1. **DefaultAzureCredential** (Recommended): Automatically uses available credentials
   - Environment variables
   - Managed Identity
   - Azure CLI
   - Visual Studio Code
   - Azure PowerShell

2. **Service Principal**: Using client ID and secret

3. **User Authentication**: Using username and password (for development only)

## Logging

Configure structured logging for your operations:

```python
from utils.logging import configure_logging, get_logger, OperationLogger

# Configure logging
configure_logging(
    log_level="INFO",
    log_file="logs/app.log",
    structured=True,
    json_logs=False
)

# Get logger
logger = get_logger(__name__)

# Use operation logger for timing
with OperationLogger(logger, "data_processing", dataset="customers"):
    # Your code here
    pass
```

## Best Practices

1. **Never commit secrets**: Use `.env` files (already in `.gitignore`)
2. **Use managed identities**: When running in Azure environments
3. **Enable logging**: For monitoring and debugging
4. **Use resource tags**: For cost tracking and organization
5. **Implement retry logic**: For transient failures
6. **Monitor costs**: Azure services can incur costs when active
7. **Clean up resources**: Terminate clusters and pause SQL pools when not in use

## Security

- Never commit credentials or secrets to version control
- Use Azure Key Vault for production secrets
- Implement least privilege access principles
- Regularly rotate credentials and tokens
- Use managed identities when possible
- Enable Azure AD authentication over SQL authentication

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues, questions, or contributions, please open an issue on GitHub.

## Acknowledgments

Built with Azure SDK for Python and designed for data engineering teams testing and developing Azure data solutions.