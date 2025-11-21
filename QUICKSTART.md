# Quick Start Guide

This guide will help you get started with the Azure Sandbox utilities quickly.

## Prerequisites

1. Python 3.8 or higher
2. Azure subscription
3. Azure services configured (at least one of: Data Factory, Databricks, Synapse, Storage, SQL Database, Event Hub, Stream Analytics)
4. PowerBI workspace (optional, for PowerBI operations)

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/victorbrub/azure-sandbox.git
cd azure-sandbox
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

Note: The requirements.txt includes all Azure SDK dependencies. You can install only what you need:

```bash
# For Azure Data Factory only
pip install azure-identity azure-mgmt-datafactory

# For PowerBI only
pip install msal requests

# For configuration and logging
pip install python-dotenv pyyaml structlog
```

### Step 3: Configure Your Environment

#### Option A: Using Environment Variables (.env file)

```bash
cp .env.example .env
# Edit .env with your Azure credentials
```

Example `.env`:
```bash
AZURE_SUBSCRIPTION_ID=your-subscription-id
AZURE_TENANT_ID=your-tenant-id
AZURE_DATA_FACTORY_RESOURCE_GROUP=your-rg
AZURE_DATA_FACTORY_NAME=your-adf-name
```

#### Option B: Using Configuration File

```bash
cp config.example.yml config.yml
# Edit config.yml with your Azure credentials
```

## Basic Usage Examples

### Example 1: Azure Data Factory - Run a Pipeline

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
run_id = adf_client.create_pipeline_run("MyPipeline")
print(f"Pipeline run started: {run_id}")

# Check status
run_info = adf_client.get_pipeline_run(run_id)
print(f"Status: {run_info.status}")
```

### Example 2: PowerBI - Refresh a Dataset

```python
from utils.powerbi import PowerBIClient
from utils.config import get_config_manager

# Load configuration
config = get_config_manager(env_file=".env")
creds = config.get_powerbi_credentials()

# Initialize client
powerbi = PowerBIClient(
    client_id=creds["client_id"],
    client_secret=creds["client_secret"],
    tenant_id=creds["tenant_id"]
)

# Trigger dataset refresh
powerbi.refresh_dataset(
    dataset_id="your-dataset-id",
    notify_option="MailOnFailure"
)

# Check refresh history
history = powerbi.get_refresh_history("your-dataset-id", top=5)
for refresh in history:
    print(f"Status: {refresh['status']}, Time: {refresh['startTime']}")
```

### Example 3: Azure Storage - Upload/Download Files

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
    file_path="local_file.txt",
    blob_name="uploaded_file.txt"
)

# Download a file
blob_client.download_file(
    container_name="my-container",
    blob_name="uploaded_file.txt",
    file_path="downloaded_file.txt"
)
```

### Example 4: Azure Databricks - Run a Job

```python
from utils.azure_databricks import AzureDatabricksClient
from utils.config import get_config_manager

# Load configuration
config = get_config_manager(env_file=".env")

# Initialize client
databricks = AzureDatabricksClient(
    workspace_url=config.get("azure.databricks.workspace_url"),
    token=config.get("azure.databricks.token")
)

# List clusters
clusters = databricks.list_clusters()
for cluster in clusters:
    print(f"Cluster: {cluster.cluster_name}")

# Run a job
run_id = databricks.run_job(
    job_id=12345,
    parameters={"date": "2024-01-01"}
)
print(f"Job run started: {run_id}")
```

## Running Examples

The repository includes complete working examples in the `examples/` directory:

```bash
# Azure Data Factory example
python examples/adf_example.py

# PowerBI example
python examples/powerbi_example.py

# Azure Storage example
python examples/storage_example.py

# Azure Databricks example
python examples/databricks_example.py

# Azure SQL Database example
python examples/sql_example.py

# Azure Event Hub example
python examples/eventhub_example.py
```

## Authentication Methods

### 1. DefaultAzureCredential (Recommended)

This automatically uses available credentials in the following order:
- Environment variables
- Managed Identity (when running in Azure)
- Azure CLI
- Visual Studio Code
- Azure PowerShell

```python
# No additional configuration needed
# Just ensure you're logged in via Azure CLI:
az login
```

### 2. Service Principal

```bash
# Set in .env or config.yml
AZURE_CLIENT_ID=your-app-id
AZURE_CLIENT_SECRET=your-app-secret
AZURE_TENANT_ID=your-tenant-id
```

### 3. Connection Strings

For services like Storage and Event Hub:
```bash
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
```

## Common Use Cases

### Use Case 1: Data Pipeline Orchestration

Monitor and trigger data pipelines across Azure Data Factory and Databricks:

```python
from utils.azure_data_factory import AzureDataFactoryClient
from utils.azure_databricks import AzureDatabricksClient
from utils.logging import configure_logging, get_logger

configure_logging(log_level="INFO")
logger = get_logger(__name__)

# Run ADF pipeline
adf = AzureDataFactoryClient(...)
run_id = adf.create_pipeline_run("ETL_Pipeline")

# Trigger Databricks job
databricks = AzureDatabricksClient(...)
job_run = databricks.run_job(job_id=123)

logger.info(f"Pipeline: {run_id}, Databricks: {job_run}")
```

### Use Case 2: PowerBI Refresh Automation

Automate dataset refreshes after data processing:

```python
from utils.powerbi import PowerBIClient
from datetime import datetime

powerbi = PowerBIClient(...)

# Refresh all datasets in workspace
datasets = powerbi.get_datasets(group_id="workspace-id")
for dataset in datasets:
    powerbi.refresh_dataset(dataset["id"])
    print(f"Refreshed: {dataset['name']}")
```

### Use Case 3: Data Lake Operations

Manage files in Azure Data Lake Storage Gen2:

```python
from utils.azure_storage import AzureDataLakeGen2Client

datalake = AzureDataLakeGen2Client(...)

# Create directory structure
datalake.create_directory("filesystem", "bronze/data")
datalake.create_directory("filesystem", "silver/data")

# Upload processed files
datalake.upload_file(
    "filesystem",
    "silver/data/processed.parquet",
    "local_processed.parquet"
)
```

## Troubleshooting

### Issue: Authentication Errors

**Solution**: Ensure you're logged in via Azure CLI:
```bash
az login
az account set --subscription "your-subscription-id"
```

### Issue: Module Import Errors

**Solution**: Install all dependencies:
```bash
pip install -r requirements.txt
```

### Issue: Permission Errors

**Solution**: Ensure your account/service principal has the necessary permissions:
- Data Factory Contributor
- Storage Blob Data Contributor
- Databricks user/admin
- PowerBI Admin (for activity events)

## Next Steps

1. Explore the [full documentation](README.md)
2. Check out the [examples](examples/) directory
3. Review the [configuration guide](config.example.yml)
4. Run the test suite: `pytest tests/`

## Getting Help

- Check the README.md for detailed documentation
- Review example files for usage patterns
- Open an issue on GitHub for bugs or questions
- Consult Azure SDK documentation for specific services

## Best Practices

1. **Use environment variables** for credentials (never commit secrets)
2. **Enable structured logging** for better debugging
3. **Implement retry logic** for transient failures
4. **Monitor costs** - some services charge when active
5. **Clean up resources** - terminate clusters when not in use
6. **Use managed identities** in production environments
7. **Test in sandbox** before deploying to production
