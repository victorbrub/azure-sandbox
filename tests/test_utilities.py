"""
Basic tests for Azure Sandbox utilities.

These tests verify the structure and basic functionality of the utilities.
"""

import pytest
from utils.config import ConfigManager, SecretsManager
from utils.logging import configure_logging, get_logger
import os


class TestConfigManager:
    """Tests for ConfigManager."""
    
    def test_config_manager_init(self):
        """Test ConfigManager initialization."""
        config = ConfigManager()
        assert config is not None
        assert isinstance(config.config, dict)
    
    def test_get_with_default(self):
        """Test getting config value with default."""
        config = ConfigManager()
        value = config.get("nonexistent.key", default="default_value")
        assert value == "default_value"
    
    def test_set_and_get(self):
        """Test setting and getting config values."""
        config = ConfigManager()
        config.set("test.key", "test_value")
        value = config.get("test.key")
        assert value == "test_value"
    
    def test_nested_config(self):
        """Test nested configuration access."""
        config = ConfigManager()
        config.set("level1.level2.level3", "nested_value")
        value = config.get("level1.level2.level3")
        assert value == "nested_value"
    
    def test_get_required_missing(self):
        """Test getting required config that doesn't exist."""
        config = ConfigManager()
        with pytest.raises(ValueError):
            config.get("missing.key", required=True)


class TestSecretsManager:
    """Tests for SecretsManager."""
    
    def test_secrets_manager_init(self):
        """Test SecretsManager initialization."""
        secrets = SecretsManager()
        assert secrets is not None
        assert isinstance(secrets.secrets, dict)
    
    def test_set_and_get_secret(self):
        """Test setting and getting secrets."""
        secrets = SecretsManager()
        secrets.set_secret("test_secret", "secret_value")
        value = secrets.get_secret("test_secret")
        assert value == "secret_value"
    
    def test_get_secret_from_env(self):
        """Test getting secret from environment variable."""
        os.environ["TEST_SECRET"] = "env_secret_value"
        secrets = SecretsManager()
        value = secrets.get_secret("test_secret")
        assert value == "env_secret_value"
        del os.environ["TEST_SECRET"]
    
    def test_get_required_secret_missing(self):
        """Test getting required secret that doesn't exist."""
        secrets = SecretsManager()
        with pytest.raises(ValueError):
            secrets.get_secret("missing_secret", required=True)
    
    def test_clear_secrets(self):
        """Test clearing all secrets."""
        secrets = SecretsManager()
        secrets.set_secret("test1", "value1")
        secrets.set_secret("test2", "value2")
        secrets.clear_secrets()
        assert len(secrets.secrets) == 0


class TestLogging:
    """Tests for logging utilities."""
    
    def test_configure_logging(self):
        """Test logging configuration."""
        configure_logging(log_level="INFO", structured=False)
        logger = get_logger(__name__, use_structlog=False)
        assert logger is not None
    
    def test_get_logger(self):
        """Test getting logger instance."""
        logger = get_logger("test_logger", use_structlog=False)
        assert logger is not None
        assert logger.name == "test_logger"


class TestUtilityImports:
    """Tests to verify all utility modules can be imported."""
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_data_factory(self):
        """Test importing Azure Data Factory utilities."""
        from utils.azure_data_factory import AzureDataFactoryClient
        assert AzureDataFactoryClient is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_databricks(self):
        """Test importing Azure Databricks utilities."""
        from utils.azure_databricks import AzureDatabricksClient
        assert AzureDatabricksClient is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_synapse(self):
        """Test importing Azure Synapse utilities."""
        from utils.azure_synapse import AzureSynapseClient
        assert AzureSynapseClient is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_storage(self):
        """Test importing Azure Storage utilities."""
        from utils.azure_storage import AzureBlobStorageClient, AzureDataLakeGen2Client
        assert AzureBlobStorageClient is not None
        assert AzureDataLakeGen2Client is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_sql(self):
        """Test importing Azure SQL utilities."""
        from utils.azure_sql import AzureSQLClient
        assert AzureSQLClient is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_eventhub(self):
        """Test importing Azure Event Hub utilities."""
        from utils.azure_eventhub import AzureEventHubProducer, AzureEventHubConsumer
        assert AzureEventHubProducer is not None
        assert AzureEventHubConsumer is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_azure_stream_analytics(self):
        """Test importing Azure Stream Analytics utilities."""
        from utils.azure_stream_analytics import AzureStreamAnalyticsClient
        assert AzureStreamAnalyticsClient is not None
    
    @pytest.mark.skipif(True, reason="Requires Azure SDK dependencies")
    def test_import_powerbi(self):
        """Test importing PowerBI utilities."""
        from utils.powerbi import PowerBIClient
        assert PowerBIClient is not None
    
    def test_import_config(self):
        """Test importing config utilities."""
        from utils.config import ConfigManager, SecretsManager
        assert ConfigManager is not None
        assert SecretsManager is not None
    
    def test_import_logging(self):
        """Test importing logging utilities."""
        from utils.logging import configure_logging, get_logger, OperationLogger
        assert configure_logging is not None
        assert get_logger is not None
        assert OperationLogger is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
