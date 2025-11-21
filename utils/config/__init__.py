"""Configuration Management Utilities

Provides utilities for managing configuration, environment variables,
and secrets for Azure data engineering projects.
"""

import os
from typing import Dict, Any, Optional
from pathlib import Path
import yaml
import json
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manager for application configuration and environment variables."""
    
    def __init__(self, config_file: Optional[str] = None, env_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Optional path to YAML/JSON config file
            env_file: Optional path to .env file
        """
        self.config = {}
        
        # Load from .env file
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)
            logger.info(f"Loaded environment from: {env_file}")
        else:
            load_dotenv()  # Load from default .env if exists
        
        # Load from config file
        if config_file and os.path.exists(config_file):
            self.config = self._load_config_file(config_file)
            logger.info(f"Loaded configuration from: {config_file}")
    
    def _load_config_file(self, config_file: str) -> Dict[str, Any]:
        """
        Load configuration from YAML or JSON file.
        
        Args:
            config_file: Path to config file
            
        Returns:
            Configuration dictionary
        """
        try:
            with open(config_file, 'r') as f:
                if config_file.endswith(('.yml', '.yaml')):
                    return yaml.safe_load(f) or {}
                elif config_file.endswith('.json'):
                    return json.load(f) or {}
                else:
                    raise ValueError(f"Unsupported config file format: {config_file}")
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            raise
    
    def get(self, key: str, default: Any = None, required: bool = False) -> Any:
        """
        Get a configuration value.
        Checks in order: environment variables, config file.
        
        Args:
            key: Configuration key (supports dot notation for nested keys)
            default: Default value if key not found
            required: If True, raises exception if key not found
            
        Returns:
            Configuration value
        """
        # Check environment variable first
        env_value = os.getenv(key.upper().replace('.', '_'))
        if env_value is not None:
            return env_value
        
        # Check config file (supports dot notation)
        value = self._get_nested(self.config, key.split('.'))
        
        if value is None:
            if required:
                raise ValueError(f"Required configuration key not found: {key}")
            return default
        
        return value
    
    def _get_nested(self, data: Dict[str, Any], keys: list) -> Any:
        """
        Get nested dictionary value using list of keys.
        
        Args:
            data: Dictionary to search
            keys: List of keys representing path
            
        Returns:
            Value at nested path or None
        """
        if not keys:
            return data
        
        key = keys[0]
        if not isinstance(data, dict) or key not in data:
            return None
        
        if len(keys) == 1:
            return data[key]
        
        return self._get_nested(data[key], keys[1:])
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value in the config dictionary.
        
        Args:
            key: Configuration key (supports dot notation)
            value: Value to set
        """
        keys = key.split('.')
        data = self.config
        
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        
        data[keys[-1]] = value
        logger.debug(f"Set config: {key} = {value}")
    
    def get_azure_credentials(self) -> Dict[str, str]:
        """
        Get Azure credentials from configuration.
        
        Returns:
            Dictionary with Azure credential keys
        """
        return {
            "subscription_id": self.get("azure.subscription_id", required=True),
            "tenant_id": self.get("azure.tenant_id", required=True),
            "client_id": self.get("azure.client_id"),
            "client_secret": self.get("azure.client_secret"),
        }
    
    def get_storage_account_url(self, account_name: Optional[str] = None) -> str:
        """
        Get Azure Storage account URL.
        
        Args:
            account_name: Optional storage account name (uses config if not provided)
            
        Returns:
            Storage account URL
        """
        account_name = account_name or self.get("azure.storage.account_name", required=True)
        return f"https://{account_name}.blob.core.windows.net"
    
    def get_datalake_account_url(self, account_name: Optional[str] = None) -> str:
        """
        Get Azure Data Lake Storage Gen2 account URL.
        
        Args:
            account_name: Optional storage account name (uses config if not provided)
            
        Returns:
            Data Lake account URL
        """
        account_name = account_name or self.get("azure.storage.account_name", required=True)
        return f"https://{account_name}.dfs.core.windows.net"
    
    def get_sql_connection_params(self) -> Dict[str, str]:
        """
        Get Azure SQL Database connection parameters.
        
        Returns:
            Dictionary with SQL connection parameters
        """
        return {
            "server": self.get("azure.sql.server", required=True),
            "database": self.get("azure.sql.database", required=True),
            "username": self.get("azure.sql.username"),
            "password": self.get("azure.sql.password"),
        }
    
    def get_powerbi_credentials(self) -> Dict[str, str]:
        """
        Get PowerBI API credentials.
        
        Returns:
            Dictionary with PowerBI credential keys
        """
        return {
            "client_id": self.get("powerbi.client_id", required=True),
            "client_secret": self.get("powerbi.client_secret"),
            "tenant_id": self.get("powerbi.tenant_id"),
            "username": self.get("powerbi.username"),
            "password": self.get("powerbi.password"),
        }
    
    def save_config(self, output_file: str) -> None:
        """
        Save current configuration to a file.
        
        Args:
            output_file: Path to output file (YAML or JSON)
        """
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w') as f:
                if output_file.endswith(('.yml', '.yaml')):
                    yaml.dump(self.config, f, default_flow_style=False)
                elif output_file.endswith('.json'):
                    json.dump(self.config, f, indent=2)
                else:
                    raise ValueError(f"Unsupported output file format: {output_file}")
            
            logger.info(f"Saved configuration to: {output_file}")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            raise


class SecretsManager:
    """Manager for handling secrets securely."""
    
    def __init__(self):
        """Initialize secrets manager."""
        self.secrets = {}
        logger.info("Initialized secrets manager")
    
    def set_secret(self, key: str, value: str) -> None:
        """
        Store a secret value.
        
        Args:
            key: Secret key
            value: Secret value
        """
        self.secrets[key] = value
        logger.debug(f"Set secret: {key}")
    
    def get_secret(self, key: str, required: bool = False) -> Optional[str]:
        """
        Retrieve a secret value.
        First checks in-memory secrets, then environment variables.
        
        Args:
            key: Secret key
            required: If True, raises exception if not found
            
        Returns:
            Secret value or None
        """
        # Check in-memory secrets first
        if key in self.secrets:
            return self.secrets[key]
        
        # Check environment variables
        env_key = key.upper().replace('.', '_')
        value = os.getenv(env_key)
        
        if value is None and required:
            raise ValueError(f"Required secret not found: {key}")
        
        return value
    
    def clear_secrets(self) -> None:
        """Clear all stored secrets from memory."""
        self.secrets.clear()
        logger.info("Cleared all secrets from memory")


# Singleton instances
_config_manager = None
_secrets_manager = None


def get_config_manager(config_file: Optional[str] = None, 
                      env_file: Optional[str] = None) -> ConfigManager:
    """
    Get or create the global ConfigManager instance.
    
    Args:
        config_file: Optional path to config file (only used on first call)
        env_file: Optional path to .env file (only used on first call)
        
    Returns:
        ConfigManager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_file=config_file, env_file=env_file)
    return _config_manager


def get_secrets_manager() -> SecretsManager:
    """
    Get or create the global SecretsManager instance.
    
    Returns:
        SecretsManager instance
    """
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager
