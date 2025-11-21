"""Azure SQL Database Utilities

Provides utilities for connecting to and managing Azure SQL Database,
executing queries, and managing database operations.
"""

import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text
from typing import List, Dict, Optional, Any, Tuple
import logging
import urllib

logger = logging.getLogger(__name__)


class AzureSQLClient:
    """Client for interacting with Azure SQL Database."""
    
    def __init__(self, server: str, database: str, username: Optional[str] = None,
                 password: Optional[str] = None, use_azure_ad: bool = False):
        """
        Initialize Azure SQL Database client.
        
        Args:
            server: SQL Server name (e.g., myserver.database.windows.net)
            database: Database name
            username: SQL authentication username (optional if using Azure AD)
            password: SQL authentication password (optional if using Azure AD)
            use_azure_ad: Use Azure AD authentication instead of SQL auth
        """
        self.server = server
        self.database = database
        self.username = username
        self.use_azure_ad = use_azure_ad
        
        if use_azure_ad:
            self.connection_string = self._build_azure_ad_connection_string()
        else:
            self.connection_string = self._build_sql_auth_connection_string(password)
        
        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(self.connection_string)}"
        )
        
        logger.info(f"Initialized Azure SQL client for database: {database}")
    
    def _build_sql_auth_connection_string(self, password: str) -> str:
        """Build connection string for SQL authentication."""
        return (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{self.server},1433;"
            f"Database={self.database};"
            f"Uid={self.username};"
            f"Pwd={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
    
    def _build_azure_ad_connection_string(self) -> str:
        """Build connection string for Azure AD authentication."""
        return (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{self.server},1433;"
            f"Database={self.database};"
            f"Authentication=ActiveDirectoryInteractive;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                rows = [dict(row._mapping) for row in result]
                logger.info(f"Executed query, returned {len(rows)} rows")
                return rows
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Number of rows affected
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                connection.commit()
                rows_affected = result.rowcount
                logger.info(f"Executed non-query, {rows_affected} rows affected")
                return rows_affected
        except Exception as e:
            logger.error(f"Failed to execute non-query: {e}")
            raise
    
    def execute_stored_procedure(self, procedure_name: str, 
                                 params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a stored procedure.
        
        Args:
            procedure_name: Name of the stored procedure
            params: Optional list of parameters
            
        Returns:
            List of dictionaries representing result rows
        """
        try:
            param_placeholders = ",".join(["?" for _ in (params or [])])
            query = f"EXEC {procedure_name} {param_placeholders}"
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or [])
                rows = [dict(row._mapping) for row in result]
                logger.info(f"Executed stored procedure: {procedure_name}, returned {len(rows)} rows")
                return rows
        except Exception as e:
            logger.error(f"Failed to execute stored procedure: {e}")
            raise
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Perform a bulk insert operation.
        
        Args:
            table_name: Name of the table
            data: List of dictionaries representing rows to insert
        """
        try:
            if not data:
                logger.warning("No data to insert")
                return
            
            with self.engine.connect() as connection:
                columns = list(data[0].keys())
                placeholders = ",".join([f":{col}" for col in columns])
                columns_str = ",".join(columns)
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                connection.execute(text(query), data)
                connection.commit()
                logger.info(f"Bulk inserted {len(data)} rows into table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to bulk insert: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the schema of a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information dictionaries
        """
        try:
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
            """
            schema = self.execute_query(query, {"table_name": table_name})
            logger.info(f"Retrieved schema for table: {table_name}")
            return schema
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            raise
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
        """
        try:
            query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            result = self.execute_query(query)
            tables = [row['TABLE_NAME'] for row in result]
            logger.info(f"Retrieved {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            query = """
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = :table_name
            """
            result = self.execute_query(query, {"table_name": table_name})
            exists = result[0]['count'] > 0
            logger.info(f"Table {table_name} exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            raise
    
    def create_table_from_dataframe(self, df: Any, table_name: str, 
                                   if_exists: str = 'fail') -> None:
        """
        Create a table from a pandas DataFrame.
        
        Args:
            df: Pandas DataFrame
            table_name: Name of the table to create
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"Created table {table_name} from DataFrame with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to create table from DataFrame: {e}")
            raise
    
    def close(self) -> None:
        """Close the database connection."""
        try:
            self.engine.dispose()
            logger.info("Closed database connection")
        except Exception as e:
            logger.error(f"Failed to close connection: {e}")
            raise
