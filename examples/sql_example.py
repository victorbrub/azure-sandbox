"""
Example: Azure SQL Database Operations

This example demonstrates how to use Azure SQL Database utilities
to execute queries, manage tables, and perform bulk operations.
"""

from utils.azure_sql import AzureSQLClient
from utils.config import get_config_manager
from utils.logging import configure_logging, get_logger, OperationLogger

# Configure logging
configure_logging(log_level="INFO")
logger = get_logger(__name__)

# Load configuration
config = get_config_manager(config_file="config.yml", env_file=".env")


def main():
    """Main example function."""
    
    # Initialize SQL client
    sql_params = config.get_sql_connection_params()
    
    sql_client = AzureSQLClient(
        server=sql_params["server"],
        database=sql_params["database"],
        username=sql_params.get("username"),
        password=sql_params.get("password"),
        use_azure_ad=not sql_params.get("username")  # Use Azure AD if no username
    )
    
    # Example 1: List all tables
    logger.info("=== Listing all tables ===")
    tables = sql_client.list_tables()
    for table in tables:
        logger.info(f"Table: {table}")
    
    # Example 2: Execute a simple query
    logger.info("\n=== Executing query ===")
    query = "SELECT TOP 10 * FROM YourTable"  # Replace with your table
    
    with OperationLogger(logger, "query_execution", query=query):
        results = sql_client.execute_query(query)
        logger.info(f"Retrieved {len(results)} rows")
        for row in results:
            logger.info(f"  {row}")
    
    # Example 3: Execute query with parameters
    logger.info("\n=== Executing parameterized query ===")
    query = "SELECT * FROM YourTable WHERE column1 = :value"
    params = {"value": "some_value"}
    
    results = sql_client.execute_query(query, params)
    logger.info(f"Retrieved {len(results)} rows")
    
    # Example 4: Insert data
    logger.info("\n=== Inserting data ===")
    insert_query = """
        INSERT INTO YourTable (column1, column2, column3)
        VALUES (:col1, :col2, :col3)
    """
    insert_params = {
        "col1": "value1",
        "col2": "value2",
        "col3": 123
    }
    
    with OperationLogger(logger, "data_insert"):
        rows_affected = sql_client.execute_non_query(insert_query, insert_params)
        logger.info(f"Inserted {rows_affected} rows")
    
    # Example 5: Bulk insert
    logger.info("\n=== Bulk insert ===")
    data = [
        {"column1": "value1", "column2": "data1", "column3": 1},
        {"column1": "value2", "column2": "data2", "column3": 2},
        {"column1": "value3", "column2": "data3", "column3": 3},
    ]
    
    with OperationLogger(logger, "bulk_insert", rows=len(data)):
        sql_client.bulk_insert("YourTable", data)
        logger.info(f"Bulk inserted {len(data)} rows")
    
    # Example 6: Get table schema
    logger.info("\n=== Getting table schema ===")
    schema = sql_client.get_table_schema("YourTable")
    logger.info("Table schema:")
    for column in schema:
        logger.info(f"  {column['COLUMN_NAME']}: {column['DATA_TYPE']}")
    
    # Example 7: Check if table exists
    logger.info("\n=== Checking table existence ===")
    exists = sql_client.table_exists("YourTable")
    logger.info(f"Table exists: {exists}")
    
    # Example 8: Execute stored procedure
    logger.info("\n=== Executing stored procedure ===")
    try:
        results = sql_client.execute_stored_procedure(
            "YourStoredProcedure",
            params=["param1_value", "param2_value"]
        )
        logger.info(f"Stored procedure returned {len(results)} rows")
    except Exception as e:
        logger.warning(f"Stored procedure execution skipped: {e}")
    
    # Close connection
    sql_client.close()
    logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    main()
