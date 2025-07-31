# salesforce_snowflake_sync/__main__.py

from config import SF_CONFIG, SNOWFLAKE_CONFIG
from logger import configure_logger
from snowflake_client import SnowflakeClient
from salesforce_client import SalesforceClient
from integration import SalesforceSnowflakeIntegration

logger = configure_logger()

def main():
    try:
        logger.info("Starting Salesforce-Snowflake integration script (UPDATE MODE - Last 7 Days)...")
        snowflake_client = SnowflakeClient(SNOWFLAKE_CONFIG)
        salesforce_client = SalesforceClient(SF_CONFIG)
        integration = SalesforceSnowflakeIntegration(snowflake_client, salesforce_client)
        integration.run()
    except Exception as e:
        logger.critical(f"Critical error in main(): {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)