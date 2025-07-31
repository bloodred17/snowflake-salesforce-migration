import os

# Salesforce Configuration - Load from environment variables
SF_CONFIG = {
    'username': os.getenv('SF_USERNAME', "madhur@lucasoil.com.snowflake"),
    'password': os.getenv('SF_PASSWORD', "sK8HQcsy4YufcEu"),
    'security_token': os.getenv('SF_SECURITY_TOKEN', "EmUQ9BqUG7zYFWcK4b4t87ii"),
    'domain': os.getenv('SF_DOMAIN', 'test')
}

# Snowflake Configuration - Load from environment variables
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER', "JEFF_DAVIS"),
    'password': os.getenv('SNOWFLAKE_PASSWORD', "Bumz$4nutterBarz"),
    'account': os.getenv('SNOWFLAKE_ACCOUNT', "ZW93400-LUCAS_OIL_ACCT"),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', "SALESFORCE_INTEGRATION_WH"),
    'database': os.getenv('SNOWFLAKE_DATABASE', "LUCAS_OIL"),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', "SALESFORCE_INTEGRATION"),
    'role': os.getenv('SNOWFLAKE_ROLE', "SALESFORCE_INTEGRATION_READER")
}

# Application Configuration - Load from environment variables
APP_CONFIG = {
    'max_retries': int(os.getenv('APP_MAX_RETRIES', '99')),
    'retry_wait': int(os.getenv('APP_RETRY_WAIT', '5')),
    'cycle_wait': int(os.getenv('APP_CYCLE_WAIT', '1000')),
    'log_level': os.getenv('APP_LOG_LEVEL', 'INFO'),
    'log_file': os.getenv('APP_LOG_FILE', 'sf_snowflake_integration.log'),
    'LOAD_METHOD': os.getenv('APP_LOAD_METHOD', 'last_7_days'),
}
