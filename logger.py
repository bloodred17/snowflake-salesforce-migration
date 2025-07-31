# salesforce_snowflake_sync/logger.py

import logging
from config import APP_CONFIG

def configure_logger():
    logger = logging.getLogger('sf_snowflake_integration')
    logger.setLevel(getattr(logging, APP_CONFIG['log_level'], logging.INFO))

    # Clear any existing handlers
    if logger.handlers:
        logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File handler
    file_handler = logging.FileHandler(APP_CONFIG['log_file'])
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger