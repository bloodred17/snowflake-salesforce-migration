# salesforce_snowflake_sync/retry.py

import functools
import time
import logging
from config import APP_CONFIG

logger = logging.getLogger('sf_snowflake_integration')

def retry(max_retries, wait_time, retry_exceptions):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"Failed after {max_retries} retries: {e}")
                        raise
                    logger.warning(f"Retry {retries}/{max_retries} after error: {e}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Unhandled exception in {func.__name__}: {e}")
                    raise
        return wrapper
    return decorator
