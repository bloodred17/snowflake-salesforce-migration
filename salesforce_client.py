# salesforce_snowflake_sync/salesforce_client.py

import logging
import time
import requests
from simple_salesforce import Salesforce, SalesforceMalformedRequest
from config import APP_CONFIG
from utils import Utils
from retry import retry

logger = logging.getLogger('sf_snowflake_integration')

class SalesforceClient:
    def __init__(self, config):
        self.config = config
        self.sf = None
        self.utils = Utils()
        self.accounts_by_lop = {}
        self.retry_exceptions = (
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError
        )
        self.connect()

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError))
    def connect(self):
        logger.info("Connecting to Salesforce...")
        self.sf = Salesforce(
            username=self.config['username'],
            password=self.config['password'],
            security_token=self.config['security_token'],
            domain=self.config['domain']
        )
        logger.info("Successfully connected to Salesforce.")

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError))
    def fetch_accounts(self):
        query = "SELECT Id, Name, LOP_Customer_Number__c, AR_Div_Number__c FROM Account WHERE IsDeleted = FALSE"
        accounts = self.sf.query_all(query)['records']

        for acc in accounts:
            lop = acc.get('LOP_Customer_Number__c', '')
            ar_div = acc.get('AR_Div_Number__c', '')
            if lop:
                key = f"{lop.strip()}|{ar_div.strip() if ar_div else ''}"
                self.accounts_by_lop[key] = {
                    'Id': acc['Id'],
                    'Name': acc.get('Name', '')
                }

        logger.info(f"{len(accounts)} accounts loaded from Salesforce.")
        return self.accounts_by_lop

    def find_account_by_customer_data(self, customer_number, ar_division_number):
        if not customer_number or not ar_division_number:
            return None, None
        key = f"{customer_number.strip()}|{ar_division_number.strip()}"
        return (self.accounts_by_lop.get(key, {}).get('Id'),
                self.accounts_by_lop.get(key, {}).get('Name')) if key in self.accounts_by_lop else (None, None)

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError))
    def check_existing_account_in_salesforce(self, customer_number, ar_division_number):
        if not customer_number or not ar_division_number:
            return None, None

        customer_number = customer_number.strip()
        ar_division_number = ar_division_number.strip()

        query = (f"SELECT Id, Name FROM Account WHERE LOP_Customer_Number__c = '{customer_number}' "
                 f"AND AR_Div_Number__c = '{ar_division_number}' LIMIT 1")

        try:
            result = self.sf.query_all(query)['records']
            if result:
                account_id = result[0]['Id']
                account_name = result[0].get('Name', '')
                key = f"{customer_number}|{ar_division_number}"
                self.accounts_by_lop[key] = {
                    'Id': account_id,
                    'Name': account_name
                }
                return account_id, account_name
            return None, None
        except Exception as e:
            logger.error(f"Error checking for existing account: {e}")
            return None, None

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError))
    def check_existing_sales_order_by_invoice(self, invoice_number, order_number=None):
        if not invoice_number:
            # For open orders without invoice numbers, check by Sales_Order_Number__c
            if order_number:
                so_query = f"SELECT Id FROM Sales_Order__c WHERE Sales_Order_Number__c = '{order_number}' LIMIT 1"
                logger.info(f"Checking for existing Sales Order with Sales_Order_Number__c = '{order_number}' (open order)")
                result = self.sf.query_all(so_query)['records']
                return result[0]['Id'] if result else None
            return None
        
        so_query = f"SELECT Id FROM Sales_Order__c WHERE Invoice_Number__c = '{invoice_number}' LIMIT 1"
        logger.info(f"Checking for existing Sales Order with Invoice_Number__c = '{invoice_number}'")
        result = self.sf.query_all(so_query)['records']
        return result[0]['Id'] if result else None

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError))
    def check_existing_sales_order_by_number(self, order_number):
        """Check for existing sales order by Sales_Order_Number__c field"""
        if not order_number:
            return None
        so_query = f"SELECT Id FROM Sales_Order__c WHERE Sales_Order_Number__c = '{order_number}' LIMIT 1"
        logger.info(f"Checking for existing Sales Order with Sales_Order_Number__c = '{order_number}'")
        result = self.sf.query_all(so_query)['records']
        return result[0]['Id'] if result else None

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError))
    def check_existing_sales_order_item(self, parent_id, product_code):
        soi_query = (
            "SELECT Id FROM Sales_Order_Item__c "
            f"WHERE Sales_Order_Number__c = '{parent_id}' AND Product_Code__c = '{product_code}' LIMIT 1"
        )
        result = self.sf.query_all(soi_query)['records']
        return result[0]['Id'] if result else None

    def safely_create_salesforce(self, object_name, data):
        retries = 0
        max_retries = APP_CONFIG['max_retries']
        retry_wait = APP_CONFIG['retry_wait']
        sf_object = getattr(self.sf, object_name)

        while retries < max_retries:
            try:
                result = sf_object.create(data)
                return result['id'] if result.get('id') else None
            except SalesforceMalformedRequest as e:
                logger.error(f"Salesforce error (create {object_name}): {e.content}")
                return None
            except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                retries += 1
                if retries < max_retries:
                    logger.error(f"Connection error (create {object_name}): {e}. Retry {retries}/{max_retries} in {retry_wait} seconds...")
                    time.sleep(retry_wait)
                else:
                    logger.error(f"Failed to create {object_name} after {max_retries} retries: {e}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error (create {object_name}): {e}")
                return None

    def safely_update_salesforce(self, object_name, record_id, data):
        retries = 0
        max_retries = APP_CONFIG['max_retries']
        retry_wait = APP_CONFIG['retry_wait']
        sf_object = getattr(self.sf, object_name)

        while retries < max_retries:
            try:
                result = sf_object.update(record_id, data)
                return result if result else None
            except SalesforceMalformedRequest as e:
                logger.error(f"Salesforce error (update {object_name}): {e.content}")
                return None
            except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                retries += 1
                if retries < max_retries:
                    logger.error(f"Connection error (update {object_name}): {e}. Retry {retries}/{max_retries} in {retry_wait} seconds...")
                    time.sleep(retry_wait)
                else:
                    logger.error(f"Failed to update {object_name} after {max_retries} retries: {e}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error (update {object_name}): {e}")
                return None

    def close(self):
        logger.info("Salesforce session ended.")
