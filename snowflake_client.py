# salesforce_snowflake_sync/snowflake_client.py

import datetime
import logging
import snowflake.connector
import requests
from config import APP_CONFIG
from utils import Utils
from retry import retry

logger = logging.getLogger('sf_snowflake_integration')

class SnowflakeClient:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.utils = Utils()
        self.retry_exceptions = (
            snowflake.connector.errors.OperationalError,
            snowflake.connector.errors.DatabaseError,
            requests.exceptions.RequestException
        )
        self.connect()

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (snowflake.connector.errors.OperationalError,
            snowflake.connector.errors.DatabaseError,
            requests.exceptions.RequestException))
    def connect(self):
        logger.info("Connecting to Snowflake...")
        self.conn = snowflake.connector.connect(
            user=self.config['user'],
            password=self.config['password'],
            account=self.config['account'],
            warehouse=self.config['warehouse'],
            database=self.config['database'],
            schema=self.config['schema'],
            role=self.config['role']
        )
        logger.info("Successfully connected to Snowflake.")

    def ensure_connection(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except Exception:
            logger.info("Reconnecting to Snowflake...")
            self.connect()

    @retry(APP_CONFIG['max_retries'], APP_CONFIG['retry_wait'],
           (snowflake.connector.errors.OperationalError,
            snowflake.connector.errors.DatabaseError,
            requests.exceptions.RequestException))
    def fetch_orders(self):
        self.ensure_connection()

        seven_days_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        query = f"""
        SELECT
            SALES_ORDER_NUMBER,
            CUSTOMER_NAME,
            CUSTOMER_ACCOUNT,
            CUSTOMER_PO_NUMBER,
            CUSTOMER_NUMBER,
            AR_DIVISION_NUMBER,
            SALES_ORDER_DATE,
            POSTING_DATE,
            INVOICE_NUMBER,
            GROSS_SALES,
            NET_SALES,
            ITEM_CODE,
            ITEM_CODE_DESC,
            QTY_ORDERED,
            QTY_SHIPPED,
            UNIT_PRICE,
            DISCOUNT,
            DEDUCTION,
            INVOICE_DETAIL_COMMENT
        FROM SALESFORCE_INTEGRATION.VW_SALES_ORDER_INVOICING_SUMMARY
        WHERE SALES_ORDER_DATE >= '{seven_days_ago}'
        AND SALES_ORDER_DATE IS NOT NULL
        """
        logger.info(f"Using filter for orders with sales order date of {seven_days_ago} or later (last 7 days).")

        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cols = [c[0] for c in cursor.description]
        orders_dict = {}

        for r in rows:
            record = dict(zip(cols, r))
            record['SALES_ORDER_DATE'] = self.utils.normalize_date(record.get('SALES_ORDER_DATE'))
            record['POSTING_DATE'] = self.utils.normalize_date(record.get('POSTING_DATE'))

            record['GROSS_SALES'] = self.utils.to_float_if_decimal(record.get('GROSS_SALES', 0.0))
            record['NET_SALES'] = self.utils.to_float_if_decimal(record.get('NET_SALES', 0.0))
            record['QTY_ORDERED'] = self.utils.to_float_if_decimal(record.get('QTY_ORDERED', 0))
            record['QTY_SHIPPED'] = self.utils.to_float_if_decimal(record.get('QTY_SHIPPED', 0))
            record['UNIT_PRICE'] = self.utils.to_float_if_decimal(record.get('UNIT_PRICE', 0.0))
            record['DISCOUNT'] = self.utils.to_float_if_decimal(record.get('DISCOUNT', 0.0))
            record['DEDUCTION'] = self.utils.to_float_if_decimal(record.get('DEDUCTION', 0.0))

            son = record['SALES_ORDER_NUMBER'] or ''

            if son not in orders_dict:
                orders_dict[son] = {
                    'SALES_ORDER_NUMBER': son,
                    'CUSTOMER_NAME': record.get('CUSTOMER_NAME', ''),
                    'CUSTOMER_ACCOUNT': record.get('CUSTOMER_ACCOUNT', ''),
                    'CUSTOMER_PO_NUMBER': record.get('CUSTOMER_PO_NUMBER', ''),
                    'CUSTOMER_NUMBER': record.get('CUSTOMER_NUMBER', ''),
                    'AR_DIVISION_NUMBER': record.get('AR_DIVISION_NUMBER', ''),
                    'SALES_ORDER_DATE': record.get('SALES_ORDER_DATE', ''),
                    'POSTING_DATE': record.get('POSTING_DATE', ''),
                    'INVOICE_NUMBER': record.get('INVOICE_NUMBER', ''),
                    'GROSS_SALES': record.get('GROSS_SALES', 0.0),
                    'NET_SALES': record.get('NET_SALES', 0.0),
                    'ITEMS': []
                }

            item_data = {
                'ITEM_CODE': record.get('ITEM_CODE', ''),
                'ITEM_CODE_DESC': record.get('ITEM_CODE_DESC', ''),
                'QTY_ORDERED': record.get('QTY_ORDERED', 0),
                'QTY_SHIPPED': record.get('QTY_SHIPPED', 0),
                'UNIT_PRICE': record.get('UNIT_PRICE', 0.0),
                'DISCOUNT': record.get('DISCOUNT', 0.0),
                'DEDUCTION': record.get('DEDUCTION', 0.0),
                'INVOICE_DETAIL_COMMENT': record.get('INVOICE_DETAIL_COMMENT', '')
            }
            orders_dict[son]['ITEMS'].append(item_data)

        logger.info(f"{len(orders_dict)} distinct orders loaded from Snowflake (including open and posted orders).")
        cursor.close()
        return orders_dict

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed.")
