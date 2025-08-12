# salesforce_snowflake_sync/integration.py

import logging
import time
import requests
from config import APP_CONFIG
from utils import Utils

logger = logging.getLogger('sf_snowflake_integration')

class SalesforceSnowflakeIntegration:
    def __init__(self, snowflake_client, salesforce_client):
        self.snowflake_client = snowflake_client
        self.salesforce_client = salesforce_client
        self.utils = Utils()

    def process_orders(self, orders_dict):
        total_orders_processed = 0
        total_items_processed = 0
        total_orders_updated = 0
        total_items_updated = 0

        for order_number, order_data in orders_dict.items():
            if not order_number:
                logger.warning("Order with empty SALES_ORDER_NUMBER, skipping.")
                continue

            invoice_number = order_data.get('INVOICE_NUMBER', '')
            posting_date = order_data.get('POSTING_DATE', None)

            # Process both open and posted orders
            if not posting_date:
                logger.info(f"Processing open order {order_number} (no posting date yet)")

            customer_name = order_data.get('CUSTOMER_NAME', '').strip()
            customer_number = order_data.get('CUSTOMER_NUMBER', '').strip()
            ar_division_number = order_data.get('AR_DIVISION_NUMBER', '')

            if not customer_number or not ar_division_number:
                logger.warning(f"Missing CUSTOMER_NUMBER or AR_DIVISION_NUMBER for order {order_number}, skipping.")
                continue

            account_id, existing_account_name = self.salesforce_client.find_account_by_customer_data(customer_number, ar_division_number)

            if not account_id:
                account_id, existing_account_name = self.salesforce_client.check_existing_account_in_salesforce(customer_number, ar_division_number)

            if not account_id:
                new_account_data = {
                    'Name': customer_name,
                    'Region__c': 'None',
                    'Customer_Type__c': 'Warehouse Distributor',
                    'Description': 'Script-created',
                    'AR_Div_Number__c': ar_division_number,
                    'LOP_Customer_Number__c': customer_number
                }
                account_id = self.salesforce_client.safely_create_salesforce('Account', new_account_data)
                if account_id:
                    logger.info(f"Created new Account Name='{customer_name}', ID={account_id}")
                    key = f"{customer_number.strip()}|{ar_division_number.strip()}"
                    self.salesforce_client.accounts_by_lop[key] = {
                        'Id': account_id,
                        'Name': customer_name
                    }
                else:
                    logger.error(f"Failed to create Account for {customer_name}, skipping order {order_number}")
                    continue
            else:
                logger.info(f"Using existing Account {account_id} (Name='{existing_account_name}')")

            sales_order_date = order_data.get('SALES_ORDER_DATE', None)
            customer_po_number = order_data.get('CUSTOMER_PO_NUMBER', '')

            existing_order_id = self.salesforce_client.check_existing_sales_order_by_invoice(invoice_number, order_number)

            if not existing_order_id:
                # Create new order
                order_type = "posted" if posting_date else "open"
                logger.info(f"Creating new {order_type} order {order_number} (Invoice: {invoice_number or 'None'})")
                so_data = {
                    'Name': f"{customer_name} - {order_number}",
                    'Sales_Order_Number__c': order_number,
                    'Account_Name__c': account_id,
                    'Sales_Order_Date__c': sales_order_date,
                    'Posting_Date__c': posting_date,
                    'Invoice_Number__c': invoice_number,
                    'Order_Type__c': 'Performance',
                    'Customer_Purchase_Order_Number__c': customer_po_number,
                    'Account_ID__c': customer_number,
                    'Order_Status__c': "Closed" if posting_date else "Open"
                }
                sales_order_id = self.salesforce_client.safely_create_salesforce('Sales_Order__c', so_data)
                if not sales_order_id:
                    logger.error(f"Failed to create Sales_Order__c for invoice {invoice_number}, skipping items.")
                    continue
                logger.info(f"Created new Sales Order {sales_order_id} (Invoice: {invoice_number}, Number: {order_number})")
                total_orders_processed += 1
            else:
                # Update existing order with current information
                sales_order_id = existing_order_id
                order_type = "posted" if posting_date else "open"
                logger.info(f"Updating existing {order_type} order {order_number} (Invoice: {invoice_number or 'None'})")
                update_data = {
                    'Posting_Date__c': posting_date,
                    'Order_Status__c': "Closed" if posting_date else "Open",
                    'Customer_Purchase_Order_Number__c': customer_po_number
                }
                result = self.salesforce_client.safely_update_salesforce('Sales_Order__c', existing_order_id, update_data)
                if result is not None:
                    logger.info(f"Updated existing Sales Order {existing_order_id} (Invoice: {invoice_number}, Number: {order_number})")
                    total_orders_updated += 1
                else:
                    logger.error(f"Failed to update Sales Order {existing_order_id}")

            items = order_data.get('ITEMS', [])
            if not items:
                logger.info(f"No items for invoice {invoice_number}")
                continue

            logger.info(f"Processing {len(items)} items for invoice {invoice_number}")
            for item in items:
                product_code = item.get('ITEM_CODE', '')
                if not product_code:
                    logger.warning(f"Item with no PRODUCT_CODE for invoice {invoice_number}, skipping.")
                    continue

                existing_item_id = self.salesforce_client.check_existing_sales_order_item(sales_order_id, product_code)

                if not existing_item_id:
                    # Create new item
                    new_item_data = {
                        'Product_Code__c': product_code,
                        'Product_Description__c': item.get('ITEM_CODE_DESC', ''),
                        'Quantity_Ordered__c': self.utils.to_float_if_decimal(item.get('QTY_ORDERED', 0)),
                        'Quantity_Shipped__c': self.utils.to_float_if_decimal(item.get('QTY_SHIPPED', 0)),
                        'Unit_Price__c': self.utils.to_float_if_decimal(item.get('UNIT_PRICE', 0.0)),
                        'Discount_Dollars__c': self.utils.to_float_if_decimal(item.get('DISCOUNT', 0.0)),
                        'Deduction_Dollars__c': self.utils.to_float_if_decimal(item.get('DEDUCTION', 0.0)),
                        'LOP_Order_Comments__c': item.get('INVOICE_DETAIL_COMMENT', ''),
                        'Sales_Order_Number__c': sales_order_id,
                        'Name': 'TempName'
                    }

                    item_id = self.salesforce_client.safely_create_salesforce('Sales_Order_Item__c', new_item_data)
                    if item_id:
                        logger.info(f"Created new item {item_id} (Product: {product_code}) for invoice {invoice_number}")
                        total_items_processed += 1
                    else:
                        logger.error(f"Failed to create item {product_code} for invoice {invoice_number}")
                else:
                    # Update existing item with current information
                    update_item_data = {
                        'Quantity_Shipped__c': self.utils.to_float_if_decimal(item.get('QTY_SHIPPED', 0)),
                        'Unit_Price__c': self.utils.to_float_if_decimal(item.get('UNIT_PRICE', 0.0)),
                        'Discount_Dollars__c': self.utils.to_float_if_decimal(item.get('DISCOUNT', 0.0)),
                        'Deduction_Dollars__c': self.utils.to_float_if_decimal(item.get('DEDUCTION', 0.0)),
                        'LOP_Order_Comments__c': item.get('INVOICE_DETAIL_COMMENT', '')
                    }
                    result = self.salesforce_client.safely_update_salesforce('Sales_Order_Item__c', existing_item_id, update_item_data)
                    if result is not None:
                        logger.info(f"Updated existing item {existing_item_id} (Product: {product_code}) for invoice {invoice_number}")
                        total_items_updated += 1
                    else:
                        logger.error(f"Failed to update item {existing_item_id} (Product: {product_code})")

        logger.info(
            f"Processing completed: {total_orders_processed} new orders, {total_orders_updated} updated orders, "
            f"{total_items_processed} new items, and {total_items_updated} updated items."
        )
        return total_orders_processed, total_items_processed, total_orders_updated, total_items_updated

    def run_integration_cycle(self):
        try:
            start_time = time.time()
            self.salesforce_client.fetch_accounts()
            orders_dict = self.snowflake_client.fetch_orders()
            total_orders, total_items, total_orders_updated, total_items_updated = self.process_orders(orders_dict)
            elapsed_time = time.time() - start_time
            logger.info(
                f"Processing cycle completed in {elapsed_time:.2f} seconds. "
                f"Created {total_orders} new orders, updated {total_orders_updated} existing orders, "
                f"created {total_items} new items, and updated {total_items_updated} existing items. "
                f"Filter: Sales Order Date OR Posting Date in last 7 days."
            )
            return total_orders, total_items, total_orders_updated, total_items_updated
        except Exception as e:
            logger.error(f"Error in processing cycle: {e}")
            return 0, 0, 0, 0

    def run(self):
        try:
            logger.info("Starting Salesforce-Snowflake integration service (CREATE/UPDATE MODE - Sales Order Date OR Posting Date in Last 7 Days)")
            while True:
                try:
                    self.run_integration_cycle()
                    cycle_wait = APP_CONFIG.get('cycle_wait', 300)
                    logger.info(f"Waiting {cycle_wait} seconds for the next execution.")
                    time.sleep(cycle_wait)
                except Exception as e:
                    logger.error(f"Error in processing cycle: {e}")
                    time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Integration service stopped by user.")
        # finally:
        #     self.cleanup()

    def cleanup(self):
        self.snowflake_client.close()
        self.salesforce_client.close()
