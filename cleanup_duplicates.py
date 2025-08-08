#!/usr/bin/env python3
"""
Cleanup script to identify and optionally remove duplicate orders.
This script helps clean up the duplicate orders that were created before the fix.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import SF_CONFIG
from salesforce_client import SalesforceClient
from logger import configure_logger
from simple_salesforce import Salesforce

def find_duplicate_orders():
    """Find duplicate orders in Salesforce"""
    logger = configure_logger()
    
    try:
        # Initialize Salesforce client
        sf_client = SalesforceClient(SF_CONFIG)
        
        # Query to find duplicate orders
        query = """
        SELECT Id, Name, Sales_Order_Number__c, Invoice_Number__c, CreatedDate
        FROM Sales_Order__c 
        WHERE Sales_Order_Number__c IS NOT NULL
        ORDER BY Sales_Order_Number__c, CreatedDate
        """
        
        logger.info("Searching for duplicate orders...")
        result = sf_client.sf.query_all(query)
        
        # Group by Sales_Order_Number__c to find duplicates
        orders_by_number = {}
        for record in result['records']:
            order_number = record.get('Sales_Order_Number__c')
            if order_number:
                if order_number not in orders_by_number:
                    orders_by_number[order_number] = []
                orders_by_number[order_number].append(record)
        
        # Find duplicates
        duplicates = {}
        for order_number, orders in orders_by_number.items():
            if len(orders) > 1:
                duplicates[order_number] = orders
        
        if duplicates:
            logger.info(f"Found {len(duplicates)} order numbers with duplicates:")
            for order_number, orders in duplicates.items():
                logger.info(f"\nOrder Number: {order_number}")
                for i, order in enumerate(orders, 1):
                    logger.info(f"  {i}. ID: {order['Id']}, Invoice: {order.get('Invoice_Number__c', 'None')}, Created: {order['CreatedDate']}")
        else:
            logger.info("No duplicate orders found!")
        
        return duplicates
        
    except Exception as e:
        logger.error(f"Error finding duplicates: {e}")
        return {}
    
    finally:
        sf_client.close()

def remove_duplicate_orders(duplicates, keep_oldest=True):
    """Remove duplicate orders, keeping either the oldest or newest"""
    logger = configure_logger()
    
    if not duplicates:
        logger.info("No duplicates to remove.")
        return
    
    try:
        # Initialize Salesforce client
        sf_client = SalesforceClient(SF_CONFIG)
        
        logger.info(f"Removing duplicates for {len(duplicates)} order numbers...")
        
        for order_number, orders in duplicates.items():
            logger.info(f"\nProcessing duplicates for order {order_number}:")
            
            # Sort by creation date
            sorted_orders = sorted(orders, key=lambda x: x['CreatedDate'])
            
            # Keep the oldest (index 0) or newest (index -1)
            keep_index = 0 if keep_oldest else -1
            orders_to_delete = [order for i, order in enumerate(sorted_orders) if i != keep_index]
            
            logger.info(f"  Keeping: {sorted_orders[keep_index]['Id']} (Created: {sorted_orders[keep_index]['CreatedDate']})")
            
            for order in orders_to_delete:
                logger.info(f"  Deleting: {order['Id']} (Created: {order['CreatedDate']})")
                try:
                    # Note: This is commented out for safety. Uncomment to actually delete.
                    # sf_client.sf.Sales_Order__c.delete(order['Id'])
                    logger.info(f"    (Would delete {order['Id']} - uncomment line above to actually delete)")
                except Exception as e:
                    logger.error(f"    Error deleting {order['Id']}: {e}")
        
        logger.info("Duplicate removal completed!")
        
    except Exception as e:
        logger.error(f"Error removing duplicates: {e}")
    
    finally:
        sf_client.close()

def main():
    """Main function"""
    logger = configure_logger()
    
    logger.info("Salesforce Duplicate Order Cleanup Tool")
    logger.info("=====================================")
    
    # Find duplicates
    duplicates = find_duplicate_orders()
    
    if duplicates:
        print("\nOptions:")
        print("1. View duplicates only (no action)")
        print("2. Remove duplicates (keep oldest)")
        print("3. Remove duplicates (keep newest)")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "2":
            remove_duplicate_orders(duplicates, keep_oldest=True)
        elif choice == "3":
            remove_duplicate_orders(duplicates, keep_oldest=False)
        elif choice == "4":
            logger.info("Exiting without changes.")
        else:
            logger.info("Showing duplicates only.")
    else:
        logger.info("No duplicates found. No action needed.")

if __name__ == "__main__":
    main() 