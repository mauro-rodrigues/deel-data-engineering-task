"""
Command line interface for ACME Delivery Services Analytics.
Provides access to key business metrics with CSV export capability.
"""
import os
import csv
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# add the parent directory to the Python path to allow imports from sibling packages
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from transformations.analytics_db import AnalyticsDB

def export_to_csv(data: List[Dict[Any, Any]], filename: str) -> str:
    """Export data to CSV file with timestamp in filename."""
    if not data:
        return "No data to export, please wait a bit before retrying."
    
    # create output directory if it doesn't exist
    # this ensures the export works even on first run
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    # add timestamp to filename
    # this prevents overwriting previous exports and provides a chronological record
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_filename = output_dir / f"{filename}_{timestamp}.csv"
    
    # create csv file with the exported data
    # using DictWriter handles the column headers automatically
    with open(full_filename, 'w', newline='') as csvfile:
        if data:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    
    return f"Data exported to {full_filename}"

def main():
    # set up command line argument parser with descriptive help text
    parser = argparse.ArgumentParser(description='ACME Delivery Services Analytics CLI')
    parser.add_argument('query', choices=['orders', 'top-dates', 'pending-items', 'top-customers'],
                      help='Query to run: orders (by delivery date and status), '
                           'top-dates (top 3 delivery dates), '
                           'pending-items (pending items by product), '
                           'top-customers (top 3 customers with pending orders)')
    
    args = parser.parse_args()
    
    try:
        # connect to the analytics database
        # uses default connection parameters from the AnalyticsDB class
        db = AnalyticsDB()
        
        # execute requested query based on command line argument
        # each query corresponds to one of the business requirements
        if args.query == 'orders':
            # number of open orders by delivery date and status
            data = db.get_orders_by_delivery_and_status()
            print(export_to_csv(data, "orders_by_delivery_status"))
            
        elif args.query == 'top-dates':
            # top 3 delivery dates with more open orders
            data = db.get_top_delivery_dates()
            print(export_to_csv(data, "top_delivery_dates"))
            
        elif args.query == 'pending-items':
            # number of open pending items by product ID
            data = db.get_pending_items_by_product()
            print(export_to_csv(data, "pending_items_by_product"))
            
        elif args.query == 'top-customers':
            # top 3 customers with more pending orders
            data = db.get_top_customers_pending()
            print(export_to_csv(data, "top_customers_pending"))
    
    except Exception as e:
        # print error message to stderr and exit with error code
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 
