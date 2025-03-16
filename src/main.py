"""
Main application script for the CDC pipeline.
"""
import os
import sys
import signal
import logging
from dotenv import load_dotenv
from cdc.connector import DebeziumConnector
from cdc.consumer import CDCConsumer
from transformations.analytics_db import AnalyticsDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def shutdown_handler(signal_received, frame):
    """Handle graceful shutdown on Ctrl+C or SIGTERM."""
    logger.info("Shutting down application...")
    sys.exit(0)

# attach signal handlers
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

def main():
    # load environment variables
    load_dotenv()

    try:
        # initialize analytics database
        analytics_db = AnalyticsDB(
            host=os.getenv('ANALYTICS_DB_HOST', 'localhost'),
            port=int(os.getenv('ANALYTICS_DB_PORT', 5433)),
            database=os.getenv('ANALYTICS_DB_NAME', 'analytics_db'),
            user=os.getenv('ANALYTICS_DB_USER', 'postgres'),
            password=os.getenv('ANALYTICS_DB_PASSWORD', 'postgres123')
        )

        # initialize schema
        # this creates all necessary tables and materialized views in the analytics database
        analytics_db.initialize_schema()

        # initialize Debezium connector
        connector = DebeziumConnector(
            connect_url=os.getenv('DEBEZIUM_CONNECT_URL', 'http://localhost:8083')
        )

        # create connectors for all tables
        tables = [
            'customers',    # for customer information
            'products',     # for product information
            'orders',       # for delivery dates and status
            'order_items'   # for pending items
        ]
        for table in tables:
            connector.create_connector(table)

        # initialize Kafka consumer
        # the consumer receives change events published by debezium to kafka topics
        consumer = CDCConsumer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            group_id=os.getenv('KAFKA_GROUP_ID', 'finance_consumer_group')
        )

        # subscribe to all required topics
        # each table has a corresponding kafka topic with the format 'finance.operations.{table}'
        topics = [f'finance.operations.{table}' for table in tables]
        consumer.subscribe(topics)

        # start processing messages
        # this starts an infinite loop that processes incoming CDC events
        # analytics_db.process_transaction_event is passed as the handler function
        logger.info(f"Starting to process CDC events for tables: {', '.join(tables)}")
        consumer.process_messages(analytics_db.process_transaction_event)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        raise

if __name__ == '__main__':
    main() 
