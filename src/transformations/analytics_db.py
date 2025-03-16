"""
Analytics database handler for transforming and storing CDC events.
Implementation focused on required metrics:
1. Number of open orders by DELIVERY_DATE and STATUS
2. Top 3 delivery dates with more open orders
3. Number of open pending items by PRODUCT_ID
4. Top 3 Customers with more pending orders
"""
import logging
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import date

logger = logging.getLogger(__name__)


class AnalyticsDB:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        database: str = "analytics_db",
        user: str = "analytics_user",
        password: str = "analytics_pass"
    ):
        """Initialize the analytics database connection."""
        self.engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        logger.info(f"Connected to analytics database at {host}:{port}/{database}")

    def initialize_schema(self):
        """Initialize the analytics schema with required tables and materialized views."""
        logger.info("Initializing analytics schema...")
        try:
            with self.engine.connect() as conn:
                # create orders_status_summary table
                # this table stores aggregated counts of orders by delivery date and status
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS orders_status_summary (
                        delivery_date DATE,
                        status VARCHAR(50),
                        order_count INTEGER DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (delivery_date, status)
                    )
                """))

                # create order_status_tracking table to track individual order statuses
                # this helps us keep track of the current status of each order
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS order_status_tracking (
                        order_id BIGINT,
                        status VARCHAR(50),
                        delivery_date DATE,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (order_id)
                    )
                """))

                # create pending_items_summary table
                # tracks pending items by product to answer query #3
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS pending_items_summary (
                        product_id BIGINT,
                        product_name VARCHAR(500),
                        pending_quantity INTEGER DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (product_id)
                    )
                """))

                # create customer_orders_summary table
                # tracks pending orders by customer to answer query #4
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS customer_orders_summary (
                        customer_id BIGINT,
                        customer_name VARCHAR(500),
                        pending_orders INTEGER DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (customer_id)
                    )
                """))

                # create materialized views for the required queries
                # these provide pre-computed results for faster query responses
                
                # materialized view for query #2: top 3 delivery dates with more open orders
                conn.execute(text("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS top_delivery_dates AS
                    SELECT delivery_date, SUM(order_count) as total_orders
                    FROM orders_status_summary
                    WHERE status = 'PENDING'
                    GROUP BY delivery_date
                    ORDER BY total_orders DESC
                    LIMIT 3
                """))
                
                # materialized view for query #4: top 3 customers with pending orders
                conn.execute(text("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS top_customers_pending AS
                    SELECT 
                        customer_id,
                        customer_name,
                        pending_orders
                    FROM customer_orders_summary
                    WHERE pending_orders > 0
                    ORDER BY pending_orders DESC
                    LIMIT 3
                """))

                conn.commit()
                logger.info("Analytics schema initialized successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize schema: {str(e)}")
            raise

    def process_transaction_event(self, event: Dict[str, Any]):
        """Process CDC events from different tables and update analytics."""
        try:
            # extract event data
            payload = event.get('payload', {})
            if not payload:
                logger.warning("No payload in event")
                return

            operation = payload.get('op')
            source = payload.get('source', {})
            table = source.get('table')
            
            logger.info(f"Processing {operation} event for table {table}")
            
            if not operation or not table:
                logger.warning(f"Invalid event: missing operation or table")
                return

            # get data based on operation type
            before_data = self._safe_get_data(payload, 'before')
            after_data = self._safe_get_data(payload, 'after')
            
            # process based on operation type
            data = after_data if operation in ('c', 'r', 'u') else before_data
            if not data:
                logger.warning("No data found in event payload")
                return
                
            # convert date fields if needed
            data = self._convert_date_fields(data)
            if before_data:
                before_data = self._convert_date_fields(before_data)
            
            # process the event based on table and operation
            with self.engine.connect() as conn:
                if table == 'orders':
                    self._process_order_event(conn, operation, data, before_data)
                elif table == 'order_items':
                    self._process_order_item_event(conn, operation, data)
                elif table == 'products':
                    self._process_product_event(conn, operation, data)
                elif table == 'customers':
                    self._process_customer_event(conn, operation, data)
                else:
                    logger.warning(f"Unhandled table: {table}")
                    return
                
                # refresh materialized views
                self._refresh_materialized_views(conn)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}")
            logger.error(f"Event data: {event}")
            raise

    def _safe_get_data(self, payload: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
        """Safely extract data from payload with error handling."""
        data = payload.get(key)
        if data:
            return data.copy()
        return None

    def _convert_date_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Debezium date formats to Python dates."""
        result = data.copy()
        for field in ['order_date', 'delivery_date']:
            if field in result and isinstance(result[field], (int, float)):
                result[field] = self._convert_debezium_date(result[field])
        return result

    def _convert_debezium_date(self, date_value):
        """Convert Debezium date format (days since epoch) to datetime.date."""
        try:
            if date_value is None:
                return None
            if isinstance(date_value, date):
                return date_value
            if isinstance(date_value, (int, float)):
                return date.fromordinal(int(date_value) + 719163)  # adjust for epoch difference
            logger.warning(f"Unexpected date format: {date_value} ({type(date_value)})")
            return None
        except Exception as e:
            logger.error(f"Error converting date {date_value}: {str(e)}")
            return None

    def _process_order_event(self, conn, operation: str, data: Dict[str, Any], before_data: Optional[Dict[str, Any]] = None):
        """
        Process order events focusing on required metrics.
        
        This method handles the core business logic for tracking orders and their status changes.
        It's responsible for maintaining the data needed for queries #1 and #2.
        
        Parameters:
            conn: Database connection
            operation: CDC operation type (c=create, r=read, u=update, d=delete)
            data: Current state of the record
            before_data: Previous state of the record (for updates)
        """
        try:
            # extract key fields from the order record
            delivery_date = data.get('delivery_date')
            status = data.get('status')
            customer_id = data.get('customer_id')
            order_id = data.get('order_id')
            
            # validate that required fields are present
            if not delivery_date or not status or not order_id:
                logger.warning(f"Missing required fields in order event: {data}")
                return
                
            logger.info(f"Processing order: id={order_id}, status={status}, op={operation}")
            
            # handle creation of new orders
            if operation in ('c', 'r'):
                # 'c' = create from insert, 'r' = read from initial snapshot
                
                # increment count for this delivery date and status combination
                # this maintains the data for query #1: orders by delivery date and status
                self._update_orders_status_summary(conn, delivery_date, status, 1)
                
                # update order status tracking table
                # this table tracks the current status of each individual order
                conn.execute(text("""
                    INSERT INTO order_status_tracking (order_id, status, delivery_date, updated_at)
                    VALUES (:order_id, :status, :delivery_date, CURRENT_TIMESTAMP)
                    ON CONFLICT (order_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        delivery_date = EXCLUDED.delivery_date,
                        updated_at = CURRENT_TIMESTAMP
                """), {"order_id": order_id, "status": status, "delivery_date": delivery_date})
                
                # update customer pending orders count if status is PENDING
                # this maintains data for query #4: top customers with pending orders
                if status == 'PENDING':
                    self._update_customer_pending_orders(conn, customer_id, 1)
                    
            # handle updates to existing orders
            elif operation == 'u' and before_data:
                # for updates, we need to handle the difference between old and new state
                old_status = before_data.get('status')
                old_delivery_date = before_data.get('delivery_date')
                
                # if status changed, update both old and new status counts
                # decrement the old status count and increment the new one
                if old_status != status:
                    if old_status:
                        self._update_orders_status_summary(conn, old_delivery_date, old_status, -1)
                    self._update_orders_status_summary(conn, delivery_date, status, 1)
                    
                    # update order status tracking table
                    conn.execute(text("""
                        UPDATE order_status_tracking
                        SET status = :status,
                            delivery_date = :delivery_date,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE order_id = :order_id
                    """), {"order_id": order_id, "status": status, "delivery_date": delivery_date})
                    
                    # update customer pending orders based on status transition
                    if old_status == 'PENDING' and status != 'PENDING':
                        self._update_customer_pending_orders(conn, customer_id, -1)
                    elif old_status != 'PENDING' and status == 'PENDING':
                        self._update_customer_pending_orders(conn, customer_id, 1)
                
                # if delivery date changed but status didn't, update both old and new delivery dates
                elif old_delivery_date != delivery_date:
                    self._update_orders_status_summary(conn, old_delivery_date, status, -1)
                    self._update_orders_status_summary(conn, delivery_date, status, 1)
                    
                    # update order status tracking table
                    conn.execute(text("""
                        UPDATE order_status_tracking
                        SET delivery_date = :delivery_date,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE order_id = :order_id
                    """), {"order_id": order_id, "delivery_date": delivery_date})
                    
            # handle deletion of orders
            elif operation == 'd':
                self._update_orders_status_summary(conn, delivery_date, status, -1)
                
                # remove from order status tracking
                conn.execute(text("""
                    DELETE FROM order_status_tracking
                    WHERE order_id = :order_id
                """), {"order_id": order_id})
                
                if status == 'PENDING':
                    self._update_customer_pending_orders(conn, customer_id, -1)
        
        except Exception as e:
            logger.error(f"Error processing order event: {str(e)}")
            logger.error(f"Data: {data}")
            raise

    def _process_order_item_event(self, conn, operation: str, data: Dict[str, Any]):
        """Process order item events focusing on pending items by product."""
        try:
            product_id = data.get('product_id')
            order_id = data.get('order_id')
            quantity = data.get('quantity', 0)
            
            if not product_id or not order_id:
                logger.warning(f"Missing required fields in order item event: {data}")
                return
                
            logger.info(f"Processing order item: order_id={order_id}, product_id={product_id}, op={operation}")
            
            # check if the related order is PENDING using our tracking table
            is_pending = self._is_order_pending(conn, order_id)
            
            if is_pending:
                # for creation, increase pending quantity
                if operation in ('c', 'r'):
                    self._update_pending_item_quantity(conn, product_id, quantity)
                # for deletion, decrease pending quantity
                elif operation == 'd':
                    self._update_pending_item_quantity(conn, product_id, -quantity)
                    
        except Exception as e:
            logger.error(f"Error processing order item event: {str(e)}")
            logger.error(f"Data: {data}")
            raise

    def _process_product_event(self, conn, operation: str, data: Dict[str, Any]):
        """Process product events to maintain product information."""
        try:
            product_id = data.get('product_id')
            product_name = data.get('product_name')
            
            if not product_id:
                logger.warning(f"Missing product_id in product event: {data}")
                return
                
            logger.info(f"Processing product: id={product_id}, name={product_name}, op={operation}")
            
            # for creation or update, ensure product exists in summary table
            if operation in ('c', 'r', 'u'):
                conn.execute(text("""
                    INSERT INTO pending_items_summary (
                        product_id, product_name, pending_quantity, updated_at
                    ) VALUES (
                        :product_id, :product_name, 0, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        updated_at = CURRENT_TIMESTAMP
                """), data)
                
        except Exception as e:
            logger.error(f"Error processing product event: {str(e)}")
            logger.error(f"Data: {data}")
            raise

    def _process_customer_event(self, conn, operation: str, data: Dict[str, Any]):
        """Process customer events to maintain customer information."""
        try:
            customer_id = data.get('customer_id')
            customer_name = data.get('customer_name')
            
            if not customer_id:
                logger.warning(f"Missing customer_id in customer event: {data}")
                return
                
            logger.info(f"Processing customer: id={customer_id}, name={customer_name}, op={operation}")
            
            # for creation or update, ensure customer exists in summary table
            if operation in ('c', 'r', 'u'):
                conn.execute(text("""
                    INSERT INTO customer_orders_summary (
                        customer_id, customer_name, pending_orders, updated_at
                    ) VALUES (
                        :customer_id, :customer_name, 0, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (customer_id) DO UPDATE SET
                        customer_name = EXCLUDED.customer_name,
                        updated_at = CURRENT_TIMESTAMP
                """), data)
                
        except Exception as e:
            logger.error(f"Error processing customer event: {str(e)}")
            logger.error(f"Data: {data}")
            raise

    def _update_orders_status_summary(self, conn, delivery_date, status, change):
        """Update the orders_status_summary table."""
        if not delivery_date or not status:
            return
            
        params = {
            "delivery_date": delivery_date,
            "status": status,
            "change": change
        }
        
        if change > 0:
            conn.execute(text("""
                INSERT INTO orders_status_summary (delivery_date, status, order_count, updated_at)
                VALUES (:delivery_date, :status, :change, CURRENT_TIMESTAMP)
                ON CONFLICT (delivery_date, status) 
                DO UPDATE SET
                    order_count = orders_status_summary.order_count + :change,
                    updated_at = CURRENT_TIMESTAMP
            """), params)
        else:
            conn.execute(text("""
                UPDATE orders_status_summary
                SET 
                    order_count = GREATEST(order_count + :change, 0),
                    updated_at = CURRENT_TIMESTAMP
                WHERE delivery_date = :delivery_date AND status = :status
            """), params)

    def _update_customer_pending_orders(self, conn, customer_id, change):
        """Update the customer_orders_summary table for pending orders."""
        if not customer_id:
            return
            
        params = {
            "customer_id": customer_id,
            "change": change
        }
        
        if change > 0:
            conn.execute(text("""
                UPDATE customer_orders_summary
                SET 
                    pending_orders = pending_orders + :change,
                    updated_at = CURRENT_TIMESTAMP
                WHERE customer_id = :customer_id
            """), params)
        else:
            conn.execute(text("""
                UPDATE customer_orders_summary
                SET 
                    pending_orders = GREATEST(pending_orders + :change, 0),
                    updated_at = CURRENT_TIMESTAMP
                WHERE customer_id = :customer_id
            """), params)

    def _update_pending_item_quantity(self, conn, product_id, quantity_change):
        """Update the pending_items_summary table."""
        if not product_id:
            return
            
        params = {
            "product_id": product_id,
            "change": quantity_change
        }
        
        if quantity_change > 0:
            conn.execute(text("""
                UPDATE pending_items_summary
                SET 
                    pending_quantity = pending_quantity + :change,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_id = :product_id
            """), params)
        else:
            conn.execute(text("""
                UPDATE pending_items_summary
                SET 
                    pending_quantity = GREATEST(pending_quantity + :change, 0),
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_id = :product_id
            """), params)

    def _is_order_pending(self, conn, order_id):
        """Check if an order has PENDING status using our order_status_tracking table."""
        try:
            result = conn.execute(text("""
                SELECT 1
                FROM order_status_tracking
                WHERE order_id = :order_id AND status = 'PENDING'
                LIMIT 1
            """), {"order_id": order_id}).fetchone()
            
            return result is not None
        except Exception as e:
            logger.error(f"Error checking if order {order_id} is pending: {str(e)}")
            # if we encounter an error, default to assuming it's not pending
            return False

    def _refresh_materialized_views(self, conn):
        """Refresh the materialized views for the required queries."""
        try:
            conn.execute(text("REFRESH MATERIALIZED VIEW top_delivery_dates"))
            conn.execute(text("REFRESH MATERIALIZED VIEW top_customers_pending"))
            logger.info("Refreshed materialized views")
        except Exception as e:
            logger.error(f"Error refreshing materialized views: {str(e)}")

    # query methods for the required metrics
    
    def get_orders_by_delivery_and_status(self):
        """
        Query 1: Number of open orders by DELIVERY_DATE and STATUS.
        
        This query returns data on how many orders are in each status,
        grouped by delivery date. This helps understand the distribution
        of orders over time and by status.
        
        Returns:
            List of dictionaries with delivery_date, status, and order_count
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT delivery_date, status, order_count
                FROM orders_status_summary
                ORDER BY delivery_date, status
            """))
            return [row._asdict() for row in result]

    def get_top_delivery_dates(self):
        """
        Query 2: Top 3 delivery dates with more open orders.
        
        This query identifies which upcoming delivery dates have the 
        highest volume of pending orders, which helps with resource planning.
        
        Returns:
            List of dictionaries with delivery_date and total_orders
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT * FROM top_delivery_dates
            """))
            return [row._asdict() for row in result]

    def get_pending_items_by_product(self):
        """
        Query 3: Number of open pending items by PRODUCT_ID.
        
        This query provides information on which products have the most
        pending orders.
        
        Returns:
            List of dictionaries with product_id, product_name, and pending_quantity
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT product_id, product_name, pending_quantity
                FROM pending_items_summary
                WHERE pending_quantity > 0
                ORDER BY pending_quantity DESC
            """))
            return [row._asdict() for row in result]

    def get_top_customers_pending(self):
        """
        Query 4: Top 3 Customers with more pending orders.
        
        This query identifies the customers with the most pending orders.
        
        Returns:
            List of dictionaries with customer_id, customer_name, and pending_orders
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT * FROM top_customers_pending
            """))
            return [row._asdict() for row in result] 
