"""
Kafka consumer for CDC events.
"""
import json
import time
import logging
from typing import Dict, Any, Callable, List
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)


class CDCConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "finance_consumer_group",
        auto_offset_reset: str = "earliest"
    ):
        """
        Initialize the Kafka consumer for CDC events.
        
        Parameters:
            bootstrap_servers: Kafka broker addresses
            group_id: Consumer group ID for load balancing
            auto_offset_reset: Where to start consuming (earliest/latest)
        """
        # configuration for the Kafka consumer
        # auto.offset.reset=earliest ensures we process all available messages
        # enable.auto.commit=False allows us to commit offsets manually after processing
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False
        }
        self.consumer = Consumer(self.config)
        self.running = False

    def wait_for_topics(self, topics: List[str], timeout_seconds: int = 180, retry_interval: int = 10) -> bool:
        """
        Wait for topics to become available.
        
        This is important during startup to ensure Kafka topics exist before trying to consume from them.
        Debezium may take some time to create the topics after connector setup.
        """
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            # get current list of available topics from Kafka cluster
            cluster_metadata = self.consumer.list_topics(timeout=10)
            available_topics = cluster_metadata.topics

            # check if all required topics are available
            all_topics_available = all(topic in available_topics for topic in topics)
            if all_topics_available:
                logger.info(f"All required topics are available: {topics}")
                return True
            
            # log missing topics and retry after a delay
            logger.warning(f"Waiting for topics to become available. Missing topics: "
                           f"{[topic for topic in topics if topic not in available_topics]}")
            time.sleep(retry_interval)
        
        return False

    def subscribe(self, topics: list):
        """
        Subscribe to the specified Kafka topics.
        """
        try:
            # wait for topics to become available
            if not self.wait_for_topics(topics):
                logger.warning(f"Topics {topics} are not available yet. Will continue checking in background...")
            self.consumer.subscribe(topics)
            logger.info(f"Subscribed to topics: {topics}")
        except KafkaException as e:
            logger.error(f"Failed to subscribe to topics: {str(e)}")
            raise

    def process_messages(self, handler: Callable[[Dict[str, Any]], None]):
        """
        Process messages from subscribed topics using the provided handler function.
        
        This method starts an infinite loop that continuously polls for new messages
        from Kafka and processes them using the provided handler function.
        
        Parameters:
            handler: A function that processes a single CDC event
        """
        self.running = True
        try:
            while self.running:
                # poll for messages with a 1-second timeout
                # this allows the loop to check the running flag periodically
                msg = self.consumer.poll(timeout=1.0)
                
                # no message received during timeout period
                if msg is None:
                    continue
                    
                # handle various Kafka error conditions
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # this is not an error, just indicates we've reached the end of a partition
                        logger.debug("Reached end of partition")
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # topic doesn't exist yet, which can happen during startup
                        logger.warning("Topic not available yet, waiting...")
                        time.sleep(1)
                    else:
                        # actual error occurred
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    # decode the message value from JSON
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    # process the message using the handler function
                    # typically this is analytics_db.process_transaction_event
                    handler(value)
                    
                    # manually commit the offset after successful processing
                    # this ensures we don't lose messages if processing fails
                    self.consumer.commit(msg)
                except json.JSONDecodeError as e:
                    # message wasn't valid JSON
                    logger.error(f"Failed to decode message: {str(e)}")
                except Exception as e:
                    # catch any other exceptions during processing
                    # this ensures the loop continues even if processing one message fails
                    logger.error(f"Error processing message: {str(e)}")

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping consumer")
        finally:
            self.stop()

    def stop(self):
        """
        Stop the consumer and close the connection.
        """
        logger.info("Stopping consumer... Committing any remaining offsets.")
        try:
            self.consumer.commit()
        except KafkaException as e:
            logger.warning(f"Failed to commit offsets before shutdown: {str(e)}")
        
        self.running = False
        self.consumer.close()
        logger.info("Consumer stopped and connection closed")
