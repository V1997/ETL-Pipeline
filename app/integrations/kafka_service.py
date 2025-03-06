import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Callable
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import threading
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class KafkaConfig:
    """Kafka configuration settings from environment variables."""
    
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    GROUP_ID = os.getenv("KAFKA_GROUP_ID", "etl_pipeline")
    AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    ENABLE_AUTO_COMMIT = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true"
    SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
    MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))
    DEFAULT_TOPIC_CONFIG = {
        "auto.offset.reset": AUTO_OFFSET_RESET
    }


class KafkaProducer:
    """
    Kafka producer for sending messages to Kafka topics.
    
    This class wraps the confluent_kafka.Producer with additional functionality:
    - JSON serialization
    - Delivery confirmations
    - Error handling
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the Kafka producer.
        
        Args:
            config: Optional producer configuration overrides
        """
        self.default_config = {
            'bootstrap.servers': KafkaConfig.BOOTSTRAP_SERVERS,
            'client.id': f'etl_producer_{uuid.uuid4().hex[:8]}'
        }
        
        if config:
            self.default_config.update(config)
            
        self.producer = Producer(self.default_config)
        logger.info(f"Kafka producer initialized with bootstrap servers: {KafkaConfig.BOOTSTRAP_SERVERS}")
    
    def delivery_callback(self, err, msg):
        """Callback function for delivery confirmations."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def publish(self, topic: str, value: Dict[str, Any], key: str = None, headers: List[Dict[str, str]] = None) -> bool:
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            value: Message payload (will be serialized to JSON)
            key: Optional message key
            headers: Optional message headers
            
        Returns:
            bool: True if the message was sent successfully, False otherwise
        """
        try:
            # Convert value to JSON string
            value_json = json.dumps(value)
            
            # Convert headers to format expected by confluent_kafka
            kafka_headers = None
            if headers:
                kafka_headers = [(k, v.encode('utf-8')) for h in headers for k, v in h.items()]
            
            # Encode message key if provided
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce the message
            self.producer.produce(
                topic=topic,
                value=value_json.encode('utf-8'),
                key=key_bytes,
                headers=kafka_headers,
                callback=self.delivery_callback
            )
            
            # Poll to handle delivery reports
            self.producer.poll(0)
            return True
            
        except Exception as e:
            logger.error(f"Error publishing message to {topic}: {str(e)}")
            return False
    
    def flush(self, timeout: float = 10.0) -> int:
        """
        Wait for all messages to be delivered.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            int: Number of messages still in queue
        """
        return self.producer.flush(timeout)
    
    def close(self):
        """Close the producer connection."""
        self.producer.flush()


class KafkaConsumer:
    """
    Kafka consumer for receiving messages from Kafka topics.
    
    This class wraps the confluent_kafka.Consumer with additional functionality:
    - JSON deserialization
    - Automatic handling of common errors
    - Background polling with callback support
    """
    
    def __init__(self, topics: List[str], config: Dict[str, Any] = None, group_id: str = None):
        """
        Initialize the Kafka consumer.
        
        Args:
            topics: List of topics to subscribe to
            config: Optional consumer configuration overrides
            group_id: Consumer group ID override
        """
        self.topics = topics
        self.is_running = False
        self.poll_thread = None
        
        self.default_config = {
            'bootstrap.servers': KafkaConfig.BOOTSTRAP_SERVERS,
            'group.id': group_id or KafkaConfig.GROUP_ID,
            'auto.offset.reset': KafkaConfig.AUTO_OFFSET_RESET,
            'enable.auto.commit': KafkaConfig.ENABLE_AUTO_COMMIT,
            'session.timeout.ms': KafkaConfig.SESSION_TIMEOUT_MS,
            'max.poll.interval.ms': KafkaConfig.MAX_POLL_INTERVAL_MS
        }
        
        if config:
            self.default_config.update(config)
            
        self.consumer = Consumer(self.default_config)
        self.consumer.subscribe(topics)
        
        logger.info(f"Kafka consumer initialized for topics: {', '.join(topics)}")
    
    def consume(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Consume a single message from subscribed topics.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Optional[Dict]: Message payload or None if no message was received
        """
        try:
            msg = self.consumer.poll(timeout)
            
            if msg is None:
                return None
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, not an error
                    logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    return None
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    return None
            
            # Process message
            try:
                value_json = msg.value().decode('utf-8')
                value = json.loads(value_json)
                
                # Extract key if present
                key = msg.key().decode('utf-8') if msg.key() else None
                
                # Extract headers if present
                headers = {}
                if msg.headers():
                    for header in msg.headers():
                        headers[header[0]] = header[1].decode('utf-8')
                
                return {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': key,
                    'value': value,
                    'headers': headers,
                    'timestamp': msg.timestamp()
                }
                
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON message: {msg.value()}")
                return None
                
        except KafkaException as e:
            logger.error(f"Kafka exception during consume: {str(e)}")
            return None
    
    def start_polling(self, callback: Callable[[Dict[str, Any]], None], poll_interval: float = 1.0):
        """
        Start polling for messages in a background thread.
        
        Args:
            callback: Function to call with each message
            poll_interval: How often to poll in seconds
        """
        if self.is_running:
            logger.warning("Consumer is already polling")
            return
            
        self.is_running = True
        
        def poll_loop():
            while self.is_running:
                try:
                    message = self.consume(poll_interval)
                    if message:
                        callback(message)
                except Exception as e:
                    logger.error(f"Error in poll loop: {str(e)}")
                    time.sleep(1)  # Prevent tight loop on repeated errors
        
        self.poll_thread = threading.Thread(target=poll_loop, daemon=True)
        self.poll_thread.start()
        logger.info("Started background message polling")
    
    def stop_polling(self):
        """Stop the background polling thread."""
        if self.is_running:
            self.is_running = False
            if self.poll_thread:
                self.poll_thread.join(timeout=5)
                logger.info("Stopped background message polling")
    
    def commit(self):
        """Commit offsets for consumed messages."""
        self.consumer.commit()
    
    def close(self):
        """Close the consumer connection."""
        self.stop_polling()
        self.consumer.close()
        logger.info("Kafka consumer closed")