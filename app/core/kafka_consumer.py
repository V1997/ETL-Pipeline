class ETLKafkaProducer:
    """
    Kafka producer for ETL pipeline notifications.
    
    Sends messages about batch processing status and events.
    """
    
    def __init__(self, bootstrap_servers, topic="etl-notifications"):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            topic: Default topic for messages
        """
        self.bootstrap_servers = bootstrap_servers
        self.default_topic = topic
        self.producer = None
        self.connect()
    
    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            self.producer = None
            return False
    
    def send_message(self, message, topic=None, key=None):
        """
        Send a message to Kafka.
        
        Args:
            message: Dictionary containing the message
            topic: Topic to send to (uses default if not specified)
            key: Message key for partitioning
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        if not self.producer:
            logger.warning("Not connected to Kafka, attempting to reconnect")
            if not self.connect():
                logger.error("Failed to reconnect to Kafka")
                return False
        
        topic = topic or self.default_topic
        
        try:
            future = self.producer.send(topic, value=message, key=key)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {str(e)}")
            return False
    
    def send_batch_start(self, batch_id, source, record_count, created_by=None):
        """Send notification about batch processing start"""
        message = {
            "type": "batch_start",
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "user": created_by,
            "details": {
                "source": source,
                "record_count": record_count
            }
        }
        return self.send_message(message, key=batch_id)
    
    def send_batch_complete(self, batch_id, stats, created_by=None):
        """Send notification about batch processing completion"""
        message = {
            "type": "batch_complete",
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "user": created_by,
            "details": stats
        }
        return self.send_message(message, key=batch_id)
    
    def send_batch_error(self, batch_id, error_message, created_by=None):
        """Send notification about batch processing error"""
        message = {
            "type": "batch_error",
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "user": created_by,
            "details": {
                "error_message": error_message
            }
        }
        return self.send_message(message, key=batch_id)
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")