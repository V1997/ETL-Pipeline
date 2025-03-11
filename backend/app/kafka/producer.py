
import json
import logging
from typing import Dict, Any
from confluent_kafka import Producer
from datetime import datetime, date
import uuid

from ..config.settings import settings

logger = logging.getLogger(__name__)

class KafkaProducerService:
    """Service for producing messages to Kafka topics"""
    
    def __init__(self):
        """Initialize Kafka producer"""
        self.producer = Producer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f'sales-etl-producer-{uuid.uuid4()}',
            'acks': 'all',  # Wait for all replicas
            'retries': 5,   # Retry up to 5 times
            'retry.backoff.ms': 500,  # 0.5 second between retries
        })
        logger.info(f"Kafka producer initialized, connecting to {settings.KAFKA_BOOTSTRAP_SERVERS}")
    
    def send_sales_record(self, record: Dict[str, Any], callback=None) -> None:
        """
        Send a single sales record to the sales topic
        Args:
            record: The sales record to send
            callback: Optional callback function for delivery confirmation
        """
        try:
            # Add metadata
            record['kafka_timestamp'] = datetime.utcnow().isoformat()
            record['event_id'] = str(uuid.uuid4())

            # Convert non-serializable data (e.g., dates) to strings
            for key, value in record.items():
                if isinstance(value, (datetime, date)):  # Check for datetime or date objects
                    record[key] = value.isoformat()

            # Convert to JSON
            message = json.dumps(record)

            # Send to Kafka
            self.producer.produce(
                topic=settings.KAFKA_TOPIC_SALES,
                key=record.get('order_id', str(uuid.uuid4())),
                value=message.encode('utf-8'),
                on_delivery=callback or self._delivery_report
            )

            # Trigger any available delivery callbacks
            self.producer.poll(0)

        except Exception as e:
            logger.error(f"Failed to send sales record to Kafka: {str(e)}")
            # Send to error topic
            self._send_error("PRODUCER_ERROR", str(e), {"record": record})
    
    def send_batch(self, records: list, callback=None) -> None:
        """
        Send a batch of sales records
        Args:
            records: List of sales records to send
            callback: Optional callback function for delivery confirmation
        """
        for record in records:
            self.send_sales_record(record, callback)
        
        # Flush to ensure all messages are sent
        self.producer.flush()
    
    def _send_error(self, error_type: str, error_message: str, error_details: Dict = None) -> None:
        """Send error information to error topic"""
        try:
            error_data = {
                'error_type': error_type,
                'error_message': error_message,
                'error_details': error_details or {},
                'timestamp': datetime.utcnow().isoformat(),
                'error_id': str(uuid.uuid4())
            }
            
            self.producer.produce(
                topic=settings.KAFKA_TOPIC_ERRORS,
                key=error_data['error_id'],
                value=json.dumps(error_data).encode('utf-8'),
                on_delivery=self._delivery_report
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send error to Kafka: {str(e)}")
    
    def _delivery_report(self, err, msg) -> None:
        """Delivery report callback for Kafka producer"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")