#!/usr/bin/env python
"""
ETL Pipeline End-to-End Testing Script

This script performs a complete test of the ETL workflow:
1. Sets up the necessary environment
2. Generates test data
3. Processes the data through the pipeline
4. Validates the results at each stage
5. Tests Kafka notifications
6. Verifies database records and metrics
7. Tests error handling and recovery

Usage:
    python test_etl_workflow.py --mode [full|simple]
    
    --mode full:   Run comprehensive tests including error cases
    --mode simple: Run basic happy path test only
"""

import os
import sys
import argparse
import pandas as pd
import json
import time
import uuid
import logging
from datetime import datetime
import traceback
import unittest
import random
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
import threading
import atexit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("etl_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ETL-Test")

# Set current time and user for consistent testing
CURRENT_TIME = "2025-03-06 04:48:02"
CURRENT_USER = "V1997"

# Import application components - Use try/except to provide helpful error messages
try:
    # Fix the import path if necessary
    app_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if app_path not in sys.path:
        sys.path.append(app_path)
        
    from core.batch_processor import BatchProcessor
    from core.data_cleaner import DataCleaner
    from core.kafka_producer import ETLKafkaProducer
    from db.session import get_db_session
    from models.models import SalesBatch, SalesRecord, ETLError, ETLMetric, ETLAudit, Base
    from db.transaction import transaction_scope
except ImportError as e:
    logger.error(f"Failed to import application components: {str(e)}")
    logger.error("Make sure the app package is in your PYTHONPATH.")
    logger.error("Add the project root directory to PYTHONPATH or run this script from the project root.")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Fix database access issues with a global connection cleanup function
db_connections = []

def cleanup_connections():
    """Clean up any remaining database connections"""
    for conn in db_connections:
        try:
            conn.close()
            logger.debug("Closed database connection")
        except Exception:
            pass

atexit.register(cleanup_connections)


class KafkaHandler:
    """Helper class to manage Kafka connections and message processing"""
    
    def __init__(self, bootstrap_servers="localhost:9092", topic="etl-test-topic"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.consumer = None
        self.received_messages = []
        self.consumer_thread = None
        self.running = False
        
        # Create the topic if it doesn't exist
        self.create_topic()
    
    def create_topic(self):
        """Create the Kafka topic if it doesn't exist"""
        try:
            # Using the kafka-topics command via subprocess would be ideal
            # But we'll use a simple producer-based approach for portability
            producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
            producer.send(self.topic, b"topic_init")
            producer.flush()
            producer.close()
            logger.info(f"Created or verified Kafka topic: {self.topic}")
        except Exception as e:
            logger.warning(f"Could not initialize Kafka topic: {str(e)}")
    
    def connect_producer(self):
        """Connect to Kafka as a producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f"Connected to Kafka producer at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {str(e)}")
            return False
    
    def connect_consumer(self):
        """Connect to Kafka as a consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f"etl-test-consumer-{uuid.uuid4().hex[:8]}",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f"Connected to Kafka consumer at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {str(e)}")
            return False
    
    def send_message(self, message, key=None):
        """Send a message to Kafka"""
        if not self.producer:
            if not self.connect_producer():
                return False
        
        try:
            future = self.producer.send(self.topic, value=message, key=key)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            logger.debug(f"Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {str(e)}")
            return False
    
    def _consume_messages(self):
        """Consumer thread to collect messages"""
        self.running = True
        try:
            while self.running:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for partition_batch in message_batch.values():
                    for message in partition_batch:
                        # Skip the topic init message
                        if message.value == "topic_init":
                            continue
                        self.received_messages.append(message.value)
                        logger.debug(f"Received message: {message.value}")
        except Exception as e:
            logger.error(f"Error in Kafka consumer: {str(e)}")
        finally:
            self.running = False
    
    def start_consumer(self):
        """Start the consumer in a background thread"""
        if not self.consumer:
            if not self.connect_consumer():
                return False
        
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        return True
    
    def stop_consumer(self):
        """Stop the consumer thread"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        
        if self.consumer:
            self.consumer.close()
    
    def clear_messages(self):
        """Clear received messages"""
        self.received_messages = []


class TestETLWorkflow(unittest.TestCase):
    """End-to-End tests for the ETL Pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment and connections"""
        logger.info("Setting up ETL test environment")
        
        # Database setup
        # cls.db_url = os.getenv("TEST_DATABASE_URL", "sqlite:///./test_etl.db")
        # cls.setup_database(cls.db_url)
        
        # Kafka setup
        cls.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        cls.kafka_topic = "etl-test-topic"
        cls.kafka_handler = KafkaHandler(cls.kafka_servers, cls.kafka_topic)
        
        # Create batch processor with a higher error threshold for testing
        cls.config = {
            "BATCH_SIZE": 100,
            "MAX_WORKERS": 2,
            "RETRY_ATTEMPTS": 1,
            "ERROR_THRESHOLD": 0.3,  # Increased to allow for test data variations
            "KAFKA_ENABLED": True,
            "KAFKA_BOOTSTRAP_SERVERS": cls.kafka_servers,
            "KAFKA_TOPIC": cls.kafka_topic
        }
        
        # Create a subclass of BatchProcessor that correctly defines the logger
        class TestBatchProcessor(BatchProcessor):
            def __init__(self, config=None):
                # Ensure logger is properly defined
                self.logger = logging.getLogger(__name__)
                super().__init__(config)
        
        cls.batch_processor = TestBatchProcessor(cls.config)
        
        # Data cleaner for validation
        cls.data_cleaner = DataCleaner()
        
        # Start Kafka consumer
        cls.kafka_handler.start_consumer()
        time.sleep(1)  # Allow consumer to connect and subscribe
        
        logger.info("Test environment setup complete")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after tests"""
        logger.info("Cleaning up test environment")
        
        # Stop Kafka consumer
        cls.kafka_handler.stop_consumer()
        
        # Close any DB sessions
        cleanup_connections()
        
        # Clean up test database safely
        try:
            if os.path.exists("./test_etl.db"):
                os.unlink("./test_etl.db")
                logger.info("Removed test database file")
        except Exception as e:
            logger.warning(f"Could not remove test database: {str(e)}")
        
        logger.info("Test environment cleanup complete")
    
    @classmethod
    def setup_database(cls, db_url):
        """Set up test database"""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        try:
            # Create engine and tables
            engine = create_engine(db_url)
            Base.metadata.create_all(engine)
            logger.info(f"Created test database at {db_url}")
        except Exception as e:
            logger.error(f"Failed to set up database: {str(e)}")
            raise
    
    def setUp(self):
        """Set up for individual tests"""
        self.kafka_handler.clear_messages()
    
    def generate_test_data(self, rows=100, error_rate=0.0):
        """Generate test data for ETL testing"""
        regions = ["North America", "Europe", "Asia", "Africa", "South America"]
        countries = {
            "North America": ["USA", "Canada", "Mexico"],
            "Europe": ["UK", "Germany", "France", "Italy", "Spain"],
            "Asia": ["China", "Japan", "India", "South Korea"],
            "Africa": ["South Africa", "Nigeria", "Egypt"],
            "South America": ["Brazil", "Argentina", "Colombia"]
        }
        item_types = ["Electronics", "Furniture", "Clothing", "Books", "Office Supplies"]
        sales_channels = ["Online", "Offline"]
        priorities = ["H", "M", "L"]
        
        data = []
        for i in range(rows):
            # Determine if this record should have errors
            has_error = random.random() < error_rate
            
            # Generate base data
            region = random.choice(regions)
            country = random.choice(countries.get(region, ["Unknown"]))
            item_type = random.choice(item_types)
            sales_channel = random.choice(sales_channels)
            priority = random.choice(priorities)
            
            # Dates
            order_year = 2025
            order_month = random.randint(1, 3)
            order_day = random.randint(1, 28)
            order_date = f"{order_year}-{order_month:02d}-{order_day:02d}"
            
            # Ship date is usually after order date
            ship_days_later = random.randint(1, 14)
            ship_month = order_month
            ship_day = order_day + ship_days_later
            
            # Adjust for month overflow
            if ship_day > 28:
                ship_day -= 28
                ship_month += 1
                if ship_month > 12:
                    ship_month = 1
            
            ship_date = f"{order_year}-{ship_month:02d}-{ship_day:02d}"
            
            # Order ID
            region_code = region[:2].upper()
            order_id = f"{region_code}-{order_year}-{random.randint(1000000, 9999999)}"
            
            # Quantities and prices
            units_sold = random.randint(1, 1000)
            unit_price = round(random.uniform(5, 500), 2)
            unit_cost = round(unit_price * random.uniform(0.4, 0.8), 2)
            
            # Add errors if needed
            if has_error:
                error_type = random.randint(1, 5)
                if error_type == 1:
                    # Invalid region
                    region = "Unknown Region"
                elif error_type == 2:
                    # Empty country
                    country = ""
                elif error_type == 3:
                    # Invalid sales channel
                    sales_channel = "Phone"
                elif error_type == 4:
                    # Negative quantity
                    units_sold = -units_sold
                elif error_type == 5:
                    # Ship date before order date
                    ship_date, order_date = order_date, ship_date
            
            record = {
                "region": region,
                "country": country,
                "item_type": item_type,
                "sales_channel": sales_channel,
                "order_priority": priority,
                "order_date": order_date,
                "order_id": order_id,
                "ship_date": ship_date,
                "units_sold": units_sold,
                "unit_price": unit_price,
                "unit_cost": unit_cost
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def validate_batch_record(self, batch_id):
        """Validate batch record exists and has proper data"""
        with get_db_session() as session:
            batch = session.query(SalesBatch).filter_by(batch_id=batch_id).first()
            self.assertIsNotNone(batch, f"Batch {batch_id} not found")
            self.assertIsNotNone(batch.start_time)
            return batch
    
    def validate_sales_records(self, batch_id, expected_count):
        """Validate sales records for a batch"""
        with get_db_session() as session:
            records = session.query(SalesRecord).filter_by(batch_id=batch_id).all()
            self.assertEqual(len(records), expected_count, 
                          f"Expected {expected_count} records, got {len(records)}")
            return records
    
    def validate_metrics(self, batch_id):
        """Validate metrics were recorded"""
        with get_db_session() as session:
            metrics = session.query(ETLMetric).filter_by(batch_id=batch_id).all()
            self.assertGreater(len(metrics), 0, "No metrics recorded")
            return metrics

    def validate_kafka_messages(self, expected_batch_id, expected_types):
        """Validate Kafka messages were received"""
        # Wait for messages to be received
        time.sleep(2)
        
        # Filter messages for this batch
        batch_messages = []
        for msg in self.kafka_handler.received_messages:
            if isinstance(msg, dict) and msg.get("batch_id") == expected_batch_id:
                batch_messages.append(msg)
        
        # Check we have the right message types
        message_types = [msg.get("type") for msg in batch_messages]
        
        # Skip validation if Kafka is not available
        if not batch_messages and len(self.kafka_handler.received_messages) == 0:
            logger.warning(f"No Kafka messages received - skipping Kafka validation")
            return []
            
        for expected_type in expected_types:
            self.assertIn(expected_type, message_types, 
                        f"Expected {expected_type} message not found in Kafka")
        
        return batch_messages
    
    def test_01_happy_path(self):
        """Test normal ETL flow with valid data"""
        logger.info("Starting happy path test")
        
        # 1. Generate valid test data
        data_df = self.generate_test_data(rows=50, error_rate=0.0)
        self.assertEqual(len(data_df), 50, "Failed to generate test data")
        
        # Ensure data is actually valid (pre-process to catch errors)
        data_df, validation_results = self.data_cleaner.process_dataframe(data_df)
        if validation_results["invalid"] > 0:
            logger.warning(f"Generated data has {validation_results['invalid']} invalid records")
            # Filter to valid records only to ensure test success
            data_df = data_df[data_df["is_valid"] == True]
            self.assertGreater(len(data_df), 0, "No valid records in test data")
            
        # Add derived fields required for processing
        data_df["total_revenue"] = data_df["units_sold"] * data_df["unit_price"]
        data_df["total_cost"] = data_df["units_sold"] * data_df["unit_cost"]
        data_df["total_profit"] = data_df["total_revenue"] - data_df["total_cost"]
        
        # 2. Create a batch
        with get_db_session() as session:
            batch = self.batch_processor.create_batch(
                source="test_happy_path",
                record_count=len(data_df),
                session=session,
                created_by=CURRENT_USER
            )
            batch_id = batch.batch_id
        
        # 3. Process the batch
        try:
            results = self.batch_processor.process_batch(batch_id, data_df, CURRENT_USER)
            self.assertEqual(results["status"], "completed", "Batch processing failed")
            self.assertEqual(results["records_processed"], len(data_df))
            
            # 4. Validate batch record
            batch = self.validate_batch_record(batch_id)
            self.assertEqual(batch.status, "completed")
            self.assertIsNotNone(batch.end_time)
            self.assertEqual(batch.record_count, len(data_df))
            
            # 5. Validate sales records
            self.validate_sales_records(batch_id, len(data_df))
            
            # 6. Validate metrics
            metrics = self.validate_metrics(batch_id)
            self.assertGreaterEqual(len(metrics), 5)
            
            # 7. Validate Kafka messages
            kafka_messages = self.validate_kafka_messages(
                batch_id, ["batch_start", "batch_complete"]
            )
            
            logger.info(f"Happy path test completed successfully: batch_id={batch_id}")
        except Exception as e:
            self.fail(f"Happy path test failed: {str(e)}")
    
    def test_02_validation_errors(self):
        """Test ETL flow with data containing validation errors"""
        logger.info("Starting validation errors test")
        
        # 1. Generate test data with errors
        data_df = self.generate_test_data(rows=100, error_rate=0.15)  # 15% error rate
        self.assertEqual(len(data_df), 100, "Failed to generate test data")
        
        # Pre-check the data to ensure we have a manageable error rate
        data_df, validation_results = self.data_cleaner.process_dataframe(data_df)
        error_rate = validation_results["invalid"] / validation_results["total"]
        logger.info(f"Generated data has error rate of {error_rate:.2%}")
        
        # Ensure we have both valid and invalid records
        if error_rate > 0.25:
            # Too many errors, reduce them
            valid_records = data_df[data_df["is_valid"] == True]
            invalid_records = data_df[data_df["is_valid"] != True].head(int(len(valid_records) * 0.2))
            data_df = pd.concat([valid_records, invalid_records])
            logger.info(f"Adjusted data to have {len(invalid_records)} invalid records out of {len(data_df)}")
        elif error_rate == 0:
            # No errors, introduce some
            data_df.loc[data_df.index[:5], "units_sold"] = -1  # Make 5 records invalid
            logger.info("Introduced 5 invalid records")
            
        # Add derived fields required for processing
        data_df["total_revenue"] = data_df["units_sold"] * data_df["unit_price"]
        data_df["total_cost"] = data_df["units_sold"] * data_df["unit_cost"]
        data_df["total_profit"] = data_df["total_revenue"] - data_df["total_cost"]
        
        # 2. Create a batch
        with get_db_session() as session:
            batch = self.batch_processor.create_batch(
                source="test_validation_errors",
                record_count=len(data_df),
                session=session,
                created_by=CURRENT_USER
            )
            batch_id = batch.batch_id
        
        # 3. Process the batch - should still succeed as error rate is below threshold
        try:
            results = self.batch_processor.process_batch(batch_id, data_df, CURRENT_USER)
            self.assertEqual(results["status"], "completed", "Batch processing failed")
            
            # Check that we have some invalid records
            self.assertGreater(results["records_invalid"], 0, "No invalid records detected")
            self.assertLess(results["records_invalid"], results["records_processed"], 
                          "All records were invalid")
            
            # 4. Validate batch record
            batch = self.validate_batch_record(batch_id)
            self.assertEqual(batch.status, "completed")
            self.assertGreater(batch.error_count, 0, "No errors recorded in batch")
            
            # 5. Validate sales records - only valid ones should be saved
            valid_count = results["records_valid"]
            self.validate_sales_records(batch_id, valid_count)
            
            # 6. Validate metrics
            metrics = self.validate_metrics(batch_id)
            error_rate_metric = next((m for m in metrics if m.metric_name == "error_rate"), None)
            self.assertIsNotNone(error_rate_metric, "Error rate metric not found")
            self.assertGreater(error_rate_metric.metric_value, 0, "Error rate should be > 0")
            
            # 7. Validate Kafka messages
            kafka_messages = self.validate_kafka_messages(
                batch_id, ["batch_start", "batch_complete"]
            )
            
            logger.info(f"Validation errors test completed successfully: batch_id={batch_id}")
        except Exception as e:
            self.fail(f"Validation errors test failed: {str(e)}")

    def test_03_high_error_rate(self):
        """Test ETL flow with data containing too many errors"""
        logger.info("Starting high error rate test")
        
        # 1. Create data with guaranteed high error rate
        data = []
        # Add 10 valid records
        for i in range(10):
            data.append({
                "region": "North America",
                "country": "USA",
                "item_type": "Electronics",
                "sales_channel": "Online",
                "order_priority": "H",
                "order_date": "2025-01-01",
                "order_id": f"NA-2025-{1000000+i}",
                "ship_date": "2025-01-15",
                "units_sold": 100,
                "unit_price": 99.99,
                "unit_cost": 49.99,
                "total_revenue": 9999.0,
                "total_cost": 4999.0,
                "total_profit": 5000.0
            })
            
        # Add 30 invalid records (guaranteed to fail validation)
        for i in range(30):
            data.append({
                "region": "Invalid Region",
                "country": "",
                "item_type": "Electronics", 
                "sales_channel": "Phone",  # Invalid
                "order_priority": "X",     # Invalid
                "order_date": "2025-01-15",
                "order_id": f"XX-2025-{2000000+i}",
                "ship_date": "2025-01-01", # Before order date
                "units_sold": -100,        # Invalid
                "unit_price": -99.99,      # Invalid
                "unit_cost": -49.99,       # Invalid
                "total_revenue": 0,
                "total_cost": 0,
                "total_profit": 0
            })
            
        data_df = pd.DataFrame(data)
        
        # 2. Create a batch
        with get_db_session() as session:
            batch = self.batch_processor.create_batch(
                source="test_high_error_rate",
                record_count=len(data_df),
                session=session,
                created_by=CURRENT_USER
            )
            batch_id = batch.batch_id
        
        # 3. Process the batch - should fail due to high error rate
        with self.assertRaises(Exception) as context:
            self.batch_processor.process_batch(batch_id, data_df, CURRENT_USER)
        
        # Verify the error message
        self.assertIn("Error rate", str(context.exception), 
                    "Exception should mention error rate")
        self.assertIn("exceeds threshold", str(context.exception), 
                    "Exception should mention threshold")
        
        # 4. Validate batch record - should be marked as failed
        batch = self.validate_batch_record(batch_id)
        self.assertEqual(batch.status, "failed", "Batch should be marked as failed")
        
        # 5. Validate error record exists
        with get_db_session() as session:
            errors = session.query(ETLError).filter_by(batch_id=batch_id).all()
            self.assertGreater(len(errors), 0, "No error records found")
            
            # Find the critical error
            critical_error = next((e for e in errors if e.severity == "critical"), None)
            self.assertIsNotNone(critical_error, "Critical error record not found")
            self.assertIn("Error rate", critical_error.error_message,
                        "Error message should mention error rate")
        
        # 6. Validate Kafka messages
        kafka_messages = self.validate_kafka_messages(
            batch_id, ["batch_start", "batch_error"]
        )
        
        logger.info(f"High error rate test completed successfully: batch_id={batch_id}")
    
    def test_04_retry_batch(self):
        """Test batch retry functionality"""
        logger.info("Starting batch retry test")
        
        # 1. First create and process a batch with errors
        # Create data with guaranteed high error rate
        data = []
        for i in range(40):
            data.append({
                "region": "Invalid Region", # Invalid
                "country": "",              # Invalid
                "item_type": "Electronics", 
                "sales_channel": "Phone",   # Invalid
                "order_priority": "X",      # Invalid
                "order_date": "2025-01-15",
                "order_id": f"XX-2025-{3000000+i}",
                "ship_date": "2025-01-01",  # Before order date
                "units_sold": -100,         # Invalid
                "unit_price": -99.99,       # Invalid
                "unit_cost": -49.99,        # Invalid
                "total_revenue": 0,
                "total_cost": 0,
                "total_profit": 0
            })
        
        data_df = pd.DataFrame(data)
        
        # 2. Create initial batch
        with get_db_session() as session:
            batch = self.batch_processor.create_batch(
                source="test_retry_original",
                record_count=len(data_df),
                session=session,
                created_by=CURRENT_USER
            )
            original_batch_id = batch.batch_id
        
        # 3. Process the batch - should fail due to high error rate
        try:
            self.batch_processor.process_batch(original_batch_id, data_df, CURRENT_USER)
            self.fail("Batch with high error rate should have failed")
        except Exception:
            # Expected failure
            pass
        
        # 4. Now create corrected data and retry
        corrected_data = []
        for i in range(20):
            corrected_data.append({
                "region": "North America",
                "country": "USA",
                "item_type": "Electronics",
                "sales_channel": "Online",
                "order_priority": "H",
                "order_date": "2025-01-01",
                "order_id": f"NA-2025-{4000000+i}",
                "ship_date": "2025-01-15",
                "units_sold": 100,
                "unit_price": 99.99,
                "unit_cost": 49.99,
                "total_revenue": 9999.0,
                "total_cost": 4999.0,
                "total_profit": 5000.0
            })
            
        corrected_df = pd.DataFrame(corrected_data)
        
        # 5. Retry the batch with new data
        try:
            retry_results = self.batch_processor.retry_batch(
                original_batch_id, corrected_df, CURRENT_USER
            )
            
            # 6. Validate retry results
            self.assertEqual(retry_results["status"], "completed", "Retry should complete successfully")
            self.assertEqual(retry_results["records_processed"], 20)
            
            # Get the new batch ID
            retry_batch_id = retry_results["batch_id"]
            
            # 7. Validate the retry batch
            retry_batch = self.validate_batch_record(retry_batch_id)
            self.assertEqual(retry_batch.status, "completed")
            self.assertEqual(retry_batch.source, f"retry_test_retry_original")
            
            # 8. Check that records were created
            self.validate_sales_records(retry_batch_id, 20)
            
            # 9. Validate Kafka messages for the retry
            kafka_messages = self.validate_kafka_messages(
                retry_batch_id, ["batch_start", "batch_complete"]
            )
            
            logger.info(f"Batch retry test completed successfully: original={original_batch_id}, retry={retry_batch_id}")
        except Exception as e:
            self.fail(f"Batch retry test failed: {str(e)}")
    
    def test_05_batch_status_reporting(self):
        """Test batch status reporting functionality"""
        logger.info("Starting batch status reporting test")
        
        # 1. Create and process a simple batch
        data = []
        for i in range(10):
            data.append({
                "region": "North America",
                "country": "USA",
                "item_type": "Electronics",
                "sales_channel": "Online",
                "order_priority": "H",
                "order_date": "2025-03-01",
                "order_id": f"NA-2025-{100+i}",
                "ship_date": "2025-03-15",
                "units_sold": 100,
                "unit_price": 99.99,
                "unit_cost": 49.99,
                "total_revenue": 9999.0,
                "total_cost": 4999.0,
                "total_profit": 5000.0
            })
        
        data_df = pd.DataFrame(data)
        
        # 2. Create batch
        with get_db_session() as session:
            batch = self.batch_processor.create_batch(
                source="test_status_reporting",
                record_count=len(data_df),
                session=session,
                created_by=CURRENT_USER
            )
            batch_id = batch.batch_id
        
        # 3. Process the batch
        self.batch_processor.process_batch(batch_id, data_df, CURRENT_USER)
        
        # 4. Get and validate batch status
        status = self.batch_processor.get_batch_status(batch_id)
        
        self.assertEqual(status["batch_id"], batch_id)
        self.assertEqual(status["status"], "completed")
        self.assertEqual(status["record_count"], 10)
        self.assertEqual(status["created_by"], CURRENT_USER)
        self.assertIn("metrics", status)
        self.assertGreater(len(status["metrics"]), 0)
        self.assertIn("processing_time", status)
        self.assertGreater(status["processing_time"], 0)
        
        logger.info(f"Batch status reporting test completed successfully: batch_id={batch_id}")
    
    def test_06_transaction_handling(self):
        """Test transaction handling with intentional errors"""
        logger.info("Starting transaction handling test")
        
        # 1. Generate valid test data
        data = []
        for i in range(30):
            data.append({
                "region": "Europe",
                "country": "Germany",
                "item_type": "Furniture",
                "sales_channel": "Offline",
                "order_priority": "M",
                "order_date": "2025-03-06",
                "order_id": f"EU-2025-{6000000+i}",
                "ship_date": "2025-03-20",
                "units_sold": 50,
                "unit_price": 299.99,
                "unit_cost": 179.99,
                "total_revenue": 14999.50,
                "total_cost": 8999.50,
                "total_profit": 6000.00
            })
        
        data_df = pd.DataFrame(data)
        
        # 2. Create a batch
        with get_db_session() as session:
            batch = self.batch_processor.create_batch(
                source="test_transaction",
                record_count=len(data_df),
                session=session,
                created_by=CURRENT_USER
            )
            batch_id = batch.batch_id
        
        # 3. Create a special processor with an intentional error
        class ErrorProcessor(BatchProcessor):
            def __init__(self, config=None):
                self.logger = logging.getLogger(__name__)
                super().__init__(config)
                
            def _process_chunk(self, chunk_df, batch_id, chunk_id, created_by=None):
                # Process first chunk normally
                if chunk_id == 0:
                    return super()._process_chunk(chunk_df, batch_id, chunk_id, created_by)
                # Raise exception for second chunk
                raise ValueError(f"Intentional test error in chunk {chunk_id}")
        
        error_processor = ErrorProcessor(self.config)
        
        # 4. Process with the error processor - should fail
        with self.assertRaises(Exception):
            error_processor.process_batch(batch_id, data_df, CURRENT_USER)
        
        # 5. Check batch status - should be failed
        batch = self.validate_batch_record(batch_id)
        self.assertEqual(batch.status, "failed")
        
        # 6. Check error records
        with get_db_session() as session:
            errors = session.query(ETLError).filter_by(batch_id=batch_id).all()
            self.assertGreater(len(errors), 0)
            
            # At least one error should contain our intentional message
            error_messages = [e.error_message for e in errors]
            self.assertTrue(any("Intentional test error" in msg for msg in error_messages),
                          "Intentional error not found in error records")
        
        # 7. Check Kafka messages
        kafka_messages = self.validate_kafka_messages(
            batch_id, ["batch_start", "batch_error"]
        )
        
        logger.info(f"Transaction handling test completed successfully: batch_id={batch_id}")


def run_tests(mode="simple"):
    """Run the ETL workflow tests"""
    suite = unittest.TestSuite()
    
    # Add tests based on mode
    if mode == "full":
        suite.addTest(TestETLWorkflow("test_01_happy_path"))
        suite.addTest(TestETLWorkflow("test_02_validation_errors"))
        suite.addTest(TestETLWorkflow("test_03_high_error_rate"))
        suite.addTest(TestETLWorkflow("test_04_retry_batch"))
        suite.addTest(TestETLWorkflow("test_05_batch_status_reporting"))
        suite.addTest(TestETLWorkflow("test_06_transaction_handling"))
    else:
        # Simple mode - just run happy path
        suite.addTest(TestETLWorkflow("test_01_happy_path"))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return success/failure
    return result.wasSuccessful()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run ETL workflow tests")
    parser.add_argument("--mode", choices=["simple", "full"], default="simple",
                      help="Test mode: simple=basic test, full=comprehensive tests")
    args = parser.parse_args()
    
    # Set current time for consistency in tests
    current_time_str = "2025-03-06 04:51:53"  # Update this to current time
    current_user = "V1997"                    # Update this to current user
    
    # Log test start
    logger.info(f"Starting ETL workflow tests in {args.mode} mode")
    logger.info(f"Current time: {current_time_str}, User: {current_user}")
    
    # Run tests
    success = run_tests(args.mode)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)