import json
import logging
from typing import Dict, Any, Callable, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException
import threading
import uuid
from datetime import datetime
import asyncio

from ..db.database import get_db
from ..core.models.sales import SalesRecord, ETLJob, ErrorLog
from ..core.etl.transform import DataTransformer
from ..core.etl.load import DataLoader
from ..config.settings import settings

logger = logging.getLogger(__name__)

class KafkaConsumerService:
    """Service for consuming messages from Kafka topics"""
    
    def __init__(self, topic=None, group_id=None):
        """Initialize Kafka consumer"""
        self.topic = topic or settings.KAFKA_TOPIC_SALES
        self.group_id = group_id or settings.KAFKA_CONSUMER_GROUP
        self.running = False
        self.consumer = None
        self.consumer_thread = None
        
        # Initialize transform and load services
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        
        logger.info(f"Kafka consumer initialized for topic {self.topic}")
    
    def start(self, message_handler: Optional[Callable] = None):
        """
        Start consuming messages in a separate thread
        
        Args:
            message_handler: Optional custom message handler function 
                             that takes a message value as argument
        """
        if self.running:
            logger.warning("Consumer is already running")
            return
        
        self.running = True
        self.handler = message_handler or self._default_message_handler
        
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f"{self.group_id}-{uuid.uuid4()}",
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,  # Manual commit for better control
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,  # 30 seconds
        })
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        
        # Start consumer in a separate thread
        self.consumer_thread = threading.Thread(
            target=self._consume_loop,
            daemon=True  # Thread will terminate when main thread exits
        )
        self.consumer_thread.start()
        
        logger.info(f"Started Kafka consumer for topic {self.topic}")
    
    def stop(self):
        """Stop the consumer thread"""
        if not self.running:
            logger.warning("Consumer is not running")
            return
        
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=10)
            
        if self.consumer:
            self.consumer.close()
            
        logger.info(f"Stopped Kafka consumer for topic {self.topic}")
        
    def _consume_loop(self):
        """Main consumption loop running asynchronously"""
        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        # Log actual error
                        logger.error(f"Consumer error: {msg.error()}")
                        self._log_error("CONSUMER_ERROR", f"Kafka consumer error: {msg.error()}")
                else:
                    try:
                        # Process message
                        message_value = msg.value().decode('utf-8')
                        self.handler(message_value)  # Corrected from `asyncio.run`

                        # Commit offset after successful processing
                        self.consumer.commit(msg)
                    
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        self._log_error("MESSAGE_PROCESSING_ERROR", f"Failed to process message: {str(e)}", 
                                            {"message": message_value if 'message_value' in locals() else None})
        
        except KafkaException as e:
            logger.error(f"Kafka consumer exception: {str(e)}")
            self._log_error("KAFKA_CONSUMER_EXCEPTION", f"Kafka consumer exception: {str(e)}")
            
        except Exception as e:
            logger.error(f"Unexpected consumer exception: {str(e)}")
            self._log_error("CONSUMER_EXCEPTION", f"Unexpected consumer exception: {str(e)}")
            
        finally:
            if self.consumer:
                try:
                    self.consumer.close()
                except:
                    pass

    def _default_message_handler(self, message_value: str):
        """
        Default message handler for sales records
        
        Args:
            message_value: JSON string containing the sales record data
        """
        try:
            # Parse JSON message
            data = json.loads(message_value)
            
            # Create streaming job
            job_id = str(uuid.uuid4())
            
            with get_db() as db:
                # Create ETL job record
                etl_job = ETLJob(
                    job_id=job_id,
                    job_type="STREAM",
                    status="RUNNING",
                    start_time=datetime.utcnow()
                )
                db.add(etl_job)
                db.commit()
            
            # Process the record (transformation)
            try:
                # Apply transformations similar to batch process
                transformed_data = self._transform_record(data, job_id)
                # Serialize data with fallback for unsupported types

                # Load the transformed record
                self._load_record(transformed_data, job_id)
                
                # Update job status
                with get_db() as db:
                    db.query(ETLJob).filter(ETLJob.job_id == job_id).update({
                        "records_processed": 1,
                        "status": "COMPLETED",
                        "end_time": datetime.utcnow()
                    })
                    db.commit()
                
                logger.info(f"Successfully processed streaming record {data.get('order_id', 'unknown')} for job {job_id}")
                
            except Exception as e:
                with get_db() as db:
                    # Update job status
                    db.query(ETLJob).filter(ETLJob.job_id == job_id).update({
                        "records_failed": 1,
                        "status": "FAILED",
                        "end_time": datetime.utcnow(),
                        "error_message": str(e)
                    })
                    db.commit()
                
                logger.error(f"Failed to process streaming record for job {job_id}: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"Error in default message handler: {str(e)}")
            self._log_error("MESSAGE_HANDLER_ERROR", f"Error in default message handler: {str(e)}", 
                           {"message": message_value})
    
    def _transform_record(self, record: Dict[str, Any], job_id: str) -> Dict[str, Any]:
        """Apply transformations to a single record"""
        # Basic data validation
        required_fields = ['region', 'country', 'item_type', 'sales_channel', 
                          'order_priority', 'order_date', 'order_id', 'ship_date',
                          'units_sold', 'unit_price', 'unit_cost']
        
        missing_fields = [field for field in required_fields if field not in record]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
        # Apply transformations similar to batch processing
        # Data type conversions
        
        try:
            if isinstance(record['order_date'], str):
                record['order_date'] = datetime.strptime(record['order_date'], "%Y-%m-%d").isoformat()
            if isinstance(record['ship_date'], str):
                record['ship_date'] = datetime.strptime(record['ship_date'], "%Y-%m-%d").isoformat()
        except ValueError:
            raise ValueError("Invalid date format. Expected YYYY-MM-DD")
        
        # Data cleaning
        record['region'] = record['region'].strip().title() if isinstance(record['region'], str) else 'Unknown'
        record['country'] = record['country'].strip().title() if isinstance(record['country'], str) else 'Unknown'
        record['item_type'] = record['item_type'].strip().title() if isinstance(record['item_type'], str) else 'Unknown'
        record['sales_channel'] = record['sales_channel'].strip().title() if isinstance(record['sales_channel'], str) else 'Unknown'
        record['order_priority'] = record['order_priority'].strip().upper()[0] if isinstance(record['order_priority'], str) else 'M'
        
        # Validate order priority
        valid_priorities = ['L', 'M', 'H', 'C']
        if record['order_priority'] not in valid_priorities:
            record['order_priority'] = 'M'  # Default to Medium if invalid
        
        # Convert numeric values
        try:
            record['units_sold'] = int(record['units_sold'])
            record['unit_price'] = float(record['unit_price'])
            record['unit_cost'] = float(record['unit_cost'])
        except (ValueError, TypeError):
            raise ValueError("Invalid numeric values for units_sold, unit_price, or unit_cost")
        
        # Calculate derived fields
        record['total_revenue'] = record['units_sold'] * record['unit_price']
        record['total_cost'] = record['units_sold'] * record['unit_cost']
        record['total_profit'] = record['total_revenue'] - record['total_cost']
        
        # Add transformation metadata
        record['etl_job_id'] = job_id
        record['etl_transformed_at'] = datetime.now().isoformat()
        
        # Check for data quality issues
        has_quality_issues = False
        quality_issues = []
        
        if record['units_sold'] <= 0:
            has_quality_issues = True
            quality_issues.append("units_sold is not positive")
        
        if record['unit_price'] <= 0:
            has_quality_issues = True
            quality_issues.append("unit_price is not positive")
            
        if record['unit_cost'] <= 0:
            has_quality_issues = True
            quality_issues.append("unit_cost is not positive")
            
        if record['ship_date'] < record['order_date']:
            has_quality_issues = True
            quality_issues.append("ship_date is before order_date")
        
        # Log quality issues if any
        if has_quality_issues:
            with get_db() as db:
                error_log = ErrorLog(
                    job_id=job_id,
                    error_type="DATA_QUALITY_ISSUE",
                    error_message=f"Data quality issues in streaming record: {', '.join(quality_issues)}",
                    severity="MEDIUM",
                    record_data=record
                )
                db.add(error_log)
                db.commit()
        
        return record
    
    def _load_record(self, record: Dict[str, Any], job_id: str) -> None:
        """Load a single record into the database"""
        try:
            # Prepare record for database insertion
            db_record = {
                'order_id': record['order_id'],
                'region': record['region'],
                'country': record['country'],
                'item_type': record['item_type'],
                'sales_channel': record['sales_channel'],
                'order_priority': record['order_priority'],
                'order_date': record['order_date'],
                'ship_date': record['ship_date'],
                'units_sold': record['units_sold'],
                'unit_price': record['unit_price'],
                'unit_cost': record['unit_cost'],
                'total_revenue': record['total_revenue'],
                'total_cost': record['total_cost'],
                'total_profit': record['total_profit']
            }
            
            # Use SQLAlchemy core for upsert operation
            from sqlalchemy.dialects.mysql import insert
            from ..core.models.sales import SalesRecord
            
            with get_db() as db:
                # Create upsert statement
                insert_stmt = insert(SalesRecord.__table__).values(**db_record)
                
                # On duplicate key, update all fields
                on_duplicate_stmt = insert_stmt.on_duplicate_key_update(
                    region=insert_stmt.inserted.region,
                    country=insert_stmt.inserted.country,
                    item_type=insert_stmt.inserted.item_type,
                    sales_channel=insert_stmt.inserted.sales_channel,
                    order_priority=insert_stmt.inserted.order_priority,
                    order_date=insert_stmt.inserted.order_date,
                    ship_date=insert_stmt.inserted.ship_date,
                    units_sold=insert_stmt.inserted.units_sold,
                    unit_price=insert_stmt.inserted.unit_price,
                    unit_cost=insert_stmt.inserted.unit_cost,
                    total_revenue=insert_stmt.inserted.total_revenue,
                    total_cost=insert_stmt.inserted.total_cost,
                    total_profit=insert_stmt.inserted.total_profit,
                    updated_at=datetime.utcnow()
                )
                
                # Execute upsert
                db.execute(on_duplicate_stmt)
                db.commit()
            
            # Update analytics (asynchronously to not block consumer)
            self._update_streaming_analytics(record)
            
        except Exception as e:
            logger.error(f"Error loading record to database: {str(e)}")
            with get_db() as db:
                error_log = ErrorLog(
                    job_id=job_id,
                    error_type="RECORD_LOADING_ERROR",
                    error_message=f"Failed to load record to database: {str(e)}",
                    severity="HIGH",
                    record_data=record
                )
                db.add(error_log)
                db.commit()
            raise
    
    def _update_streaming_analytics(self, record: Dict[str, Any]) -> None:
        """Update analytics tables for streaming data"""
        from sqlalchemy import text
        from ..db.database import engine
        
        try:
            # Extract dimension values
            region = record['region']
            country = record['country']
            item_type = record['item_type']
            sales_channel = record['sales_channel']
            order_date = record['order_date']
            
            # Format for analytics
            if isinstance(order_date, datetime):
                period_start = order_date.replace(day=1)
                month_str = order_date.strftime('%Y-%m')
            else:
                period_start = datetime.strptime(f"{order_date[:7]}-01", "%Y-%m-%d")
                month_str = order_date[:7]
            
            # Use raw SQL for better performance on analytics updates
            with engine.connect() as connection:
                # Update region analytics
                connection.execute(text("""
                    INSERT INTO sales_analytics 
                        (dimension_type, dimension_value, time_period, period_start, period_end, 
                         total_sales, total_cost, total_profit, units_sold, avg_order_value)
                    VALUES 
                        ('REGION', :region, 'MONTHLY', :period_start, LAST_DAY(:period_start), 
                         :revenue, :cost, :profit, :units, :revenue)
                    ON DUPLICATE KEY UPDATE
                        total_sales = total_sales + :revenue,
                        total_cost = total_cost + :cost,
                        total_profit = total_profit + :profit,
                        units_sold = units_sold + :units,
                        avg_order_value = (total_sales + :revenue) / (units_sold + :units),
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'region': region,
                    'period_start': period_start,
                    'revenue': record['total_revenue'],
                    'cost': record['total_cost'],
                    'profit': record['total_profit'],
                    'units': record['units_sold']
                })
                
                # Update country analytics (similar structure to region)
                connection.execute(text("""
                    INSERT INTO sales_analytics 
                        (dimension_type, dimension_value, time_period, period_start, period_end, 
                         total_sales, total_cost, total_profit, units_sold, avg_order_value)
                    VALUES 
                        ('COUNTRY', :country, 'MONTHLY', :period_start, LAST_DAY(:period_start), 
                         :revenue, :cost, :profit, :units, :revenue)
                    ON DUPLICATE KEY UPDATE
                        total_sales = total_sales + :revenue,
                        total_cost = total_cost + :cost,
                        total_profit = total_profit + :profit,
                        units_sold = units_sold + :units,
                        avg_order_value = (total_sales + :revenue) / (units_sold + :units),
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'country': country,
                    'period_start': period_start,
                    'revenue': record['total_revenue'],
                    'cost': record['total_cost'],
                    'profit': record['total_profit'],
                    'units': record['units_sold']
                })
                
                # Update item_type analytics
                connection.execute(text("""
                    INSERT INTO sales_analytics 
                        (dimension_type, dimension_value, time_period, period_start, period_end, 
                         total_sales, total_cost, total_profit, units_sold, avg_order_value)
                    VALUES 
                        ('ITEM_TYPE', :item_type, 'MONTHLY', :period_start, LAST_DAY(:period_start), 
                         :revenue, :cost, :profit, :units, :revenue)
                    ON DUPLICATE KEY UPDATE
                        total_sales = total_sales + :revenue,
                        total_cost = total_cost + :cost,
                        total_profit = total_profit + :profit,
                        units_sold = units_sold + :units,
                        avg_order_value = (total_sales + :revenue) / (units_sold + :units),
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'item_type': item_type,
                    'period_start': period_start,
                    'revenue': record['total_revenue'],
                    'cost': record['total_cost'],
                    'profit': record['total_profit'],
                    'units': record['units_sold']
                })
                
                # Update sales_channel analytics
                connection.execute(text("""
                    INSERT INTO sales_analytics 
                        (dimension_type, dimension_value, time_period, period_start, period_end, 
                         total_sales, total_cost, total_profit, units_sold, avg_order_value)
                    VALUES 
                        ('SALES_CHANNEL', :sales_channel, 'MONTHLY', :period_start, LAST_DAY(:period_start), 
                         :revenue, :cost, :profit, :units, :revenue)
                    ON DUPLICATE KEY UPDATE
                        total_sales = total_sales + :revenue,
                        total_cost = total_cost + :cost,
                        total_profit = total_profit + :profit,
                        units_sold = units_sold + :units,
                        avg_order_value = (total_sales + :revenue) / (units_sold + :units),
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'sales_channel': sales_channel,
                    'period_start': period_start,
                    'revenue': record['total_revenue'],
                    'cost': record['total_cost'],
                    'profit': record['total_profit'],
                    'units': record['units_sold']
                })
                
                # Update date-based analytics
                connection.execute(text("""
                    INSERT INTO sales_analytics 
                        (dimension_type, dimension_value, time_period, period_start, period_end, total_sales, total_cost, total_profit, units_sold, avg_order_value)
                    VALUES 
                        ('DATE', :month, 'MONTHLY', :period_start, LAST_DAY(:period_start), 
                         :revenue, :cost, :profit, :units, :revenue)
                    ON DUPLICATE KEY UPDATE
                        total_sales = total_sales + :revenue,
                        total_cost = total_cost + :cost,
                        total_profit = total_profit + :profit,
                        units_sold = units_sold + :units,
                        avg_order_value = (total_sales + :revenue) / (units_sold + :units),
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'month': month_str,
                    'period_start': period_start,
                    'revenue': record['total_revenue'],
                    'cost': record['total_cost'],
                    'profit': record['total_profit'],
                    'units': record['units_sold']
                })
                
                connection.commit()
            
        except Exception as e:
            logger.error(f"Error updating streaming analytics: {str(e)}")
            self._log_error("ANALYTICS_UPDATE_ERROR", f"Failed to update streaming analytics: {str(e)}", 
                           {"record_id": record.get('order_id', 'unknown')})
    
    def _log_error(self, error_type: str, error_message: str, error_details: Dict = None) -> None:
        """Log error to database"""
        try:
            with get_db() as db:
                error_log = ErrorLog(
                    job_id=f"consumer-{uuid.uuid4()}",
                    error_type=error_type,
                    error_message=error_message,
                    severity="HIGH",
                    error_details=error_details
                )
                db.add(error_log)
                db.commit()
        except Exception as e:
            logger.error(f"Failed to log error to database: {str(e)}")



                        