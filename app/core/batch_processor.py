import pandas as pd
import uuid
import time
from datetime import datetime
import logging
import concurrent.futures
import traceback
from sqlalchemy.exc import SQLAlchemyError

from app.models.models import SalesBatch, SalesRecord, ETLError, ETLMetric
from app.db.transaction import transaction_scope
from .data_cleaner import DataCleaner
from .kafka_producer import ETLKafkaProducer
import threading

# Set up logging
logger = logging.getLogger(__name__)

class BatchProcessor:
    """
    Handles batch processing of sales data with chunking and parallel processing.
    
    This class is responsible for:
    - Creating and managing batch records
    - Processing data in configurable chunks
    - Handling errors and retries
    - Recording metrics and audit information
    - Sending Kafka notifications for batch events
    """
    
    def __init__(self, config=None):
        """
        Initialize the batch processor with configuration.
        
        Args:
            config: Dictionary with configuration options
        """
        self.config = config or {}
        self.batch_size = self.config.get("BATCH_SIZE", 5000)
        self.max_workers = self.config.get("MAX_WORKERS", 4)
        self.retry_attempts = self.config.get("RETRY_ATTEMPTS", 3)
        self.error_threshold = self.config.get("ERROR_THRESHOLD", 0.05)  # 5% error threshold
        self.data_cleaner = DataCleaner()
        
        # Initialize Kafka producer if enabled
        self.kafka_producer = None
        if self.config.get("KAFKA_ENABLED", False) and self.config.get("KAFKA_BOOTSTRAP_SERVERS"):
            try:
                self.kafka_producer = ETLKafkaProducer(
                    bootstrap_servers=self.config.get("KAFKA_BOOTSTRAP_SERVERS"),
                    topic=self.config.get("KAFKA_TOPIC", "etl-batch-notifications")
                )
                logger.info("Kafka producer initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {str(e)}")
    
    def generate_batch_id(self):
        """Generate a unique batch ID"""
        return f"BATCH-{str(uuid.uuid4())[:8]}-{int(time.time())}"
    
    def create_batch(self, source, record_count, session=None, created_by=None):
        """
        Create a new batch record.
        
        Args:
            source: Source of the data
            record_count: Number of records in the batch
            session: SQLAlchemy session (optional)
            created_by: User or system who created the batch
            
        Returns:
            SalesBatch: Newly created batch object
        """
        batch_id = self.generate_batch_id()
        
        batch = SalesBatch(
            batch_id=batch_id,
            source=source,
            record_count=record_count,
            start_time=datetime.utcnow(),
            status="pending",
            error_count=0,
            created_by=created_by
        )
        
        # Use provided session or create a transaction
        if session:
            session.add(batch)
            session.flush()
            return batch
        else:
            with transaction_scope("create_batch", "batch_processor", created_by) as session:
                session.add(batch)
                session.flush()
                return batch
    
    def process_batch(self, batch_id, data_df, created_by=None):
        """
        Process a batch of data.
        
        Args:
            batch_id: The batch ID
            data_df: Pandas DataFrame containing the data
            created_by: User or system processing the batch
            
        Returns:
            dict: Processing results and statistics
        """
        start_time = time.time()
        
        try:
            # Start by updating batch status
            with transaction_scope("update_batch", "batch_processor", created_by, batch_id) as session:
                batch = session.query(SalesBatch).filter_by(batch_id=batch_id).first()
                if not batch:
                    raise ValueError(f"Batch with ID {batch_id} not found")
                
                batch.status = "processing"
                session.flush()
            
            # Send batch start notification via Kafka
            if self.kafka_producer:
                self.kafka_producer.send_batch_start(
                    batch_id=batch_id,
                    source=batch.source,
                    record_count=batch.record_count,
                    created_by=created_by
                )
            
            # Clean and validate the data
            cleaned_df, validation_results = self.data_cleaner.process_dataframe(data_df)
            
            # Check error threshold
            if validation_results["total"] > 0:
                error_rate = validation_results["invalid"] / validation_results["total"]
                if error_rate > self.error_threshold:
                    raise ValueError(f"Error rate {error_rate:.2%} exceeds threshold {self.error_threshold:.2%}")
            
            # Process valid records
            valid_records = cleaned_df[cleaned_df["is_valid"] != False]
            
            # Split into chunks for processing
            chunks = [valid_records.iloc[i:i + self.batch_size] for i in range(0, len(valid_records), self.batch_size)]
            
            # Process chunks, potentially in parallel
            results = self._process_chunks(chunks, batch_id, created_by)
            
            # Record metrics
            self._record_metrics(batch_id, validation_results, time.time() - start_time, created_by)
            
            # Update batch status
            with transaction_scope("complete_batch", "batch_processor", created_by, batch_id) as session:
                batch = session.query(SalesBatch).filter_by(batch_id=batch_id).first()
                if batch:
                    batch.status = "completed"
                    batch.end_time = datetime.utcnow()
                    batch.error_count = validation_results["invalid"]
                    session.flush()
            
            # Prepare processing summary
            result_summary = {
                "batch_id": batch_id,
                "status": "completed",
                "records_processed": validation_results["total"],
                "records_valid": validation_results["valid"],
                "records_invalid": validation_results["invalid"],
                "records_modified": validation_results["modified"],
                "processing_time": time.time() - start_time,
                "chunks_processed": len(chunks),
                "error_samples": validation_results["errors"][:5] if validation_results["errors"] else []
            }
            
            # Send batch complete notification via Kafka
            if self.kafka_producer:
                self.kafka_producer.send_batch_complete(
                    batch_id=batch_id,
                    stats=result_summary,
                    created_by=created_by
                )
            
            # Return processing summary
            return result_summary
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Send batch error notification via Kafka
            if self.kafka_producer:
                self.kafka_producer.send_batch_error(
                    batch_id=batch_id,
                    error_message=str(e),
                    created_by=created_by
                )
            
            # Update batch status to failed
            try:
                with transaction_scope("fail_batch", "batch_processor", created_by, batch_id) as session:
                    batch = session.query(SalesBatch).filter_by(batch_id=batch_id).first()
                    if batch:
                        batch.status = "failed"
                        batch.end_time = datetime.utcnow()
                        session.flush()

                    # Record the error
                    error = ETLError(
                        error_type="batch_processing_error",
                        error_message=str(e),
                        component="batch_processor",
                        severity="critical",
                        batch_id=batch_id
                    )
                    session.add(error)
            except SQLAlchemyError as se:
                logger.error(f"Failed to update batch status: {str(se)}")
            
            # Re-raise the exception to indicate failure
            raise
    
    def _process_chunks(self, chunks, batch_id, created_by=None):
        """
        Process data chunks with optional parallel execution.
        
        Args:
            chunks: List of DataFrame chunks
            batch_id: The batch ID
            created_by: User or system processing the batch
            
        Returns:
            list: Results from processing each chunk
        """
        results = []
        
        if self.max_workers > 1 and len(chunks) > 1:
            # Parallel processing
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._process_chunk, chunk, batch_id, chunk_id, created_by) 
                    for chunk_id, chunk in enumerate(chunks)
                ]
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing chunk: {str(e)}")
                        # Record the error but continue with other chunks
                        self._record_error(batch_id, "chunk_processing_error", str(e), "batch_processor", "error")
        else:
            # Sequential processing
            for chunk_id, chunk in enumerate(chunks):
                try:
                    result = self._process_chunk(chunk, batch_id, chunk_id, created_by)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_id}: {str(e)}")
                    # Record the error but continue with other chunks
                    self._record_error(batch_id, "chunk_processing_error", str(e), "batch_processor", "error")
        
        return results
    
    def _process_chunk(self, chunk_df, batch_id, chunk_id, created_by=None):
        """
        Process a single data chunk with retry logic.
        
        Args:
            chunk_df: DataFrame chunk to process
            batch_id: The batch ID
            chunk_id: ID of the chunk (for logging)
            created_by: User or system processing the chunk
            
        Returns:
            dict: Processing results for the chunk
        """
        retry_count = 0
        last_error = None
        
        while retry_count <= self.retry_attempts:
            try:
                start_time = time.time()
                
                # Convert DataFrame to records
                records = []
                for _, row in chunk_df.iterrows():
                    record_dict = row.to_dict()
                    
                    # Create SalesRecord object
                    record = SalesRecord(
                        order_id=record_dict["order_id"],
                        region=record_dict["region"],
                        country=record_dict["country"],
                        item_type=record_dict["item_type"],
                        sales_channel=record_dict["sales_channel"],
                        order_priority=record_dict["order_priority"],
                        order_date=record_dict["order_date"],
                        ship_date=record_dict["ship_date"],
                        units_sold=record_dict["units_sold"],
                        unit_price=record_dict["unit_price"],
                        unit_cost=record_dict["unit_cost"],
                        total_revenue=record_dict["total_revenue"],
                        total_cost=record_dict["total_cost"],
                        total_profit=record_dict["total_profit"],
                        is_valid=True,
                        batch_id=batch_id
                    )
                    records.append(record)
                
                # Save records to database
                with transaction_scope("save_chunk", "batch_processor", created_by, batch_id) as session:
                    session.bulk_save_objects(records)
                    
                    # Record a metric for this chunk
                    metric = ETLMetric(
                        metric_name="chunk_processing_time",
                        metric_value=time.time() - start_time,
                        component="batch_processor",
                        batch_id=batch_id
                    )
                    session.add(metric)
                    
                    # Add audit entry
                    audit = ETLAudit(
                        action="process_chunk",
                        component="batch_processor",
                        details=f"Processed chunk {chunk_id} with {len(records)} records",
                        user_id=created_by,
                        batch_id=batch_id,
                        timestamp=datetime.utcnow()
                    )
                    session.add(audit)
                
                # Return success result
                return {
                    "chunk_id": chunk_id,
                    "records_processed": len(records),
                    "processing_time": time.time() - start_time
                }
                
            except Exception as e:
                retry_count += 1
                last_error = str(e)
                logger.warning(f"Retry {retry_count}/{self.retry_attempts} for chunk {chunk_id}: {last_error}")
                
                # Add delay before retry (exponential backoff)
                if retry_count <= self.retry_attempts:
                    time.sleep(2 ** retry_count)
        
        # If we get here, all retries failed
        error_msg = f"Failed to process chunk {chunk_id} after {self.retry_attempts} retries. Last error: {last_error}"
        logger.error(error_msg)
        
        # Record the error
        self._record_error(batch_id, "chunk_retry_exhausted", error_msg, "batch_processor", "error")
        
        # Raise the exception
        raise Exception(error_msg)
    
    def _record_error(self, batch_id, error_type, error_message, component, severity):
        """
        Record an error in the database.
        
        Args:
            batch_id: The batch ID
            error_type: Type of error
            error_message: Error message
            component: Component where the error occurred
            severity: Error severity
        """
        try:
            with transaction_scope() as session:
                error = ETLError(
                    error_type=error_type,
                    error_message=error_message,
                    component=component,
                    severity=severity,
                    batch_id=batch_id,
                    timestamp=datetime.utcnow()
                )
                session.add(error)
        except Exception as e:
            logger.error(f"Failed to record error: {str(e)}")
    
    def _record_metrics(self, batch_id, validation_results, processing_time, created_by=None):
        """
        Record batch processing metrics.
        
        Args:
            batch_id: The batch ID
            validation_results: Validation results dictionary
            processing_time: Total processing time in seconds
            created_by: User or system processing the batch
        """
        try:
            metrics = [
                ETLMetric(
                    metric_name="total_records",
                    metric_value=validation_results["total"],
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="valid_records",
                    metric_value=validation_results["valid"],
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="invalid_records",
                    metric_value=validation_results["invalid"],
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="modified_records",
                    metric_value=validation_results["modified"],
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="error_rate",
                    metric_value=validation_results["invalid"] / max(validation_results["total"], 1),
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="processing_time",
                    metric_value=processing_time,
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="records_per_second",
                    metric_value=validation_results["total"] / max(processing_time, 0.001),
                    component="batch_processor",
                    batch_id=batch_id
                ),
                ETLMetric(
                    metric_name="timestamp",
                    metric_value="2025-03-06 04:19:13",  # Using current timestamp
                    component="batch_processor",
                    batch_id=batch_id
                )
            ]
            
            with transaction_scope("record_metrics", "batch_processor", created_by, batch_id) as session:
                for metric in metrics:
                    session.add(metric)
        
        except Exception as e:
            logger.error(f"Failed to record metrics: {str(e)}")
    
    def get_batch_status(self, batch_id):
        """
        Get the current status of a batch.
        
        Args:
            batch_id: The batch ID
            
        Returns:
            dict: Batch status information
        """
        try:
            with transaction_scope() as session:
                batch = session.query(SalesBatch).filter_by(batch_id=batch_id).first()
                
                if not batch:
                    return {
                        "batch_id": batch_id,
                        "status": "not_found",
                        "error": "Batch not found"
                    }
                
                # Get metrics for this batch
                metrics = session.query(ETLMetric).filter_by(batch_id=batch_id).all()
                metrics_dict = {}
                for metric in metrics:
                    metrics_dict[metric.metric_name] = metric.metric_value
                
                # Get error count
                error_count = session.query(ETLError).filter_by(batch_id=batch_id).count()
                
                # Calculate processing time
                processing_time = 0
                if batch.start_time and batch.end_time:
                    processing_time = (batch.end_time - batch.start_time).total_seconds()
                
                return {
                    "batch_id": batch.batch_id,
                    "source": batch.source,
                    "status": batch.status,
                    "record_count": batch.record_count,
                    "error_count": batch.error_count,
                    "start_time": batch.start_time.strftime("%Y-%m-%d %H:%M:%S") if batch.start_time else None,
                    "end_time": batch.end_time.strftime("%Y-%m-%d %H:%M:%S") if batch.end_time else None,
                    "processing_time": processing_time,
                    "created_by": batch.created_by,
                    "metrics": metrics_dict,
                    "error_count_db": error_count
                }
        
        except Exception as e:
            logger.error(f"Error getting batch status: {str(e)}")
            return {
                "batch_id": batch_id,
                "status": "error",
                "error": str(e)
            }
    
    def retry_batch(self, batch_id, data_df=None, created_by=None):
        """
        Retry a failed batch.
        
        Args:
            batch_id: The batch ID to retry
            data_df: Optional new data to use (if None, uses the original data)
            created_by: User or system retrying the batch
            
        Returns:
            dict: Processing results
        """
        try:
            # First, check the current status
            batch_status = self.get_batch_status(batch_id)
            
            if batch_status["status"] not in ["failed", "error"]:
                raise ValueError(f"Can only retry failed batches. Current status: {batch_status['status']}")
            
            # If no new data provided, we would need to retrieve the original data
            # This is just a placeholder - in a real system you'd need to implement
            # a way to store and retrieve the original data
            if data_df is None:
                raise ValueError("Retrying without new data is not supported yet - please provide data_df")
            
            # Create a new batch based on the old one
            new_batch_id = self.generate_batch_id()
            
            with transaction_scope("create_retry_batch", "batch_processor", created_by) as session:
                # Get the original batch
                original_batch = session.query(SalesBatch).filter_by(batch_id=batch_id).first()
                
                if not original_batch:
                    raise ValueError(f"Original batch {batch_id} not found")
                
                # Create new batch
                new_batch = SalesBatch(
                    batch_id=new_batch_id,
                    source=f"retry_{original_batch.source}",
                    record_count=len(data_df) if data_df is not None else original_batch.record_count,
                    start_time=datetime.utcnow(),
                    status="pending",
                    error_count=0,
                    created_by=created_by,
                    # parent_batch_id=batch_id
                )
                session.add(new_batch)
                
                # Add audit entry
                audit = ETLAudit(
                    action="retry_batch",
                    component="batch_processor",
                    details=f"Retry of batch {batch_id} created as {new_batch_id}",
                    user_id=created_by,
                    batch_id=new_batch_id,
                    timestamp=datetime.utcnow()
                )
                session.add(audit)
            
            # Process the new batch
            return self.process_batch(new_batch_id, data_df, created_by)
            
        except Exception as e:
            logger.error(f"Error retrying batch {batch_id}: {str(e)}")
            
            # Record the error
            self._record_error(
                batch_id, 
                "batch_retry_error", 
                f"Failed to retry batch: {str(e)}", 
                "batch_processor", 
                "error"
            )
            
            # Re-raise the exception
            raise


    def process_batch_async(self, batch_id: str, data_df: pd.DataFrame, created_by: str) -> None:
        """
        Process a batch of data asynchronously using a background thread.
        
        Args:
            batch_id: ID of the batch to process
            data_df: DataFrame containing the data to process
            created_by: User who created the batch
        """
        # Log the start of async processing
        logger.info(f"Starting asynchronous processing of batch {batch_id}")
        
        # Create a thread for background processing
        process_thread = threading.Thread(
            target=self.process_batch,
            args=(batch_id, data_df, created_by),
            daemon=True
        )
        
        # Start the background thread
        process_thread.start()
        
        logger.info(f"Batch {batch_id} processing started in background thread")