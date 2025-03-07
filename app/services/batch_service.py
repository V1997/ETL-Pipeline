from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
import logging
import uuid
import time

from models.models import SalesBatch
from db.repository import SalesBatchRepository, SalesRecordRepository, ETLErrorRepository
from db.transaction import transaction_scope
from core.batch_processor import BatchProcessor

# Set up logging
logger = logging.getLogger(__name__)

class BatchService:
    """
    Service for batch-related operations.
    
    This service acts as a facade over the repositories and core processing logic,
    providing a higher-level API for the application.
    """
    
    def __init__(self, config=None):
        """Initialize the service with repositories."""
        self.batch_repo = SalesBatchRepository()
        self.record_repo = SalesRecordRepository()
        self.error_repo = ETLErrorRepository()
        self.batch_processor = BatchProcessor(config)
    
    def create_batch(self, source: str, created_by: str = None) -> Dict[str, Any]:
        """
        Create a new batch record.
        
        Args:
            source: Source of the data
            created_by: User or system creating the batch
            
        Returns:
            Dict: Batch information
        """
        batch_id = f"BATCH-{str(uuid.uuid4())[:8]}-{int(time.time())}"
        
        with transaction_scope("create_batch", "batch_service", created_by) as session:
            batch = self.batch_repo.create(session, {
                "batch_id": batch_id,
                "source": source,
                "record_count": 0,
                "start_time": datetime.utcnow(),
                "status": "pending",
                "created_by": created_by
            })
            
            return {
                "batch_id": batch.batch_id,
                "source": batch.source,
                "status": batch.status,
                "created_at": batch.created_at.isoformat()
            }

    def process_batch(self, batch_id: str, data_file_path: str, created_by: str = None) -> Dict[str, Any]:
        """
        Process a data file into a batch.
        
        Args:
            batch_id: Batch ID
            data_file_path: Path to the data file
            created_by: User or system processing the batch
            
        Returns:
            Dict: Processing results
        """
        try:
            # Check if batch exists
            with transaction_scope() as session:
                batch = self.batch_repo.get_by_batch_id(session, batch_id)
                if not batch:
                    raise ValueError(f"Batch with ID {batch_id} not found")
                
                # Check if batch is in a valid state
                if batch.status != "pending":
                    raise ValueError(f"Batch has invalid status: {batch.status}. Expected 'pending'.")
            
            # Load the data file
            logger.info(f"Loading data from {data_file_path}")
            if data_file_path.endswith('.csv'):
                df = pd.read_csv(data_file_path, parse_dates=['order_date', 'ship_date'])
            elif data_file_path.endswith('.xlsx'):
                df = pd.read_excel(data_file_path, parse_dates=['order_date', 'ship_date'])
            elif data_file_path.endswith('.json'):
                df = pd.read_json(data_file_path)
            else:
                raise ValueError(f"Unsupported file format: {data_file_path}")
            
            # Update batch record count
            with transaction_scope() as session:
                batch = self.batch_repo.get_by_batch_id(session, batch_id)
                batch.record_count = len(df)
                session.add(batch)
            
            # Process the batch with the processor
            logger.info(f"Processing {len(df)} records for batch {batch_id}")
            result = self.batch_processor.process_batch(batch_id, df, created_by)
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            
            # Mark batch as failed
            try:
                with transaction_scope() as session:
                    self.batch_repo.fail_batch(session, batch_id)
            except Exception as inner_e:
                logger.error(f"Failed to update batch status: {str(inner_e)}")
            
            # Re-raise the exception
            raise
    
    def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """
        Get the current status of a batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dict: Batch status information
        """
        with transaction_scope() as session:
            batch = self.batch_repo.get_by_batch_id(session, batch_id)
            if not batch:
                return None
            
            return {
                "batch_id": batch.batch_id,
                "source": batch.source,
                "record_count": batch.record_count,
                "start_time": batch.start_time.isoformat() if batch.start_time else None,
                "end_time": batch.end_time.isoformat() if batch.end_time else None,
                "status": batch.status,
                "error_count": batch.error_count,
                "created_by": batch.created_by,
                "created_at": batch.created_at.isoformat()
            }
    
    def get_batch_statistics(self, batch_id: str) -> Dict[str, Any]:
        """
        Get detailed statistics for a batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dict: Batch statistics
        """
        with transaction_scope() as session:
            stats = self.batch_repo.get_batch_statistics(session, batch_id)
            return stats
    
    def list_batches(
        self, skip: int = 0, limit: int = 100, status: str = None, order_by: str = "-created_at"
    ) -> List[Dict[str, Any]]:
        """
        List batches with filtering and pagination.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            status: Optional status filter
            order_by: Field to order by (prefix with - for descending)
            
        Returns:
            List[Dict]: List of batch information
        """
        with transaction_scope() as session:
            filters = {}
            if status:
                filters["status"] = status
                
            batches = self.batch_repo.get_multi(
                session, skip=skip, limit=limit, order_by=order_by, filters=filters
            )
            
            result = []
            for batch in batches:
                result.append({
                    "batch_id": batch.batch_id,
                    "source": batch.source,
                    "record_count": batch.record_count,
                    "status": batch.status,
                    "error_count": batch.error_count,
                    "start_time": batch.start_time.isoformat() if batch.start_time else None,
                    "end_time": batch.end_time.isoformat() if batch.end_time else None,
                    "created_at": batch.created_at.isoformat()
                })
            
            return result
    
    def cancel_batch(self, batch_id: str, user_id: str = None) -> Dict[str, Any]:
        """
        Cancel a pending or processing batch.
        
        Args:
            batch_id: Batch ID
            user_id: User performing the cancellation
            
        Returns:
            Dict: Updated batch information
        """
        with transaction_scope("cancel_batch", "batch_service", user_id, batch_id) as session:
            batch = self.batch_repo.get_by_batch_id(session, batch_id)
            if not batch:
                raise ValueError(f"Batch with ID {batch_id} not found")
            
            if batch.status not in ["pending", "processing"]:
                raise ValueError(f"Cannot cancel batch with status '{batch.status}'")
            
            batch.status = "cancelled"
            batch.end_time = datetime.utcnow()
            session.add(batch)
            
            return {
                "batch_id": batch.batch_id,
                "source": batch.source,
                "status": batch.status,
                "cancelled_by": user_id,
                "cancelled_at": batch.end_time.isoformat()
            }