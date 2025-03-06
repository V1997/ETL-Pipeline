from typing import List, Dict, Any, Optional, TypeVar, Generic, Type
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from pydantic import BaseModel

from ..models.models import Base, SalesBatch, SalesRecord, ETLError, ETLMetric, ETLAudit
from .transaction import transaction_scope

# Setup logging
logger = logging.getLogger(__name__)

# Generic type for SQLAlchemy models
T = TypeVar('T', bound=Base)
# Generic type for Pydantic models
P = TypeVar('P', bound=BaseModel)

class Repository(Generic[T, P]):
    """
    Generic repository for database CRUD operations.
    
    This class provides a standard interface for basic CRUD operations
    for any SQLAlchemy model paired with a corresponding Pydantic schema.
    """
    
    def __init__(self, model: Type[T]):
        """
        Initialize the repository with the SQLAlchemy model.
        
        Args:
            model: SQLAlchemy model class
        """
        self.model = model
    
    def create(self, session: Session, obj_in: Dict[str, Any]) -> T:
        """
        Create a new database record.
        
        Args:
            session: SQLAlchemy session
            obj_in: Dictionary containing the object data
            
        Returns:
            T: Created database object
        """
        db_obj = self.model(**obj_in)
        session.add(db_obj)
        session.flush()
        session.refresh(db_obj)
        return db_obj
    
    def get(self, session: Session, id: Any) -> Optional[T]:
        """
        Get an object by ID.
        
        Args:
            session: SQLAlchemy session
            id: Object ID
            
        Returns:
            Optional[T]: Retrieved object or None if not found
        """
        return session.query(self.model).filter(self.model.id == id).first()
    
    def get_by_field(self, session: Session, field_name: str, value: Any) -> Optional[T]:
        """
        Get an object by a specific field value.
        
        Args:
            session: SQLAlchemy session
            field_name: Name of the field to filter by
            value: Field value to match
            
        Returns:
            Optional[T]: Retrieved object or None if not found
        """
        return session.query(self.model).filter(getattr(self.model, field_name) == value).first()
    
    def get_multi(
        self, session: Session, *, skip: int = 0, limit: int = 100, order_by: str = None, filters: Dict = None
    ) -> List[T]:
        """
        Get multiple objects with pagination, ordering, and filtering.
        
        Args:
            session: SQLAlchemy session
            skip: Number of records to skip (offset)
            limit: Maximum number of records to return
            order_by: Field name to order by (prefix with - for descending)
            filters: Dictionary of {field_name: value} pairs to filter by
            
        Returns:
            List[T]: List of retrieved objects
        """
        query = session.query(self.model)

        # Apply filters if provided
        if filters:
            for field_name, value in filters.items():
                if hasattr(self.model, field_name):
                    query = query.filter(getattr(self.model, field_name) == value)
        
        # Apply ordering
        if order_by:
            desc = False
            if order_by.startswith('-'):
                desc = True
                order_by = order_by[1:]
            
            if hasattr(self.model, order_by):
                column = getattr(self.model, order_by)
                if desc:
                    query = query.order_by(column.desc())
                else:
                    query = query.order_by(column.asc())
        
        # Apply pagination
        query = query.offset(skip).limit(limit)
        
        return query.all()
    
    def update(self, session: Session, *, db_obj: T, obj_in: Dict[str, Any]) -> T:
        """
        Update an existing database object.
        
        Args:
            session: SQLAlchemy session
            db_obj: Database object to update
            obj_in: Dictionary containing update data
            
        Returns:
            T: Updated database object
        """
        # Update the object attributes
        for field in obj_in:
            if hasattr(db_obj, field):
                setattr(db_obj, field, obj_in[field])
        
        session.add(db_obj)
        session.flush()
        session.refresh(db_obj)
        return db_obj
    
    def delete(self, session: Session, *, id: Any) -> Optional[T]:
        """
        Delete an object by ID.
        
        Args:
            session: SQLAlchemy session
            id: Object ID
            
        Returns:
            Optional[T]: Deleted object or None if not found
        """
        obj = session.query(self.model).get(id)
        if obj:
            session.delete(obj)
            session.flush()
        return obj
    
    def count(self, session: Session, filters: Dict = None) -> int:
        """
        Count objects with optional filtering.
        
        Args:
            session: SQLAlchemy session
            filters: Dictionary of {field_name: value} pairs to filter by
            
        Returns:
            int: Count of matching objects
        """
        query = session.query(self.model)
        
        # Apply filters if provided
        if filters:
            for field_name, value in filters.items():
                if hasattr(self.model, field_name):
                    query = query.filter(getattr(self.model, field_name) == value)
        
        return query.count()


# Create specific repositories for our models
class SalesBatchRepository(Repository[SalesBatch, BaseModel]):
    """Repository for SalesBatch operations with additional batch-specific methods."""
    
    def __init__(self):
        super().__init__(SalesBatch)
    
    def get_by_batch_id(self, session: Session, batch_id: str) -> Optional[SalesBatch]:
        """Get a batch by its batch_id."""
        return session.query(self.model).filter(self.model.batch_id == batch_id).first()
    
    def get_active_batches(self, session: Session) -> List[SalesBatch]:
        """Get all batches that are currently processing."""
        return session.query(self.model).filter(self.model.status == "processing").all()
    
    def complete_batch(self, session: Session, batch_id: str, error_count: int = 0) -> Optional[SalesBatch]:
        """Mark a batch as completed."""
        batch = self.get_by_batch_id(session, batch_id)
        if batch:
            batch.status = "completed"
            batch.end_time = datetime.utcnow()
            batch.error_count = error_count
            session.add(batch)
            session.flush()
            session.refresh(batch)
        return batch
    
    def fail_batch(self, session: Session, batch_id: str, error_count: int = 0) -> Optional[SalesBatch]:
        """Mark a batch as failed."""
        batch = self.get_by_batch_id(session, batch_id)
        if batch:
            batch.status = "failed"
            batch.end_time = datetime.utcnow()
            batch.error_count = error_count
            session.add(batch)
            session.flush()
            session.refresh(batch)
        return batch
    
    def get_batch_statistics(self, session: Session, batch_id: str) -> Dict[str, Any]:
        """
        Get detailed statistics for a batch.
        
        Args:
            session: SQLAlchemy session
            batch_id: Batch ID
            
        Returns:
            Dict: Statistics about the batch
        """
        batch = self.get_by_batch_id(session, batch_id)
        if not batch:
            return None
        
        # Get record counts
        total_records = session.query(SalesRecord).filter(SalesRecord.batch_id == batch_id).count()
        valid_records = session.query(SalesRecord).filter(
            SalesRecord.batch_id == batch_id, 
            SalesRecord.is_valid == True
        ).count()
        invalid_records = total_records - valid_records
        
        # Get error counts
        error_count = session.query(ETLError).filter(ETLError.batch_id == batch_id).count()
        
        # Calculate processing time
        processing_time = None
        if batch.end_time:
            processing_time = (batch.end_time - batch.start_time).total_seconds()
        
        # Get processing metrics
        metrics = session.query(ETLMetric).filter(
            ETLMetric.batch_id == batch_id,
            ETLMetric.metric_name == "processing_time"
        ).first()
        
        metrics_value = metrics.metric_value if metrics else None
        
        return {
            "batch_id": batch.batch_id,
            "source": batch.source,
            "status": batch.status,
            "start_time": batch.start_time,
            "end_time": batch.end_time,
            "processing_time_seconds": processing_time,
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "error_count": error_count,
            "processing_metrics": metrics_value
        }


class SalesRecordRepository(Repository[SalesRecord, BaseModel]):
    """Repository for SalesRecord operations with additional record-specific methods."""
    
    def __init__(self):
        super().__init__(SalesRecord)
    
    def get_by_order_id(self, session: Session, order_id: str) -> Optional[SalesRecord]:
        """Get a record by its order_id."""
        return session.query(self.model).filter(self.model.order_id == order_id).first()
    
    def get_records_by_batch(
        self, session: Session, batch_id: str, skip: int = 0, limit: int = 100
    ) -> List[SalesRecord]:
        """Get records for a specific batch with pagination."""
        return session.query(self.model).filter(
            self.model.batch_id == batch_id
        ).offset(skip).limit(limit).all()
    
    def get_records_by_region(
        self, session: Session, region: str, skip: int = 0, limit: int = 100
    ) -> List[SalesRecord]:
        """Get records for a specific region with pagination."""
        return session.query(self.model).filter(
            self.model.region == region
        ).offset(skip).limit(limit).all()
    
    def get_sales_summary_by_region(self, session: Session) -> List[Dict[str, Any]]:
        """Get sales summary aggregated by region."""
        from sqlalchemy import func, desc
        
        query = session.query(
            self.model.region,
            func.count().label("order_count"),
            func.sum(self.model.units_sold).label("total_units"),
            func.sum(self.model.total_revenue).label("total_revenue"),
            func.sum(self.model.total_profit).label("total_profit")
        ).group_by(self.model.region).order_by(desc("total_profit"))
        
        result = []
        for row in query.all():
            result.append({
                "region": row.region,
                "order_count": row.order_count,
                "total_units": row.total_units,
                "total_revenue": float(row.total_revenue),
                "total_profit": float(row.total_profit)
            })
        
        return result


class ETLErrorRepository(Repository[ETLError, BaseModel]):
    """Repository for ETLError operations with additional error-specific methods."""
    
    def __init__(self):
        super().__init__(ETLError)
    
    def get_unresolved_errors(self, session: Session, limit: int = 100) -> List[ETLError]:
        """Get unresolved errors."""
        return session.query(self.model).filter(
            self.model.resolved == False
        ).order_by(self.model.timestamp.desc()).limit(limit).all()
    
    def resolve_error(self, session: Session, error_id: int, resolution_note: str) -> Optional[ETLError]:
        """Mark an error as resolved."""
        error = self.get(session, error_id)
        if error:
            error.resolved = True
            error.resolution_note = resolution_note
            session.add(error)
            session.flush()
            session.refresh(error)
        return error
    
    def resolve_errors_by_type(
        self, session: Session, error_type: str, resolution_note: str
    ) -> int:
        """
        Resolve all errors of a specific type.
        
        Args:
            session: SQLAlchemy session
            error_type: Type of error to resolve
            resolution_note: Note explaining the resolution
            
        Returns:
            int: Number of errors resolved
        """
        result = session.query(self.model).filter(
            self.model.error_type == error_type,
            self.model.resolved == False
        ).update({
            "resolved": True,
            "resolution_note": resolution_note
        }, synchronize_session=False)
        
        session.flush()
        return result


class ETLMetricRepository(Repository[ETLMetric, BaseModel]):
    """Repository for ETLMetric operations with additional metric-specific methods."""
    
    def __init__(self):
        super().__init__(ETLMetric)
    
    def get_metrics_by_batch(self, session: Session, batch_id: str) -> List[ETLMetric]:
        """Get all metrics for a specific batch."""
        return session.query(self.model).filter(self.model.batch_id == batch_id).all()
    
    def get_metrics_by_name(
        self, session: Session, metric_name: str, limit: int = 100
    ) -> List[ETLMetric]:
        """Get metrics with a specific name."""
        return session.query(self.model).filter(
            self.model.metric_name == metric_name
        ).order_by(self.model.timestamp.desc()).limit(limit).all()
    
    def get_metric_average(self, session: Session, metric_name: str) -> float:
        """Calculate the average value for a specific metric."""
        from sqlalchemy import func
        
        result = session.query(func.avg(self.model.metric_value)).filter(
            self.model.metric_name == metric_name
        ).scalar()
        
        return float(result) if result is not None else 0.0


class ETLAuditRepository(Repository[ETLAudit, BaseModel]):
    """Repository for ETLAudit operations with additional audit-specific methods."""
    
    def __init__(self):
        super().__init__(ETLAudit)
    
    def get_audit_by_batch(self, session: Session, batch_id: str) -> List[ETLAudit]:
        """Get all audit entries for a specific batch."""
        return session.query(self.model).filter(
            self.model.batch_id == batch_id
        ).order_by(self.model.timestamp.desc()).all()
    
    def get_recent_activity(self, session: Session, limit: int = 100) -> List[ETLAudit]:
        """Get recent audit activity."""
        return session.query(self.model).order_by(
            self.model.timestamp.desc()
        ).limit(limit).all()
    
    def log_action(
        self, session: Session, action: str, component: str, details: str = None,
        user_id: str = None, batch_id: str = None
    ) -> ETLAudit:
        """
        Log an action in the audit table.
        
        Args:
            session: SQLAlchemy session
            action: Action name
            component: Component name
            details: Additional details
            user_id: User who performed the action
            batch_id: Associated batch ID
            
        Returns:
            ETLAudit: Created audit entry
        """
        audit = ETLAudit(
            action=action,
            component=component,
            details=details,
            user_id=user_id,
            batch_id=batch_id
        )
        session.add(audit)
        session.flush()
        session.refresh(audit)
        return audit