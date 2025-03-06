from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta

from ..db.repository import ETLErrorRepository, ETLMetricRepository, ETLAuditRepository
from ..db.transaction import transaction_scope

# Set up logging
logger = logging.getLogger(__name__)

class MonitoringService:
    """
    Service for monitoring and metrics operations.
    
    This service handles error management, metrics collection, and audit logging.
    """
    
    def __init__(self):
        """Initialize the service with repositories."""
        self.error_repo = ETLErrorRepository()
        self.metric_repo = ETLMetricRepository()
        self.audit_repo = ETLAuditRepository()
    
    def get_recent_errors(self, limit: int = 100, include_resolved: bool = False) -> List[Dict[str, Any]]:
        """
        Get recent errors.
        
        Args:
            limit: Maximum number of errors to return
            include_resolved: Whether to include resolved errors
            
        Returns:
            List[Dict]: Recent errors
        """
        with transaction_scope() as session:
            filters = {}
            if not include_resolved:
                filters["resolved"] = False
                
            errors = self.error_repo.get_multi(session, limit=limit, order_by="-timestamp", filters=filters)
            
            result = []
            for error in errors:
                result.append({
                    "id": error.id,
                    "error_type": error.error_type,
                    "error_message": error.error_message,
                    "component": error.component,
                    "severity": error.severity,
                    "record_id": error.record_id,
                    "batch_id": error.batch_id,
                    "timestamp": error.timestamp.isoformat(),
                    "resolved": error.resolved,
                    "resolution_note": error.resolution_note
                })
            
            return result
    
    def resolve_error(self, error_id: int, resolution_note: str, user_id: str = None) -> Dict[str, Any]:
        """
        Resolve an error.
        
        Args:
            error_id: Error ID
            resolution_note: Resolution note
            user_id: User resolving the error
            
        Returns:
            Dict: Updated error information
        """
        with transaction_scope("resolve_error", "monitoring_service", user_id) as session:
            error = self.error_repo.resolve_error(session, error_id, resolution_note)
            if not error:
                raise ValueError(f"Error with ID {error_id} not found")
            
            # Add audit entry
            self.audit_repo.log_action(
                session,
                action="resolve_error",
                component="monitoring_service",
                details=f"Resolved error {error_id}: {error.error_type}",
                user_id=user_id,
                batch_id=error.batch_id
            )
            
            return {
                "id": error.id,
                "error_type": error.error_type,
                "resolved": error.resolved,
                "resolution_note": error.resolution_note,
                "resolved_at": datetime.utcnow().isoformat(),
                "resolved_by": user_id
            }
    
    def resolve_errors_by_type(
        self, error_type: str, resolution_note: str, user_id: str = None
    ) -> Dict[str, Any]:
        """
        Resolve all errors of a specific type.
        
        Args:
            error_type: Error type
            resolution_note: Resolution note
            user_id: User resolving the errors
            
        Returns:
            Dict: Resolution results
        """
        with transaction_scope("bulk_resolve_errors", "monitoring_service", user_id) as session:
            resolved_count = self.error_repo.resolve_errors_by_type(session, error_type, resolution_note)
            
            # Add audit entry
            self.audit_repo.log_action(
                session,
                action="bulk_resolve_errors",
                component="monitoring_service",
                details=f"Bulk resolved {resolved_count} errors of type: {error_type}",
                user_id=user_id
            )
            
            return {
                "error_type": error_type,
                "resolved_count": resolved_count,
                "resolved_at": datetime.utcnow().isoformat(),
                "resolved_by": user_id
            }
    
    def get_system_metrics(self, time_range: str = "day") -> Dict[str, Any]:
        """
        Get system metrics for a time range.
        
        Args:
            time_range: Time range ('hour', 'day', 'week', 'month')
            
        Returns:
            Dict: System metrics
        """
        with transaction_scope() as session:
            # Define the time cutoff based on the range
            now = datetime.utcnow()
            if time_range == "hour":
                cutoff = now - timedelta(hours=1)
            elif time_range == "day":
                cutoff = now - timedelta(days=1)
            elif time_range == "week":
                cutoff = now - timedelta(weeks=1)
            elif time_range == "month":
                cutoff = now - timedelta(days=30)
            else:
                raise ValueError(f"Invalid time range: {time_range}")
            
            # Get metrics within the time range
            from sqlalchemy import func
            
            metrics_query = session.query(
                self.metric_repo.model.metric_name,
                func.avg(self.metric_repo.model.metric_value).label("avg_value"),
                func.min(self.metric_repo.model.metric_value).label("min_value"),
                func.max(self.metric_repo.model.metric_value).label("max_value"),
                func.count().label("count")
            ).filter(
                self.metric_repo.model.timestamp >= cutoff
            ).group_by(
                self.metric_repo.model.metric_name
            )
            
            metrics_result = {}
            for row in metrics_query.all():
                metrics_result[row.metric_name] = {
                    "avg": float(row.avg_value),
                    "min": float(row.min_value),
                    "max": float(row.max_value),
                    "count": row.count
                }
            
            # Get error counts
            error_count = session.query(self.error_repo.model).filter(
                self.error_repo.model.timestamp >= cutoff
            ).count()
            
            unresolved_error_count = session.query(self.error_repo.model).filter(
                self.error_repo.model.timestamp >= cutoff,
                self.error_repo.model.resolved == False
            ).count()
            
            # Get batch statistics
            from ..models.models import SalesBatch
            
            batch_query = session.query(
                func.count().label("total_batches"),
                func.sum(SalesBatch.record_count).label("total_records"),
                func.avg(func.timestampdiff(
                    "SECOND", SalesBatch.start_time, SalesBatch.end_time
                )).label("avg_processing_time")
            ).filter(
                SalesBatch.created_at >= cutoff,
                SalesBatch.status == "completed"
            )
            
            batch_row = batch_query.first()
            batch_stats = {
                "total_batches": batch_row.total_batches or 0,
                "total_records": int(batch_row.total_records) if batch_row.total_records else 0,
                "avg_processing_time": float(batch_row.avg_processing_time) if batch_row.avg_processing_time else 0
            }
            
            return {
                "time_range": time_range,
                "from_time": cutoff.isoformat(),
                "to_time": now.isoformat(),
                "metrics": metrics_result,
                "errors": {
                    "total": error_count,
                    "unresolved": unresolved_error_count
                },
                "batch_stats": batch_stats
            }
    
    def get_recent_activity(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent system activity from audit log.
        
        Args:
            limit: Maximum number of audit entries to return
            
        Returns:
            List[Dict]: Recent activity
        """
        with transaction_scope() as session:
            audits = self.audit_repo.get_recent_activity(session, limit)
            
            result = []
            for audit in audits:
                result.append({
                    "id": audit.id,
                    "action": audit.action,
                    "component": audit.component,
                    "details": audit.details,
                    "user_id": audit.user_id,
                    "timestamp": audit.timestamp.isoformat(),
                    "batch_id": audit.batch_id
                })
            
            return result
    
    def log_metric(
        self, metric_name: str, metric_value: float, component: str, batch_id: str = None
    ) -> Dict[str, Any]:
        """
        Log a new metric.
        
        Args:
            metric_name: Name of the metric
            metric_value: Metric value
            component: Component name
            batch_id: Associated batch ID
            
        Returns:
            Dict: Created metric information
        """
        with transaction_scope() as session:
            metric = self.metric_repo.create(session, {
                "metric_name": metric_name,
                "metric_value": metric_value,
                "component": component,
                "batch_id": batch_id,
                "timestamp": datetime.utcnow()
            })
            
            return {
                "id": metric.id,
                "metric_name": metric.metric_name,
                "metric_value": float(metric.metric_value),
                "component": metric.component,
                "timestamp": metric.timestamp.isoformat(),
                "batch_id": metric.batch_id
            }
    
    def log_audit(
        self, action: str, component: str, details: str = None,
        user_id: str = None, batch_id: str = None
    ) -> Dict[str, Any]:
        """
        Log an audit entry.
        
        Args:
            action: Action name
            component: Component name
            details: Additional details
            user_id: User who performed the action
            batch_id: Associated batch ID
            
        Returns:
            Dict: Created audit entry information
        """
        with transaction_scope() as session:
            audit = self.audit_repo.log_action(
                session, action, component, details, user_id, batch_id
            )
            
            return {
                "id": audit.id,
                "action": audit.action,
                "component": audit.component,
                "details": audit.details,
                "user_id": audit.user_id,
                "timestamp": audit.timestamp.isoformat(),
                "batch_id": audit.batch_id
            }