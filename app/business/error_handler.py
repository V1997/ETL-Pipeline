import logging
import traceback
import sys
from typing import Dict, Any, List, Optional, Union
from enum import Enum
import json
import re
import pandas as pd
from datetime import datetime

from db.transaction import transaction_scope
from models.models import ETLError, ETLAudit
from integrations.email_service import EmailService

# Set up logging
logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    """Enum representing error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Enum representing error categories."""
    DATA_VALIDATION = "data_validation"
    BUSINESS_RULE = "business_rule"
    DATABASE = "database"
    NETWORK = "network"
    SYSTEM = "system"
    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    INTEGRATION = "integration"
    FILE_PROCESSING = "file_processing"
    UNKNOWN = "unknown"


class ErrorHandler:
    """
    Central error handling and categorization facility.
    
    This class handles:
    - Error categorization and classification
    - Error recording and persistence
    - Error notification and reporting
    - Error analysis and grouping
    """
    
    def __init__(
        self,
        email_service: EmailService = None,
        send_notifications: bool = True,
        admin_email: str = None
    ):
        """
        Initialize the error handler.
        
        Args:
            email_service: Optional EmailService for notifications
            send_notifications: Whether to send email notifications
            admin_email: Email address for admin notifications
        """
        self.email_service = email_service
        self.send_notifications = send_notifications
        self.admin_email = admin_email
        
        # Error pattern matchers for categorization
        self.error_patterns = {
            ErrorCategory.DATA_VALIDATION: [
                r'validation failed',
                r'invalid data',
                r'does not match pattern',
                r'required field',
                r'constraint violation'
            ],
            ErrorCategory.BUSINESS_RULE: [
                r'business rule',
                r'rule violation',
                r'policy violation'
            ],
            ErrorCategory.DATABASE: [
                r'database error',
                r'sql error',
                r'deadlock',
                r'foreign key',
                r'unique constraint',
                r'timeout',
                r'connection refused',
                r'database is locked'
            ],
            ErrorCategory.NETWORK: [
                r'network error',
                r'connection refused',
                r'host not found',
                r'timeout',
                r'socket error'
            ],
            ErrorCategory.SYSTEM: [
                r'system error',
                r'out of memory',
                r'file not found',
                r'permission denied',
                r'i/o error'
            ],
            ErrorCategory.CONFIGURATION: [
                r'configuration error',
                r'missing config',
                r'invalid config',
                r'environment variable'
            ],
            ErrorCategory.AUTHENTICATION: [
                r'authentication failed',
                r'invalid credentials',
                r'password incorrect',
                r'not authenticated'
            ],
            ErrorCategory.AUTHORIZATION: [
                r'authorization failed',
                r'permission denied',
                r'access denied',
                r'forbidden',
                r'not authorized'
            ],
            ErrorCategory.INTEGRATION: [
                r'integration error',
                r'api error',
                r'service unavailable',
                r'gateway error'
            ],
            ErrorCategory.FILE_PROCESSING: [
                r'file error',
                r'parse error',
                r'invalid format',
                r'encoding error',
                r'file not found'
            ]
        }
    
    def categorize_error(self, error_message: str) -> ErrorCategory:
        """
        Categorize an error based on its message.
        
        Args:
            error_message: Error message to categorize
            
        Returns:
            ErrorCategory: Categorization of the error
        """
        error_message = error_message.lower()
        
        for category, patterns in self.error_patterns.items():
            for pattern in patterns:
                if re.search(pattern, error_message, re.IGNORECASE):
                    return category
        
        return ErrorCategory.UNKNOWN
    
    def determine_severity(
        self, 
        error_category: ErrorCategory, 
        exception: Exception = None
    ) -> ErrorSeverity:
        """
        Determine error severity based on category and exception.
        
        Args:
            error_category: Error category
            exception: Exception object (optional)
            
        Returns:
            ErrorSeverity: Severity level
        """
        # Default severities by category
        category_severities = {
            ErrorCategory.DATA_VALIDATION: ErrorSeverity.WARNING,
            ErrorCategory.BUSINESS_RULE: ErrorSeverity.WARNING,
            ErrorCategory.DATABASE: ErrorSeverity.ERROR,
            ErrorCategory.NETWORK: ErrorSeverity.WARNING,
            ErrorCategory.SYSTEM: ErrorSeverity.ERROR,
            ErrorCategory.CONFIGURATION: ErrorSeverity.ERROR,
            ErrorCategory.AUTHENTICATION: ErrorSeverity.WARNING,
            ErrorCategory.AUTHORIZATION: ErrorSeverity.WARNING,
            ErrorCategory.INTEGRATION: ErrorSeverity.ERROR,
            ErrorCategory.FILE_PROCESSING: ErrorSeverity.WARNING,
            ErrorCategory.UNKNOWN: ErrorSeverity.ERROR
        }
        
        # Critical exceptions override category defaults
        critical_exception_types = [
            'SystemError',
            'MemoryError',
            'RuntimeError',
            'SystemExit',
            'KeyboardInterrupt'
        ]
        
        if exception:
            exception_type = exception.__class__.__name__
            if exception_type in critical_exception_types:
                return ErrorSeverity.CRITICAL
        
        return category_severities.get(error_category, ErrorSeverity.ERROR)
    
    def handle_error(
        self,
        error_message: str,
        error_type: str = None,
        component: str = None,
        batch_id: str = None,
        record_id: str = None,
        exception: Exception = None,
        user_id: str = None,
        transaction_id: str = None,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Handle an error by categorizing, recording, and notifying.
        
        Args:
            error_message: Error message
            error_type: Error type
            component: Component where the error occurred
            batch_id: Associated batch ID
            record_id: Associated record ID
            exception: Exception object
            user_id: User ID
            transaction_id: Transaction ID
            context: Additional context data
            
        Returns:
            Dict: Error handling result with error_id and other info
        """
        try:
            # Get stack trace if exception is provided
            stack_trace = None
            if exception:
                stack_trace = ''.join(traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                ))
            
            # Determine error type if not provided
            if not error_type:
                error_type = exception.__class__.__name__ if exception else "UnknownError"
            
            # Categorize the error
            category = self.categorize_error(error_message)
            
            # Determine severity
            severity = self.determine_severity(category, exception)
            
            # Record the error
            error_id = self._record_error(
                error_type, error_message, component, 
                severity.value, category.value,
                batch_id, record_id, user_id, transaction_id,
                stack_trace, context
            )
            
            # Log the error
            log_method = logger.warning if severity == ErrorSeverity.WARNING else logger.error
            log_method(f"[{category.value}] {error_type}: {error_message}")
            
            # Send notification for errors and critical issues
            if self.send_notifications and self.email_service and severity in [ErrorSeverity.ERROR, ErrorSeverity.CRITICAL]:
                self._send_notification(
                    error_id, error_type, error_message, component,
                    severity.value, category.value,
                    batch_id, record_id, transaction_id,
                    stack_trace
                )
            
            return {
                'error_id': error_id,
                'error_type': error_type,
                'message': error_message,
                'category': category.value,
                'severity': severity.value,
                'component': component,
                'batch_id': batch_id,
                'record_id': record_id,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            # Fallback if error handling itself fails
            logger.error(f"Error in error handler: {str(e)}")
            return {
                'error_id': None,
                'error_type': error_type or "UnknownError",
                'message': error_message,
                'category': "unknown",
                'severity': "error",
                'component': component,
                'batch_id': batch_id,
                'record_id': record_id,
                'timestamp': datetime.utcnow().isoformat(),
                'error_handler_error': str(e)
            }
    
    def _record_error(
        self,
        error_type: str,
        error_message: str,
        component: str,
        severity: str,
        category: str,
        batch_id: str = None,
        record_id: str = None,
        user_id: str = None,
        transaction_id: str = None,
        stack_trace: str = None,
        context: Dict[str, Any] = None
    ) -> int:
        """
        Record an error in the database.
        
        Args:
            error_type: Error type
            error_message: Error message
            component: Component name
            severity: Error severity
            category: Error category
            batch_id: Associated batch ID
            record_id: Associated record ID
            user_id: User ID
            transaction_id: Transaction ID
            stack_trace: Error stack trace
            context: Additional context data
            
        Returns:
            int: Error ID
        """
        try:
            with transaction_scope() as session:
                error = ETLError(
                    error_type=error_type,
                    error_message=error_message,
                    component=component,
                    severity=severity,
                    category=category,
                    batch_id=batch_id,
                    record_id=record_id,
                    user_id=user_id,
                    transaction_id=transaction_id,
                    stack_trace=stack_trace,
                    context=json.dumps(context) if context else None,
                    timestamp=datetime.utcnow(),
                    resolved=False
                )
                session.add(error)
                session.flush()  # Flush to get the ID
                error_id = error.id
                
                # Add audit entry for error
                audit = ETLAudit(
                    action="error_recorded",
                    component=component or "error_handler",
                    details=f"Error recorded: {error_type} - {error_message[:100]}",
                    user_id=user_id,
                    batch_id=batch_id,
                    transaction_id=transaction_id
                )
                session.add(audit)
                
                return error_id
                
        except Exception as e:
            logger.error(f"Failed to record error: {str(e)}")
            return None
    
    def _send_notification(
        self,
        error_id: int,
        error_type: str,
        error_message: str,
        component: str,
        severity: str,
        category: str,
        batch_id: str = None,
        record_id: str = None,
        transaction_id: str = None,
        stack_trace: str = None
    ) -> bool:
        """
        Send an error notification email.
        
        Args:
            error_id: Error ID
            error_type: Error type
            error_message: Error message
            component: Component name
            severity: Error severity
            category: Error category
            batch_id: Associated batch ID
            record_id: Associated record ID
            transaction_id: Transaction ID
            stack_trace: Error stack trace
            
        Returns:
            bool: True if notification was sent successfully
        """
        try:
            if not self.email_service:
                logger.warning("Email service not available for error notification")
                return False
                
            recipients = self.admin_email or "admin@example.com"
            
            # Construct error details
            details = {
                'error_id': error_id,
                'component': component,
                'category': category,
                'batch_id': batch_id,
                'record_id': record_id,
                'transaction_id': transaction_id,
                'timestamp': datetime.utcnow().isoformat(),
                'stack_trace': stack_trace[-1000:] if stack_trace else None  # Include only last part if too long
            }
            
            # Send notification
            return self.email_service.send_error_notification(
                error_type=error_type,
                error_message=error_message,
                component=component,
                batch_id=batch_id,
                record_id=record_id,
                details=details,
                recipients=recipients
            )
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {str(e)}")
            return False
    
    def analyze_errors(
        self,
        time_range_hours: int = 24,
        component: str = None,
        batch_id: str = None,
        severity: str = None,
        category: str = None
    ) -> Dict[str, Any]:
        """
        Analyze errors within a time range.
        
        Args:
            time_range_hours: Time range in hours
            component: Filter by component
            batch_id: Filter by batch ID
            severity: Filter by severity
            category: Filter by category
            
        Returns:
            Dict: Error analysis results
        """
        try:
            from sqlalchemy import func
            
            with transaction_scope() as session:
                # Base query
                query = session.query(ETLError)
                
                # Apply filters
                if time_range_hours:
                    cutoff = datetime.utcnow() - pd.Timedelta(hours=time_range_hours)
                    query = query.filter(ETLError.timestamp >= cutoff)
                
                if component:
                    query = query.filter(ETLError.component == component)
                
                if batch_id:
                    query = query.filter(ETLError.batch_id == batch_id)
                
                if severity:
                    query = query.filter(ETLError.severity == severity)
                
                if category:
                    query = query.filter(ETLError.category == category)
                
                # Get total count
                total_count = query.count()
                
                # Get severity breakdown
                severity_query = session.query(
                    ETLError.severity,
                    func.count(ETLError.id).label('count')
                ).group_by(ETLError.severity)
                
                if time_range_hours:
                    severity_query = severity_query.filter(ETLError.timestamp >= cutoff)
                
                severity_breakdown = {row.severity: row.count for row in severity_query.all()}
                
                # Get category breakdown
                category_query = session.query(
                    ETLError.category,
                    func.count(ETLError.id).label('count')
                ).group_by(ETLError.category)
                
                if time_range_hours:
                    category_query = category_query.filter(ETLError.timestamp >= cutoff)
                
                category_breakdown = {row.category: row.count for row in category_query.all()}
                
                # Get component breakdown
                component_query = session.query(
                    ETLError.component,
                    func.count(ETLError.id).label('count')
                ).group_by(ETLError.component)
                
                if time_range_hours:
                    component_query = component_query.filter(ETLError.timestamp >= cutoff)
                
                component_breakdown = {row.component: row.count for row in component_query.all()}
                
                # Get most common errors
                common_errors_query = session.query(
                    ETLError.error_type,
                    func.count(ETLError.id).label('count')
                ).group_by(ETLError.error_type).order_by(func.count(ETLError.id).desc()).limit(10)
                
                if time_range_hours:
                    common_errors_query = common_errors_query.filter(ETLError.timestamp >= cutoff)
                
                common_errors = [{'error_type': row.error_type, 'count': row.count} for row in common_errors_query.all()]
                
                return {
                    'total_count': total_count,
                    'time_range_hours': time_range_hours,
                    'severity_breakdown': severity_breakdown,
                    'category_breakdown': category_breakdown,
                    'component_breakdown': component_breakdown,
                    'common_errors': common_errors,
                    'timestamp': datetime.utcnow().isoformat(),
                }
                
        except Exception as e:
            logger.error(f"Error analyzing errors: {str(e)}")
            return {
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }