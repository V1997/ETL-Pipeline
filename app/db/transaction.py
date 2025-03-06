from contextlib import contextmanager
from sqlalchemy.exc import SQLAlchemyError
import logging
from .session import get_db_session
from models.models import ETLAudit, ETLError

# Set up logging
logger = logging.getLogger(__name__)

@contextmanager
def transaction_scope(audit_action=None, component=None, user_id=None, batch_id=None):
    """
    Provides transaction management with built-in auditing and error handling.
    
    Args:
        audit_action: Action name to record in the audit table
        component: Component name performing the operation
        user_id: User ID performing the operation
        batch_id: Batch ID associated with the operation
        
    Yields:
        SQLAlchemy session object
        
    Example:
        with transaction_scope("import_data", "batch_processor", "system", batch_id) as session:
            session.add(new_record)
    """
    with get_db_session() as session:
        try:
            # Start transaction
            if audit_action and component:
                # Log the start of the transaction in the audit table
                audit_entry = ETLAudit(
                    action=f"{audit_action}_started",
                    component=component,
                    details=f"Starting {audit_action} operation",
                    user_id=user_id,
                    batch_id=batch_id
                )
                session.add(audit_entry)
                session.flush()
            
            # Yield session to the caller
            yield session
            
            # If we get here, the transaction was successful
            if audit_action and component:
                # Log successful completion
                audit_entry = ETLAudit(
                    action=f"{audit_action}_completed",
                    component=component,
                    details=f"Successfully completed {audit_action} operation",
                    user_id=user_id,
                    batch_id=batch_id
                )
                session.add(audit_entry)
            
            # Commit the transaction
            session.commit()
            
        except Exception as e:
            # Transaction failed, roll it back
            session.rollback()
            
            # Log the error
            logger.error(f"Transaction failed: {str(e)}")
            
            if audit_action and component:
                try:
                    # Create a new session to log the error
                    with get_db_session() as error_session:
                        # Record the error in ETLError table
                        error_entry = ETLError(
                            error_type="transaction_error",
                            error_message=str(e),
                            component=component,
                            severity="error",
                            batch_id=batch_id
                        )
                        error_session.add(error_entry)
                        
                        # Record in audit table
                        audit_entry = ETLAudit(
                            action=f"{audit_action}_failed",
                            component=component,
                            details=f"Failed during {audit_action} operation: {str(e)}",
                            user_id=user_id,
                            batch_id=batch_id
                        )
                        error_session.add(audit_entry)
                        error_session.commit()
                except SQLAlchemyError as se:
                    # If even this fails, just log it
                    logger.error(f"Failed to record error: {str(se)}")
            
            # Re-raise the original exception
            raise