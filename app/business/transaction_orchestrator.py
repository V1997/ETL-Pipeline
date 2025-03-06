        
import logging
import uuid
from typing import Dict, Any, List, Optional, Callable, Union
from datetime import datetime
import json
from enum import Enum
import time
import traceback
import threading

from db.transaction import transaction_scope
from models.models import ETLAudit, ETLError

# Set up logging
logger = logging.getLogger(__name__)

class TransactionStatus(Enum):
    """Enum representing transaction status values."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PARTIALLY_COMPLETE = "partially_complete"


class TransactionStep:
    """
    Represents a single step in a transaction.
    
    Each step has a function to execute and optional
    compensation logic for rollbacks.
    """
    
    def __init__(
        self,
        name: str,
        execute_func: Callable[..., Any],
        compensate_func: Optional[Callable[..., Any]] = None,
        description: str = None
    ):
        """
        Initialize a transaction step.
        
        Args:
            name: Step name
            execute_func: Function to execute for this step
            compensate_func: Function to execute for rollback (optional)
            description: Step description
        """
        self.name = name
        self.execute_func = execute_func
        self.compensate_func = compensate_func
        self.description = description or f"Transaction step: {name}"
        self.result = None
        self.status = TransactionStatus.PENDING
        self.error = None
        self.start_time = None
        self.end_time = None
    
    def execute(self, *args, **kwargs) -> Any:
        """
        Execute the step function.
        
        Returns:
            Any: Result of the execute function
        """
        self.start_time = datetime.utcnow()
        self.status = TransactionStatus.IN_PROGRESS
        
        try:
            self.result = self.execute_func(*args, **kwargs)
            self.status = TransactionStatus.COMPLETED
            return self.result
        except Exception as e:
            self.error = str(e)
            self.status = TransactionStatus.FAILED
            raise
        finally:
            self.end_time = datetime.utcnow()
    
    def compensate(self, *args, **kwargs) -> Any:
        """
        Execute the compensation function.
        
        Returns:
            Any: Result of the compensation function
        """
        if not self.compensate_func:
            logger.warning(f"No compensation function defined for step '{self.name}'")
            return None
        
        try:
            result = self.compensate_func(*args, **kwargs)
            self.status = TransactionStatus.ROLLED_BACK
            return result
        except Exception as e:
            logger.error(f"Error during compensation for step '{self.name}': {str(e)}")
            # We don't re-raise here to allow the orchestrator to continue other compensations
            return None
    
    def get_execution_time(self) -> float:
        """
        Get the execution time in seconds.
        
        Returns:
            float: Execution time in seconds, or None if not executed
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert step information to a dictionary.
        
        Returns:
            Dict: Step information
        """
        return {
            'name': self.name,
            'description': self.description,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'execution_time': self.get_execution_time(),
            'error': self.error
        }


class TransactionOrchestrator:
    """
    Orchestrates complex transactions with multiple steps.
    
    This class handles:
    - Executing a sequence of transaction steps
    - Rollback on failure using compensation functions
    - Logging transaction activity
    - Handling errors and state management
    """
    
    def __init__(self, transaction_id: str = None):
        """
        Initialize a transaction orchestrator.
        
        Args:
            transaction_id: Optional transaction ID (generated if not provided)
        """
        self.transaction_id = transaction_id or f"TRANS-{str(uuid.uuid4())[:8]}-{int(time.time())}"
        self.steps: List[TransactionStep] = []
        self.current_step_index = -1
        self.status = TransactionStatus.PENDING
        self.start_time = None
        self.end_time = None
        self.context = {}
    
    def add_step(
        self,
        name: str,
        execute_func: Callable[..., Any],
        compensate_func: Optional[Callable[..., Any]] = None,
        description: str = None
    ) -> None:
        """
        Add a step to the transaction.
        
        Args:
            name: Step name
            execute_func: Function to execute for this step
            compensate_func: Function to execute for rollback (optional)
            description: Step description
        """
        step = TransactionStep(name, execute_func, compensate_func, description)
        self.steps.append(step)
        logger.debug(f"Added step '{name}' to transaction {self.transaction_id}")
    
    def execute(
        self,
        context: Dict[str, Any] = None,
        record_audit: bool = True,
        user_id: str = None,
        component: str = None
    ) -> Dict[str, Any]:
        """
        Execute all transaction steps in sequence.
        Args:
            context: Initial transaction context
            record_audit: Whether to record audit entries
            user_id: User ID for audit entries
            component: Component name for audit entries
            
        Returns:
            Dict: Transaction result including context and step details
        """
        self.context = context or {}
        self.start_time = datetime.utcnow()
        self.status = TransactionStatus.IN_PROGRESS
        
        if record_audit:
            self._record_audit("transaction_start", component, f"Started transaction {self.transaction_id}", user_id)
        
        try:
            # Execute each step
            for i, step in enumerate(self.steps):
                self.current_step_index = i
                
                # Log step start
                logger.info(f"Executing step {i+1}/{len(self.steps)}: '{step.name}'")
                if record_audit:
                    self._record_audit("step_start", component, f"Starting step '{step.name}'", user_id)
                
                # Execute the step
                step.execute(context=self.context)
                
                # Update context with step result if it returned a dict
                if isinstance(step.result, dict):
                    self.context.update(step.result)
                
                # Log step completion
                if record_audit:
                    self._record_audit("step_complete", component, f"Completed step '{step.name}'", user_id)
            
            # All steps completed successfully
            self.status = TransactionStatus.COMPLETED
            self.end_time = datetime.utcnow()
            
            if record_audit:
                self._record_audit("transaction_complete", component, f"Transaction {self.transaction_id} completed successfully", user_id)
                
            return self._create_result()
            
        except Exception as e:
            # Step failed, attempt rollback
            logger.error(f"Transaction {self.transaction_id} failed at step '{self.steps[self.current_step_index].name}': {str(e)}")
            
            if record_audit:
                error_details = f"Transaction {self.transaction_id} failed at step '{self.steps[self.current_step_index].name}': {str(e)}"
                self._record_audit("transaction_failed", component, error_details, user_id)
                self._record_error("transaction_failure", str(e), component, user_id)
            
            # Roll back the transaction
            self._rollback(record_audit, user_id, component)
            
            self.end_time = datetime.utcnow()
            
            # Re-raise the exception after rollback
            raise
    
    def _rollback(self, record_audit: bool, user_id: str, component: str) -> None:
        """
        Roll back the transaction by executing compensation functions.
        
        Args:
            record_audit: Whether to record audit entries
            user_id: User ID for audit entries
            component: Component name for audit entries
        """
        if record_audit:
            self._record_audit("rollback_start", component, f"Starting rollback for transaction {self.transaction_id}", user_id)
        
        # Roll back steps in reverse order, starting from the failed step
        for i in range(self.current_step_index, -1, -1):
            step = self.steps[i]
            
            if step.status == TransactionStatus.COMPLETED:
                logger.info(f"Rolling back step '{step.name}'")
                
                if record_audit:
                    self._record_audit("step_rollback", component, f"Rolling back step '{step.name}'", user_id)
                
                try:
                    step.compensate(context=self.context)
                except Exception as e:
                    logger.error(f"Error during rollback of step '{step.name}': {str(e)}")
                    if record_audit:
                        self._record_error("rollback_failure", str(e), component, user_id)
        
        self.status = TransactionStatus.ROLLED_BACK
        
        if record_audit:
            self._record_audit("rollback_complete", component, f"Completed rollback for transaction {self.transaction_id}", user_id)
    
    def _record_audit(self, action: str, component: str, details: str, user_id: str) -> None:
        """
        Record an audit entry for the transaction.
        
        Args:
            action: Action name
            component: Component name
            details: Audit details
            user_id: User ID
        """
        try:
            with transaction_scope() as session:
                audit = ETLAudit(
                    action=action,
                    component=component or "transaction_orchestrator",
                    details=details,
                    user_id=user_id,
                    transaction_id=self.transaction_id
                )
                session.add(audit)
        except Exception as e:
            logger.error(f"Failed to record audit entry: {str(e)}")
    
    def _record_error(self, error_type: str, error_message: str, component: str, user_id: str) -> None:
        """
        Record an error for the transaction.
        
        Args:
            error_type: Error type
            error_message: Error message
            component: Component name
            user_id: User ID
        """
        try:
            with transaction_scope() as session:
                error = ETLError(
                    error_type=error_type,
                    error_message=error_message,
                    component=component or "transaction_orchestrator",
                    severity="error",
                    transaction_id=self.transaction_id,
                    stack_trace=traceback.format_exc()
                )
                session.add(error)
        except Exception as e:
            logger.error(f"Failed to record error: {str(e)}")
    
    def get_execution_time(self) -> float:
        """
        Get the total execution time in seconds.
        
        Returns:
            float: Execution time in seconds, or None if not executed
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def _create_result(self) -> Dict[str, Any]:
        """
        Create a result dictionary with transaction information.
        
        Returns:
            Dict: Transaction result
        """
        steps_info = [step.to_dict() for step in self.steps]
        
        return {
            'transaction_id': self.transaction_id,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'execution_time': self.get_execution_time(),
            'steps': steps_info,
            'context': self.context
        }