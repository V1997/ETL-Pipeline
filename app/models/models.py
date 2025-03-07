"""
Database models for the ETL Pipeline API
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text, Enum as SQLAlchemyEnum
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.mysql import LONGTEXT
from datetime import datetime
import uuid
import enum
import json
from typing import Dict, Any, Optional

from app.db.session import Base


class SalesBatch(Base):
    """Sales batch database model"""
    __tablename__ = "sales_batches"
    
    batch_id = Column(String(36), primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    source = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False, index=True, default="pending")
    record_count = Column(Integer, nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)
    created_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    
    # Relationships
    records = relationship("SalesRecord", back_populates="batch", cascade="all, delete-orphan")
    errors = relationship("ETLError", back_populates="batch", cascade="all, delete-orphan")
    metrics = relationship("ETLMetric", back_populates="batch", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<SalesBatch {self.batch_id}, status={self.status}>"

class SalesRecord(Base):
    """Sales record database model"""
    __tablename__ = "sales_records"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(36), nullable=False, index=True)
    region = Column(String(50), nullable=False, index=True)
    country = Column(String(50), nullable=False, index=True)
    item_type = Column(String(50), nullable=False, index=True)
    sales_channel = Column(String(20), nullable=False, index=True)
    order_priority = Column(String(1), nullable=False)
    order_date = Column(DateTime, nullable=False, index=True)
    ship_date = Column(DateTime, nullable=False)
    units_sold = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    unit_cost = Column(Float, nullable=False)
    total_revenue = Column(Float, nullable=False)
    total_cost = Column(Float, nullable=False)
    total_profit = Column(Float, nullable=False)
    batch_id = Column(String(36), ForeignKey("sales_batches.batch_id"), nullable=False)
    
    # Relationships
    batch = relationship("SalesBatch", back_populates="records")
    
    def __repr__(self):
        return f"<SalesRecord {self.order_id}, region={self.region}>"


class ErrorSeverity(enum.Enum):
    """Enum for error severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class ETLMetric(Base):
    """ETL metric database model"""
    __tablename__ = "etl_metrics"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(36), ForeignKey("sales_batches.batch_id"), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Float, nullable=False)
    component = Column(String(100), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Relationships
    batch = relationship("SalesBatch", back_populates="metrics")
    
    def __repr__(self):
        return f"<ETLMetric {self.metric_name}={self.metric_value}>"


class User(Base):
    """User database model"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    hashed_password = Column(String(100), nullable=False)
    disabled = Column(Boolean, default=False)
    roles = Column(Text, nullable=False)  # Stored as comma-separated values
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_login = Column(DateTime)
    
    def __repr__(self):
        return f"<User {self.username}>"

class ETLError(Base):
    """ETL error database model"""
    __tablename__ = "etl_errors"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(36), ForeignKey("sales_batches.batch_id"), nullable=False)
    error_type = Column(String(50), nullable=False)
    error_message = Column(String(500), nullable=False)
    component = Column(String(100), nullable=False)
    severity = Column(SQLAlchemyEnum(ErrorSeverity), nullable=False, default=ErrorSeverity.ERROR)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    detail = Column(Text)
    
    # Relationships
    batch = relationship("SalesBatch", back_populates="errors")
    
    def __repr__(self):
        return f"<ETLError {self.error_type}, severity={self.severity}>"


class ETLAudit(Base):
    """ETL audit log database model for tracking system activities"""
    __tablename__ = "etl_audit"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String(50), nullable=False, index=True)  # Using String instead of Enum for flexibility
    user_id = Column(String(50), nullable=False, index=True)
    component = Column(String(100), nullable=True)
    batch_id = Column(String(36), nullable=True, index=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    details = Column(Text, nullable=True)
    # status = Column(String(20), nullable=False, default="success")
    
    def __init__(self, **kwargs):
        """
        Initialize an ETLAudit instance.
        
        Handles conversion of dictionaries to JSON strings for the details field
        """
        # Convert dictionary details to JSON string
        if 'details' in kwargs and isinstance(kwargs['details'], dict):
            kwargs['details'] = json.dumps(kwargs['details'])
            
        # Proceed with standard initialization
        super(ETLAudit, self).__init__(**kwargs)
    
    def __repr__(self):
        return f"<ETLAudit id={self.id}, action={self.action}, component={self.component}>"
    
    @classmethod
    def create_audit_entry(cls, action, user_id, component=None, batch_id=None, 
                          details=None):
        """
        Factory method to create an ETLAudit instance.
        
        Args:
            action (str): The action being performed (e.g., "process_started", "update")
            user_id (str): The ID of the user performing the action
            component (str, optional): The component performing the action
            batch_id (str, optional): The ID of the batch related to this audit
            details (dict or str, optional): Additional details about the action
            status (str, optional): Status of the action (default: "success")
            
        Returns:
            ETLAudit: A new ETLAudit instance
        """
        return cls(
            action=action,
            user_id=user_id,
            component=component,
            batch_id=batch_id,
            details=details,
            # status=status
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert audit log entry to a dictionary.
        
        Returns:
            Dict[str, Any]: Dictionary representation of the audit log
        """
        details_dict = None
        if self.details:
            try:
                details_dict = json.loads(self.details)
            except json.JSONDecodeError:
                details_dict = {"raw": self.details}
        
        return {
            "id": self.id,
            "action": self.action,
            "user_id": self.user_id,
            "component": self.component,
            "batch_id": self.batch_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "details": details_dict,
            # "status": self.status
        }