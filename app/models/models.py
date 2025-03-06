from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, 
    ForeignKey, Text, TIMESTAMP, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

# Create base class for models
Base = declarative_base()

class SalesBatch(Base):
    """SQLAlchemy model for sales_batches table"""
    __tablename__ = "sales_batches"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(50), unique=True, nullable=False)
    source = Column(String(100), nullable=False)
    record_count = Column(Integer, nullable=False, default=0)
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="pending")
    error_count = Column(Integer, nullable=False, default=0)
    created_by = Column(String(100), nullable=True)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    
    # Relationships
    sales_records = relationship("SalesRecord", back_populates="batch", cascade="all, delete-orphan")
    etl_errors = relationship("ETLError", back_populates="batch", cascade="all, delete-orphan")
    etl_metrics = relationship("ETLMetric", back_populates="batch", cascade="all, delete-orphan")
    etl_audits = relationship("ETLAudit", back_populates="batch", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<SalesBatch(batch_id='{self.batch_id}', status='{self.status}', records={self.record_count})>"


class SalesRecord(Base):
    """SQLAlchemy model for sales_records table"""
    __tablename__ = "sales_records"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(50), unique=True, nullable=False)
    region = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    item_type = Column(String(100), nullable=False)
    sales_channel = Column(String(50), nullable=False)
    order_priority = Column(String(1), nullable=False)
    order_date = Column(DateTime, nullable=False)
    ship_date = Column(DateTime, nullable=False)
    units_sold = Column(Integer, nullable=False)
    unit_price = Column(Float(precision=10, decimal_return_scale=2), nullable=False)
    unit_cost = Column(Float(precision=10, decimal_return_scale=2), nullable=False)
    total_revenue = Column(Float(precision=12, decimal_return_scale=2), nullable=False)
    total_cost = Column(Float(precision=12, decimal_return_scale=2), nullable=False)
    total_profit = Column(Float(precision=12, decimal_return_scale=2), nullable=False)
    processed_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    is_valid = Column(Boolean, nullable=False, default=True)
    batch_id = Column(String(50), ForeignKey("sales_batches.batch_id"), nullable=False)
    
    # Relationship
    batch = relationship("SalesBatch", back_populates="sales_records")
    
    def __repr__(self):
        return f"<SalesRecord(order_id='{self.order_id}', region='{self.region}', profit={self.total_profit})>"


class ETLError(Base):
    """SQLAlchemy model for etl_errors table"""
    __tablename__ = "etl_errors"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    error_type = Column(String(100), nullable=False)
    error_message = Column(Text, nullable=False)
    component = Column(String(100), nullable=False)
    severity = Column(String(20), nullable=False)
    record_id = Column(String(100), nullable=True)
    batch_id = Column(String(50), ForeignKey("sales_batches.batch_id"), nullable=True)
    timestamp = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    resolved = Column(Boolean, nullable=False, default=False)
    resolution_note = Column(Text, nullable=True)
    
    # Relationship
    batch = relationship("SalesBatch", back_populates="etl_errors")
    
    def __repr__(self):
        return f"<ETLError(type='{self.error_type}', component='{self.component}', resolved={self.resolved})>"


class ETLMetric(Base):
    """SQLAlchemy model for etl_metrics table"""
    __tablename__ = "etl_metrics"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Float, nullable=False)
    component = Column(String(100), nullable=False)
    timestamp = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    batch_id = Column(String(50), ForeignKey("sales_batches.batch_id"), nullable=True)
    
    # Relationship
    batch = relationship("SalesBatch", back_populates="etl_metrics")
    
    def __repr__(self):
        return f"<ETLMetric(name='{self.metric_name}', value={self.metric_value}, component='{self.component}')>"


class ETLAudit(Base):
    """SQLAlchemy model for etl_audit table"""
    __tablename__ = "etl_audit"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String(100), nullable=False)
    component = Column(String(100), nullable=False)
    details = Column(Text, nullable=True)
    user_id = Column(String(100), nullable=True)
    timestamp = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    batch_id = Column(String(50), ForeignKey("sales_batches.batch_id"), nullable=True)
    
    # Relationship
    batch = relationship("SalesBatch", back_populates="etl_audits")
    
    def __repr__(self):
        return f"<ETLAudit(action='{self.action}', component='{self.component}', user='{self.user_id}')>"