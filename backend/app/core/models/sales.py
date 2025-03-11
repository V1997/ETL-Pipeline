from sqlalchemy import Column, Integer, String, Float, Numeric, Date, DateTime, Enum, Text, JSON, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
import uuid
from datetime import datetime

Base = declarative_base()

class SalesRecord(Base):
    __tablename__ = "sales_records"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(50), unique=True, nullable=False)
    region = Column(String(100), nullable=False, index=True)
    country = Column(String(100), nullable=False, index=True)
    item_type = Column(String(100), nullable=False, index=True)
    sales_channel = Column(String(50), nullable=False)
    order_priority = Column(String(1), nullable=False)
    order_date = Column(Date, nullable=False, index=True)
    ship_date = Column(Date, nullable=False, index=True)
    units_sold = Column(Integer, nullable=False)
    unit_price = Column(Numeric(precision=10, scale=2), nullable=False)
    unit_cost = Column(Numeric(precision=10, scale=2), nullable=False)
    total_revenue = Column(Numeric(precision=15, scale=2), nullable=False)
    total_cost = Column(Numeric(precision=15, scale=2), nullable=False)
    total_profit = Column(Numeric(precision=15, scale=2), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class ETLJob(Base):
    __tablename__ = "etl_jobs"

    job_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_type = Column(Enum('BATCH', 'STREAM'), nullable=False)
    status = Column(Enum('PENDING', 'RUNNING', 'COMPLETED', 'FAILED'), nullable=False)
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    source_file = Column(String(255), nullable=True)
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

class ErrorLog(Base):
    __tablename__ = "error_logs"

    error_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("etl_jobs.job_id", ondelete="CASCADE"), nullable=False)
    error_type = Column(String(100), nullable=False)
    error_message = Column(Text, nullable=False)
    error_details = Column(JSON, nullable=True)
    severity = Column(Enum('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'), nullable=False)
    record_data = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

class SalesAnalytics(Base):
    __tablename__ = "sales_analytics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dimension_type = Column(Enum('REGION', 'COUNTRY', 'ITEM_TYPE', 'SALES_CHANNEL', 'DATE'), nullable=False)
    dimension_value = Column(String(255), nullable=False)
    time_period = Column(String(20), nullable=False)
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    total_sales = Column(Numeric(precision=15, scale=2), nullable=False)
    total_cost = Column(Numeric(precision=15, scale=2), nullable=False)
    total_profit = Column(Numeric(precision=15, scale=2), nullable=False)
    units_sold = Column(Integer, nullable=False)
    avg_order_value = Column(Numeric(precision=10, scale=2), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())