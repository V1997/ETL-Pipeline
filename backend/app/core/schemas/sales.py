
from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import date, datetime
import re

class SalesRecordBase(BaseModel):
    region: str
    country: str
    item_type: str
    sales_channel: str
    order_priority: str
    order_date: date
    ship_date: date
    order_id: str
    units_sold: int = Field(gt=0)
    unit_price: float = Field(gt=0)
    unit_cost: float = Field(gt=0)
    total_revenue: Optional[float] = None
    total_cost: Optional[float] = None
    total_profit: Optional[float] = None

    @validator('order_id')
    def validate_order_id(cls, v):
        if not re.match(r'^[A-Za-z0-9-]+$', v):
            raise ValueError('Invalid order ID format')
        return v

    @validator('order_priority')
    def validate_priority(cls, v):
        if v not in ['L', 'M', 'H', 'C']:  # Low, Medium, High, Critical
            raise ValueError('Order priority must be L, M, H, or C')
        return v

    @validator('ship_date')
    def validate_ship_date(cls, v, values):
        if 'order_date' in values and v < values['order_date']:
            raise ValueError('Ship date cannot be before order date')
        return v

    @validator('total_revenue', 'total_cost', 'total_profit', always=True)
    def compute_totals(cls, v, values):
        if 'units_sold' in values and 'unit_price' in values and 'unit_cost' in values:
            units = values['units_sold']
            if v is None:
                if v == 'total_revenue':
                    return units * values['unit_price']
                elif v == 'total_cost':
                    return units * values['unit_cost']
                elif v == 'total_profit':
                    return (units * values['unit_price']) - (units * values['unit_cost'])
        return v

class SalesRecordCreate(SalesRecordBase):
    pass

class SalesRecordInDB(SalesRecordBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True

class ETLJobBase(BaseModel):
    job_type: str
    status: str
    records_processed: int = 0
    records_failed: int = 0
    source_file: Optional[str] = None

class ETLJobCreate(ETLJobBase):
    pass

class ETLJobInDB(ETLJobBase):
    job_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    created_at: datetime

    class Config:
        orm_mode = True

class ErrorLogBase(BaseModel):
    job_id: str
    error_type: str
    error_message: str
    severity: str
    error_details: Optional[dict] = None
    record_data: Optional[dict] = None

class ErrorLogCreate(ErrorLogBase):
    pass

class ErrorLogInDB(ErrorLogBase):
    error_id: int
    created_at: datetime

    class Config:
        orm_mode = True

class SalesAnalyticsBase(BaseModel):
    dimension_type: str
    dimension_value: str
    time_period: str
    period_start: date
    period_end: date
    total_sales: float
    total_cost: float
    total_profit: float
    units_sold: int
    avg_order_value: float

class SalesAnalyticsCreate(SalesAnalyticsBase):
    pass

class SalesAnalyticsInDB(SalesAnalyticsBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True

class BatchUploadResponse(BaseModel):
    job_id: str
    status: str
    message: str
    records_processed: int
    records_failed: int

class HealthCheckResponse(BaseModel):
    status: str
    version: str
    timestamp: datetime
    database_status: str
    kafka_status: str
    uptime_seconds: int
    system_load: Optional[dict] = None