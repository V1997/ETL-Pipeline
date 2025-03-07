"""
API data models for sales records
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import date


class SalesRecord(BaseModel):
    """Model for sales record"""
    order_id: str = Field(..., description="Unique order identifier")
    region: str = Field(..., description="Sales region")
    country: str = Field(..., description="Country")
    item_type: str = Field(..., description="Type of item sold")
    sales_channel: str = Field(..., description="Sales channel (Online/Offline)")
    order_priority: str = Field(..., description="Order priority (H/M/L)")
    order_date: str = Field(..., description="Date order was placed")
    ship_date: str = Field(..., description="Date order was shipped")
    units_sold: int = Field(..., description="Number of units sold")
    unit_price: float = Field(..., description="Price per unit")
    unit_cost: float = Field(..., description="Cost per unit")
    total_revenue: float = Field(..., description="Total revenue from sale")
    total_cost: float = Field(..., description="Total cost of sale")
    total_profit: float = Field(..., description="Total profit from sale")
    batch_id: str = Field(..., description="ID of batch this record belongs to")


class RecordList(BaseModel):
    """Model for record list response"""
    records: List[SalesRecord] = Field(..., description="List of sales records")
    total_count: int = Field(..., description="Total count of records matching criteria")
    limit: int = Field(..., description="Maximum number of records per page")
    offset: int = Field(..., description="Offset for pagination")


class RecordQuery(BaseModel):
    """Model for record query parameters"""
    batch_id: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    item_type: Optional[str] = None
    sales_channel: Optional[str] = None
    order_date_from: Optional[date] = None
    order_date_to: Optional[date] = None
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    min_revenue: Optional[float] = None
    max_revenue: Optional[float] = None
    limit: int = 100
    offset: int = 0