"""
API data models for metrics and analytics
"""

from pydantic import BaseModel, Field
from enum import Enum
from typing import List, Dict, Any, Optional
from datetime import datetime


class MetricAggregation(str, Enum):
    """Metric aggregation type"""
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    COUNT = "count"


class MetricDataPoint(BaseModel):
    """Single metric data point"""
    timestamp: str = Field(..., description="Timestamp for the metric")
    value: float = Field(..., description="Value of the metric")


class MetricResponse(BaseModel):
    """Model for metric response"""
    metric_name: str = Field(..., description="Name of the metric")
    description: str = Field(..., description="Description of the metric")
    aggregation: str = Field(..., description="Type of aggregation used")
    unit: str = Field(..., description="Unit of measurement")
    data_points: List[MetricDataPoint] = Field(..., description="Series of metric values over time")