"""
API data models for batch operations
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class BatchCreate(BaseModel):
    """Model for batch creation request"""
    source: str = Field(..., description="Source of the batch data")
    description: Optional[str] = Field(None, description="Optional batch description")


class BatchResponse(BaseModel):
    """Model for batch response"""
    batch_id: str = Field(..., description="Unique identifier for the batch")
    source: str = Field(..., description="Source of the batch data")
    status: str = Field(..., description="Current status of the batch")
    record_count: int = Field(..., description="Number of records in the batch")
    created_by: str = Field(..., description="User who created the batch")
    created_at: str = Field(..., description="Timestamp when the batch was created")
    message: Optional[str] = Field(None, description="Optional status message")
    # parent_batch_id: Optional[str] = Field(None, description="ID of parent batch if this is a retry")


class BatchSummary(BaseModel):
    """Model for batch list item"""
    batch_id: str = Field(..., description="Unique identifier for the batch")
    source: str = Field(..., description="Source of the batch data")
    status: str = Field(..., description="Current status of the batch")
    record_count: int = Field(..., description="Number of records in the batch")
    error_count: int = Field(..., description="Number of error records")
    created_by: str = Field(..., description="User who created the batch")
    start_time: str = Field(..., description="Timestamp when processing started")
    end_time: Optional[str] = Field(None, description="Timestamp when processing completed")
    processing_time: Optional[float] = Field(None, description="Processing time in seconds")


class BatchList(BaseModel):
    """Model for batch list response"""
    batches: List[BatchSummary] = Field(..., description="List of batches")
    total_count: int = Field(..., description="Total count of batches matching criteria")
    limit: int = Field(..., description="Maximum number of batches per page")
    offset: int = Field(..., description="Offset for pagination")


class BatchError(BaseModel):
    """Model for batch error details"""
    error_id: str = Field(..., description="Unique identifier for the error")
    error_type: str = Field(..., description="Type of error")
    error_message: str = Field(..., description="Error message")
    component: str = Field(..., description="Component where error occurred")
    severity: str = Field(..., description="Error severity (info, warning, error, critical)")
    timestamp: str = Field(..., description="Timestamp when error occurred")


class BatchMetric(BaseModel):
    """Model for batch metric details"""
    metric_name: str = Field(..., description="Name of the metric")
    metric_value: float = Field(..., description="Value of the metric")
    component: str = Field(..., description="Component the metric belongs to")
    timestamp: str = Field(..., description="Timestamp when metric was recorded")


class BatchStatus(BaseModel):
    """Model for detailed batch status"""
    batch_id: str = Field(..., description="Unique identifier for the batch")
    source: str = Field(..., description="Source of the batch data")
    status: str = Field(..., description="Current status of the batch")
    record_count: int = Field(..., description="Number of records in the batch")
    error_count: int = Field(..., description="Number of error records")
    created_by: str = Field(..., description="User who created the batch")
    start_time: str = Field(..., description="Timestamp when processing started")
    end_time: Optional[str] = Field(None, description="Timestamp when processing completed")
    processing_time: Optional[float] = Field(None, description="Processing time in seconds")
    metrics: Dict[str, Any] = Field(..., description="Batch processing metrics")
    errors: List[BatchError] = Field([], description="List of errors for this batch")
    # parent_batch_id: Optional[str] = Field(None, description="ID of parent batch if this is a retry")


class BatchRetry(BaseModel):
    """Model for batch retry options"""
    use_original_data: bool = Field(False, description="Use the original batch data for retry")
    filter_invalid: bool = Field(True, description="Filter out invalid records from original batch")