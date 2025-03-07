"""
API router for metrics and analytics

Provides endpoints for accessing ETL metrics and analytics.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional, Dict, Any
import pandas as pd
import logging
from datetime import datetime, timedelta

# Import models and services
from app.api.models.metric import MetricResponse, MetricAggregation
from app.api.auth.auth_handler import viewer_required, User
from app.api.services.metric_service import (
    get_system_metrics,
    get_batch_processing_stats,
    get_data_quality_metrics,
    get_etl_performance_metrics
)

# Create router
router = APIRouter()
logger = logging.getLogger(__name__)

@router.get(
    "/system", 
    response_model=Dict[str, Any],
    summary="Get system metrics",
    description="Get current system metrics and health information"
)
async def get_system_metrics_api(
    current_user: User = Depends(viewer_required)
):
    """
    Get current system metrics and health information
    
    Args:
        current_user: Authenticated user with viewer role
    
    Returns:
        Dict: System metrics
    """
    try:
        # Get system metrics
        metrics = get_system_metrics()
        
        return {
            "timestamp": "2025-03-06 05:16:02",
            "metrics": metrics
        }
        
    except Exception as e:
        logger.error(f"Error retrieving system metrics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving system metrics: {str(e)}"
        )


@router.get(
    "/batches/stats", 
    response_model=List[MetricResponse],
    summary="Get batch processing statistics",
    description="Get aggregated statistics about batch processing"
)
async def get_batch_stats(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    group_by: str = "day",
    current_user: User = Depends(viewer_required)
):
    """
    Get aggregated statistics about batch processing
    
    Args:
        from_date: Start date for metrics (YYYY-MM-DD)
        to_date: End date for metrics (YYYY-MM-DD)
        group_by: Aggregation period (hour, day, week, month)
        current_user: Authenticated user with viewer role
    
    Returns:
        List[MetricResponse]: Batch processing metrics
    """
    try:
        # Parse dates if provided or use defaults
        if from_date:
            from_datetime = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            # Default to last 30 days
            from_datetime = datetime.strptime("2025-03-06 05:16:02", "%Y-%m-%d %H:%M:%S") - timedelta(days=30)
        
        if to_date:
            to_datetime = datetime.strptime(to_date, "%Y-%m-%d")
        else:
            # Default to now
            to_datetime = datetime.strptime("2025-03-06 05:16:02", "%Y-%m-%d %H:%M:%S")
        
        # Validate group_by parameter
        valid_groupings = ["hour", "day", "week", "month"]
        if group_by not in valid_groupings:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid group_by parameter. Must be one of: {', '.join(valid_groupings)}"
            )
        
        # Get batch statistics
        stats = get_batch_processing_stats(
            from_date=from_datetime,
            to_date=to_datetime,
            group_by=group_by
        )
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving batch statistics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving batch statistics: {str(e)}"
        )


@router.get(
    "/quality", 
    response_model=List[MetricResponse],
    summary="Get data quality metrics",
    description="Get data quality metrics across batches"
)
async def get_data_quality(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    source: Optional[str] = None,
    current_user: User = Depends(viewer_required)
):
    """
    Get data quality metrics across batches
    
    Args:
        from_date: Start date for metrics (YYYY-MM-DD)
        to_date: End date for metrics (YYYY-MM-DD)
        source: Filter by source
        current_user: Authenticated user with viewer role
    
    Returns:
        List[MetricResponse]: Data quality metrics
    """
    try:
        # Parse dates if provided or use defaults
        if from_date:
            from_datetime = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            # Default to last 30 days
            from_datetime = datetime.strptime("2025-03-06 05:16:02", "%Y-%m-%d %H:%M:%S") - timedelta(days=30)
        
        if to_date:
            to_datetime = datetime.strptime(to_date, "%Y-%m-%d")
        else:
            # Default to now
            to_datetime = datetime.strptime("2025-03-06 05:16:02", "%Y-%m-%d %H:%M:%S")
        
        # Get data quality metrics
        metrics = get_data_quality_metrics(
            from_date=from_datetime,
            to_date=to_datetime,
            source=source
        )
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error retrieving data quality metrics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving data quality metrics: {str(e)}"
        )


@router.get(
    "/performance", 
    response_model=List[MetricResponse],
    summary="Get ETL performance metrics",
    description="Get performance metrics for ETL processing"
)
async def get_performance_metrics(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    metric_type: str = "processing_time",
    aggregation: MetricAggregation = "avg",
    current_user: User = Depends(viewer_required)
):
    """
    Get performance metrics for ETL processing
    
    Args:
        from_date: Start date for metrics (YYYY-MM-DD)
        to_date: End date for metrics (YYYY-MM-DD)
        metric_type: Type of metric to retrieve (processing_time, records_per_second, etc.)
        aggregation: Aggregation method (avg, max, min, sum)
        current_user: Authenticated user with viewer role
    
    Returns:
        List[MetricResponse]: Performance metrics
    """
    try:
        # Parse dates if provided or use defaults
        if from_date:
            from_datetime = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            # Default to last 30 days
            from_datetime = datetime.strptime("2025-03-06 05:16:02", "%Y-%m-%d %H:%M:%S") - timedelta(days=30)
        
        if to_date:
            to_datetime = datetime.strptime(to_date, "%Y-%m-%d")
        else:
            # Default to now
            to_datetime = datetime.strptime("2025-03-06 05:16:02", "%Y-%m-%d %H:%M:%S")
        
        # Get performance metrics
        metrics = get_etl_performance_metrics(
            from_date=from_datetime,
            to_date=to_datetime,
            metric_type=metric_type,
            aggregation=aggregation
        )
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error retrieving performance metrics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving performance metrics: {str(e)}"
        )