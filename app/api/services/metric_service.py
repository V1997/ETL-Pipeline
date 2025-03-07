"""
Service layer for metrics and analytics

Provides functions for retrieving and analyzing ETL metrics.
"""

from typing import List, Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import os
import psutil
import pandas as pd

from app.db.session import get_db_session
from app.models.models import ETLMetric, SalesBatch
from sqlalchemy import func, case, literal_column
from sqlalchemy.sql import text

# Configure logging
logger = logging.getLogger(__name__)


def get_system_metrics() -> Dict[str, Any]:
    """
    Get current system metrics
    
    Returns:
        Dict: System metrics
    """
    try:
        # Get system metrics
        cpu_percent = psutil.cpu_percent(interval=0.5)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Get database metrics
        db_metrics = {}
        with get_db_session() as session:
            # Count batches by status
            batch_counts = (
                session.query(
                    SalesBatch.status,
                    func.count(SalesBatch.batch_id).label('count')
                )
                .group_by(SalesBatch.status)
                .all()
            )
            
            # Convert to dictionary
            status_counts = {}
            for status, count in batch_counts:
                status_counts[status] = count
            
            db_metrics["batch_counts"] = status_counts
            
            # Get recent activity
            recent_batches = (
                session.query(SalesBatch)
                .order_by(SalesBatch.start_time.desc())
                .limit(5)
                .all()
            )
            
            recent_activity = []
            for batch in recent_batches:
                recent_activity.append({
                    "batch_id": batch.batch_id,
                    "source": batch.source,
                    "status": batch.status,
                    "start_time": batch.start_time.isoformat() if batch.start_time else None,
                    "created_by": batch.created_by
                })
            
            db_metrics["recent_activity"] = recent_activity
        
        # Assemble system metrics
        metrics = {
            "system": {
                "cpu_usage_percent": cpu_percent,
                "memory_total_mb": memory.total / (1024 * 1024),
                "memory_available_mb": memory.available / (1024 * 1024),
                "memory_used_percent": memory.percent,
                "disk_total_gb": disk.total / (1024 * 1024 * 1024),
                "disk_free_gb": disk.free / (1024 * 1024 * 1024),
                "disk_used_percent": disk.percent
            },
            "database": db_metrics,
            # "timestamp": date.timestamp
            "timestamp": "2025-03-06 05:38:10"
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error retrieving system metrics: {str(e)}")
        raise


def get_batch_processing_stats(
    from_date: datetime,
    to_date: datetime,
    group_by: str = "day"
) -> List[Dict[str, Any]]:
    """
    Get batch processing statistics grouped by time period
    
    Args:
        from_date: Start date for statistics
        to_date: End date for statistics
        group_by: Grouping period (hour, day, week, month)
        
    Returns:
        List[Dict]: List of batch processing statistics by time period
    """
    try:
        with get_db_session() as session:
            # Define time grouping SQL based on group_by parameter
            if group_by == "hour":
                # Group by hour
                time_format = func.date_trunc('hour', SalesBatch.start_time)
                time_format_label = "Hourly"
            elif group_by == "day":
                # Group by day
                time_format = func.date_trunc('day', SalesBatch.start_time)
                time_format_label = "Daily"
            elif group_by == "week":
                # Group by week
                time_format = func.date_trunc('week', SalesBatch.start_time)
                time_format_label = "Weekly"
            elif group_by == "month":
                # Group by month
                time_format = func.date_trunc('month', SalesBatch.start_time)
                time_format_label = "Monthly"
            else:
                # Default to day
                time_format = func.date_trunc('day', SalesBatch.start_time)
                time_format_label = "Daily"
            
            # Get batch counts by time and status
            query_results = (
                session.query(
                    time_format.label('period'),
                    SalesBatch.status,
                    func.count(SalesBatch.batch_id).label('count'),
                    func.sum(SalesBatch.record_count).label('record_count'),
                    func.sum(SalesBatch.error_count).label('error_count'),
                    func.avg(
                        case(
                            [(SalesBatch.end_time != None, 
                              func.extract('epoch', SalesBatch.end_time - SalesBatch.start_time))],
                            else_=None
                        )
                    ).label('avg_duration')
                )
                .filter(SalesBatch.start_time >= from_date)
                .filter(SalesBatch.start_time <= to_date)
                .group_by(time_format, SalesBatch.status)
                .order_by(time_format, SalesBatch.status)
                .all()
            )
            
            # Process results into appropriate format for the API
            # First, organize by time period
            period_data = {}
            for period, status, count, record_count, error_count, avg_duration in query_results:
                # Skip None periods
                if not period:
                    continue
                    
                period_str = period.isoformat()
                
                if period_str not in period_data:
                    period_data[period_str] = {
                        "timestamp": period_str,
                        "statuses": {},
                        "totals": {
                            "batches": 0,
                            "records": 0,
                            "errors": 0
                        }
                    }
                
                # Add status data
                period_data[period_str]["statuses"][status] = {
                    "count": count,
                    "record_count": record_count if record_count else 0,
                    "error_count": error_count if error_count else 0,
                    "avg_duration_seconds": float(avg_duration) if avg_duration else None
                }
                
                # Update totals
                period_data[period_str]["totals"]["batches"] += count
                period_data[period_str]["totals"]["records"] += (record_count if record_count else 0)
                period_data[period_str]["totals"]["errors"] += (error_count if error_count else 0)
            
            # Convert to metrics format
            metrics = []
            
            # Total batches by time period
            data_points = []
            for period_str, data in sorted(period_data.items()):
                data_points.append({
                    "timestamp": period_str,
                    "value": data["totals"]["batches"]
                })
            
            metrics.append({
                "metric_name": "total_batches",
                "description": f"{time_format_label} batch count",
                "aggregation": "sum",
                "unit": "batches",
                "data_points": data_points
            })
            
            # Total records by time period
            data_points = []
            for period_str, data in sorted(period_data.items()):
                data_points.append({
                    "timestamp": period_str,
                    "value": data["totals"]["records"]
                })
            
            metrics.append({
                "metric_name": "total_records",
                "description": f"{time_format_label} record count",
                "aggregation": "sum",
                "unit": "records",
                "data_points": data_points
            })
            
            # Error rate by time period
            data_points = []
            for period_str, data in sorted(period_data.items()):
                if data["totals"]["records"] > 0:
                    error_rate = data["totals"]["errors"] / data["totals"]["records"]
                else:
                    error_rate = 0.0
                    
                data_points.append({
                    "timestamp": period_str,
                    "value": error_rate
                })
            
            metrics.append({
                "metric_name": "error_rate",
                "description": f"{time_format_label} error rate",
                "aggregation": "avg",
                "unit": "percentage",
                "data_points": data_points
            })
            
            # Status breakdown
            all_statuses = set()
            for data in period_data.values():
                all_statuses.update(data["statuses"].keys())
                
            for status in sorted(all_statuses):
                data_points = []
                for period_str, data in sorted(period_data.items()):
                    count = data["statuses"].get(status, {}).get("count", 0)
                    data_points.append({
                        "timestamp": period_str,
                        "value": count
                    })
                
                metrics.append({
                    "metric_name": f"status_{status}",
                    "description": f"{time_format_label} {status} batch count",
                    "aggregation": "sum",
                    "unit": "batches",
                    "data_points": data_points
                })
            
            return metrics
            
    except Exception as e:
        logger.error(f"Error retrieving batch statistics: {str(e)}")
        raise


def get_data_quality_metrics(
    from_date: datetime,
    to_date: datetime,
    source: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get data quality metrics across batches
    
    Args:
        from_date: Start date for metrics
        to_date: End date for metrics
        source: Filter by source
        
    Returns:
        List[Dict]: Data quality metrics
    """
    try:
        with get_db_session() as session:
            # Base query for batches
            batch_query = (
                session.query(SalesBatch)
                .filter(SalesBatch.start_time >= from_date)
                .filter(SalesBatch.start_time <= to_date)
                .filter(SalesBatch.status == "completed")
            )
            
            # Apply source filter if provided
            if source:
                batch_query = batch_query.filter(SalesBatch.source == source)
            
            # Get batches
            batches = batch_query.all()
            
            # Get their metrics
            batch_ids = [batch.batch_id for batch in batches]
            
            if not batch_ids:
                # Return empty metrics if no batches
                return []
            
            # Get metrics
            metrics_query = (
                session.query(
                    ETLMetric.metric_name,
                    func.avg(ETLMetric.metric_value).label("avg_value"),
                    func.min(ETLMetric.metric_value).label("min_value"),
                    func.max(ETLMetric.metric_value).label("max_value"),
                    func.stddev(ETLMetric.metric_value).label("stddev_value"),
                    SalesBatch.start_time
                )
                .join(SalesBatch, ETLMetric.batch_id == SalesBatch.batch_id)
                .filter(ETLMetric.batch_id.in_(batch_ids))
                .filter(ETLMetric.metric_name.in_([
                    "error_rate", 
                    "valid_records", 
                    "invalid_records",
                    "modified_records"
                ]))
                .group_by(
                    ETLMetric.metric_name,
                    func.date_trunc('day', SalesBatch.start_time)
                )
                .order_by(
                    ETLMetric.metric_name,
                    func.date_trunc('day', SalesBatch.start_time)
                )
                .all()
            )
            
            # Process into the required format
            metrics_by_name = {}
            for metric_name, avg_value, min_value, max_value, stddev_value, start_time in metrics_query:
                if metric_name not in metrics_by_name:
                    metrics_by_name[metric_name] = {
                        "metric_name": metric_name,
                        "description": f"Data quality metric: {metric_name}",
                        "aggregation": "avg",
                        "unit": "value",
                        "data_points": []
                    }
                
                # Format date
                date_str = start_time.date().isoformat() if start_time else "2025-03-06"
                
                metrics_by_name[metric_name]["data_points"].append({
                    "timestamp": date_str,
                    "value": float(avg_value) if avg_value is not None else 0.0
                })
                
            # Convert to list
            return list(metrics_by_name.values())
            
    except Exception as e:
        logger.error(f"Error retrieving data quality metrics: {str(e)}")
        raise


def get_etl_performance_metrics(
    from_date: datetime,
    to_date: datetime,
    metric_type: str = "processing_time",
    aggregation: str = "avg"
) -> List[Dict[str, Any]]:
    """
    Get ETL performance metrics
    
    Args:
        from_date: Start date for metrics
        to_date: End date for metrics
        metric_type: Type of metric to retrieve (processing_time, records_per_second, etc.)
        aggregation: Aggregation method (avg, max, min, sum)
        
    Returns:
        List[Dict]: ETL performance metrics
    """
    try:
        # Validate metric type
        valid_metrics = [
            "processing_time",
            "records_per_second",
            "error_rate",
            "memory_usage",
            "cpu_usage"
        ]
        
        if metric_type not in valid_metrics:
            raise ValueError(f"Invalid metric type: {metric_type}. Must be one of: {valid_metrics}")
            
        # Validate aggregation
        valid_aggregations = ["avg", "max", "min", "sum"]
        if aggregation not in valid_aggregations:
            raise ValueError(f"Invalid aggregation: {aggregation}. Must be one of: {valid_aggregations}")
        
        # Set up aggregation function
        if aggregation == "avg":
            agg_func = func.avg
            agg_label = "Average"
        elif aggregation == "max":
            agg_func = func.max
            agg_label = "Maximum"
        elif aggregation == "min":
            agg_func = func.min
            agg_label = "Minimum"
        else:  # sum
            agg_func = func.sum
            agg_label = "Total"
        
        with get_db_session() as session:
            # Get metrics by day
            query_results = (
                session.query(
                    func.date_trunc('day', SalesBatch.start_time).label('day'),
                    agg_func(ETLMetric.metric_value).label('value')
                )
                .join(SalesBatch, ETLMetric.batch_id == SalesBatch.batch_id)
                .filter(SalesBatch.start_time >= from_date)
                .filter(SalesBatch.start_time <= to_date)
                .filter(ETLMetric.metric_name == metric_type)
                .group_by(func.date_trunc('day', SalesBatch.start_time))
                .order_by(func.date_trunc('day', SalesBatch.start_time))
                .all()
            )
            
            # Process into the required format
            data_points = []
            for day, value in query_results:
                if day:
                    data_points.append({
                        "timestamp": day.date().isoformat(),
                        "value": float(value) if value is not None else 0.0
                    })
            
            # Get units and description based on metric type
            units = {
                "processing_time": "seconds",
                "records_per_second": "records/second",
                "error_rate": "percentage",
                "memory_usage": "MB",
                "cpu_usage": "percentage"
            }
            
            descriptions = {
                "processing_time": "Batch processing duration",
                "records_per_second": "Record processing throughput",
                "error_rate": "Percentage of invalid records",
                "memory_usage": "Memory consumption during processing",
                "cpu_usage": "CPU utilization during processing"
            }
            
            # Return a single metric
            return [{
                "metric_name": metric_type,
                "description": f"{agg_label} {descriptions.get(metric_type, metric_type)}",
                "aggregation": aggregation,
                "unit": units.get(metric_type, "value"),
                "data_points": data_points
            }]
            
    except Exception as e:
        logger.error(f"Error retrieving ETL performance metrics: {str(e)}")
        raise