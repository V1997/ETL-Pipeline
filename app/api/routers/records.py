"""
API router for sales records operations

Provides endpoints for querying and exporting ETL processed records.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional, Dict, Any
import pandas as pd
import io
import logging
from datetime import datetime

# Import models and services
from app.api.models.record import RecordList, RecordQuery
from app.api.auth.auth_handler import viewer_required, User
from app.api.services.record_service import get_batch_records, get_record_by_id

# Create router
router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/", 
    response_model=RecordList,
    summary="Query sales records",
    description="Query sales records with filtering options"
)
async def query_records(
    batch_id: Optional[str] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
    item_type: Optional[str] = None,
    sales_channel: Optional[str] = None,
    order_date_from: Optional[str] = None,
    order_date_to: Optional[str] = None,
    min_units: Optional[int] = None,
    max_units: Optional[int] = None,
    min_revenue: Optional[float] = None,
    max_revenue: Optional[float] = None,
    limit: int = Query(100, le=1000),
    offset: int = 0,
    current_user: User = Depends(viewer_required)
):
    """
    Query sales records with filtering options
    
    Args:
        batch_id: Filter by batch ID
        region: Filter by region
        country: Filter by country
        item_type: Filter by item type
        sales_channel: Filter by sales channel
        order_date_from: Filter by order date (from) in YYYY-MM-DD format
        order_date_to: Filter by order date (to) in YYYY-MM-DD format
        min_units: Filter by minimum units sold
        max_units: Filter by maximum units sold
        min_revenue: Filter by minimum revenue
        max_revenue: Filter by maximum revenue
        limit: Maximum number of records to return (max 1000)
        offset: Number of records to skip
        current_user: Authenticated user with viewer role
    
    Returns:
        RecordList: List of matching records
    """
    try:
        # Parse dates if provided
        from_date = None
        to_date = None
        
        if order_date_from:
            from_date = datetime.strptime(order_date_from, "%Y-%m-%d")
            
        if order_date_to:
            to_date = datetime.strptime(order_date_to, "%Y-%m-%d")
        
        # Build query filters
        query_filters = {
            "batch_id": batch_id,
            "region": region,
            "country": country,
            "item_type": item_type,
            "sales_channel": sales_channel,
            "order_date_from": from_date,
            "order_date_to": to_date,
            "min_units": min_units,
            "max_units": max_units,
            "min_revenue": min_revenue,
            "max_revenue": max_revenue
        }
        
        # Filter out None values
        query_filters = {k: v for k, v in query_filters.items() if v is not None}
        
        # Get records with filtering
        records, total_count = get_batch_records(
            filters=query_filters,
            limit=limit,
            offset=offset
        )
        
        # Return the records
        return {
            "records": records,
            "total_count": total_count,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error querying records: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error querying records: {str(e)}"
        )


@router.get(
    "/export", 
    summary="Export sales records",
    description="Export filtered sales records as CSV"
)
async def export_records(
    batch_id: Optional[str] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
    item_type: Optional[str] = None,
    sales_channel: Optional[str] = None,
    order_date_from: Optional[str] = None,
    order_date_to: Optional[str] = None,
    min_units: Optional[int] = None,
    max_units: Optional[int] = None,
    min_revenue: Optional[float] = None,
    max_revenue: Optional[float] = None,
    limit: int = Query(10000, le=100000),
    offset: int = 0,
    format: str = "csv", 
    current_user: User = Depends(viewer_required)
):
    """
    Export filtered sales records as CSV
    
    Args:
        batch_id: Filter by batch ID
        region: Filter by region
        country: Filter by country
        item_type: Filter by item type
        sales_channel: Filter by sales channel
        order_date_from: Filter by order date (from) in YYYY-MM-DD format
        order_date_to: Filter by order date (to) in YYYY-MM-DD format
        min_units: Filter by minimum units sold
        max_units: Filter by maximum units sold
        min_revenue: Filter by minimum revenue
        max_revenue: Filter by maximum revenue
        limit: Maximum number of records to export (max 100,000)
        offset: Number of records to skip
        format: Export format (csv or excel)
        current_user: Authenticated user with viewer role
    
    Returns:
        StreamingResponse: File download response
    """
    try:
        # Parse dates if provided
        from_date = None
        to_date = None
        
        if order_date_from:
            from_date = datetime.strptime(order_date_from, "%Y-%m-%d")
            
        if order_date_to:
            to_date = datetime.strptime(order_date_to, "%Y-%m-%d")
        
        # Build query filters
        query_filters = {
            "batch_id": batch_id,
            "region": region,
            "country": country,
            "item_type": item_type,
            "sales_channel": sales_channel,
            "order_date_from": from_date,
            "order_date_to": to_date,
            "min_units": min_units,
            "max_units": max_units,
            "min_revenue": min_revenue,
            "max_revenue": max_revenue
        }
        
        # Filter out None values
        query_filters = {k: v for k, v in query_filters.items() if v is not None}
        
        # Get records with filtering
        records, _ = get_batch_records(
            filters=query_filters,
            limit=limit,
            offset=offset
        )
        
        # Convert records to DataFrame
        df = pd.DataFrame([r.dict() for r in records])
        
        # Validate format
        if format.lower() not in ["csv", "excel"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unsupported export format. Use 'csv' or 'excel'"
            )
        
        # Generate filename based on parameters
        filename_parts = ["sales_records"]
        if batch_id:
            filename_parts.append(f"batch_{batch_id}")
            
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename_parts.append(timestamp)
        
        # Create in-memory file
        output = io.BytesIO()
        
        # Export to requested format
        if format.lower() == "csv":
            df.to_csv(output, index=False)
            media_type = "text/csv"
            filename = f"{'-'.join(filename_parts)}.csv"
        else:  # Excel
            df.to_excel(output, index=False)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"{'-'.join(filename_parts)}.xlsx"
        
        # Reset buffer position
        output.seek(0)
        
        # Return streaming response
        return StreamingResponse(
            output,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting records: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error exporting records: {str(e)}"
        )


@router.get(
    "/{record_id}", 
    summary="Get record by ID",
    description="Get a specific sales record by its ID"
)
async def get_record(
    record_id: str,
    current_user: User = Depends(viewer_required)
):
    """
    Get a specific sales record by its ID
    
    Args:
        record_id: Record ID to retrieve
        current_user: Authenticated user with viewer role
    
    Returns:
        dict: Record details
    """
    try:
        # Get record
        record = get_record_by_id(record_id)
        
        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Record with ID {record_id} not found"
            )
        
        return record
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving record: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving record: {str(e)}"
        )