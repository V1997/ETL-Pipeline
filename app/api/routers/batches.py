"""
API router for batch operations

Provides endpoints for creating, monitoring, and managing ETL batches.
"""

from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import pandas as pd
import io
import uuid
import logging
from datetime import datetime
from app.db.session import get_db_session
from app.models.models import SalesBatch
from app.api.models.batch import BatchResponse
# Import models and services
from app.api.models.batch import (
    BatchCreate, 
    BatchResponse,
    BatchList,
    BatchStatus,
    BatchRetry
)
from app.core.batch_processor import BatchProcessor
from app.api.auth.auth_handler import (
    admin_required, 
    operator_required,
    viewer_required,
    get_current_user,
    User
)
from app.api.services.batch_service import (
    get_batch_by_id,
    list_batches,
    get_batch_errors,
    get_batch_metrics
)

# Create router
router = APIRouter()
logger = logging.getLogger(__name__)

# Initialize batch processor
batch_processor = BatchProcessor()


@router.post(
    "/", 
    response_model=BatchResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new batch",
    description="Create a new ETL batch from uploaded CSV file"
)
async def create_batch(
    source: str,
    file: UploadFile = File(...),
    current_user: User = Depends(operator_required)
):
    """
    Create a new ETL batch from uploaded data file
    
    Args:
        source: Source identifier for the batch
        file: CSV file containing batch data
        current_user: Authenticated user with operator role
    
    Returns:
        BatchResponse: Created batch information
    """
    try:
        # Read the uploaded file
        contents = await file.read()
        
        # Parse CSV into DataFrame
        df = pd.read_csv(io.BytesIO(contents))
        
        # Create batch
        batch = batch_processor.create_batch(
            source=source,
            record_count=len(df),
            created_by=current_user.username
        )


        batch_id = BatchResponse.batch_id 
        current_time = datetime.utcnow()
        source_value = source
        username_value = current_user.username

        # Create batch record
        with get_db_session() as session:
            batch = SalesBatch(
                batch_id=batch_id,
                source=source_value,
                created_by=username_value,
                created_at=current_time,
                start_time=current_time,
                status="pending"
            )
            session.add(batch)
            session.commit()

        # Start batch processing (non-blocking) - outside the session
        batch_processor.process_batch_async(
            batch_id=batch_id,  # Use the saved value
            data_df=df,
            created_by=username_value  # Use the saved value
        )

        # Return the created batch - use saved values instead of batch object
        return {
            "batch_id": batch_id,
            "source": source_value,
            "status": "pending",
            "record_count": len(df),
            "created_by": username_value,
            "created_at": current_time.isoformat(),
            "message": "Batch created and processing started"
        }
        
    except Exception as e:
        logger.error(f"Error creating batch: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating batch: {str(e)}"
        )


@router.get(
    "/", 
    response_model=BatchList,
    summary="List batches",
    description="List all ETL batches with optional filtering"
)
async def get_batches(
    status: Optional[str] = None,
    source: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    created_by: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    current_user: User = Depends(viewer_required)
):
    """
    List all ETL batches with optional filtering
    
    Args:
        status: Filter by batch status
        source: Filter by source
        from_date: Filter by start date (YYYY-MM-DD)
        to_date: Filter by end date (YYYY-MM-DD)
        created_by: Filter by creator
        limit: Maximum number of records to return
        offset: Number of records to skip
        current_user: Authenticated user with viewer role
    
    Returns:
        BatchList: List of batches matching criteria
    """
    try:
        # Convert date strings to datetime if provided
        from_datetime = None
        to_datetime = None
        
        if from_date:
            from_datetime = datetime.strptime(from_date, "%Y-%m-%d")
        
        if to_date:
            to_datetime = datetime.strptime(to_date, "%Y-%m-%d")
        
        # Get batches with filtering
        batches = list_batches(
            status=status,
            source=source,
            from_date=from_datetime,
            to_date=to_datetime,
            created_by=created_by,
            limit=limit,
            offset=offset
        )
        
        # Count total matching records (for pagination)
        total_count = len(batches)  # This should be replaced with a proper count query
        
        # Return the batches
        return {
            "batches": batches,
            "total_count": total_count,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error listing batches: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing batches: {str(e)}"
        )


@router.get(
    "/{batch_id}", 
    response_model=BatchStatus,
    summary="Get batch status",
    description="Get detailed status information for a specific batch"
)
async def get_batch_status(
    batch_id: str,
    current_user: User = Depends(viewer_required)
):
    """
    Get detailed status information for a specific batch
    
    Args:
        batch_id: Batch ID to retrieve
        current_user: Authenticated user with viewer role
    
    Returns:
        BatchStatus: Detailed batch status information
    """
    try:
        # Get batch status
        batch_status = batch_processor.get_batch_status(batch_id)
        
        if not batch_status or batch_status.get("status") == "not_found":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Batch with ID {batch_id} not found"
            )
        
        # Get batch errors
        errors = get_batch_errors(batch_id)
        
        # Get batch metrics
        metrics = get_batch_metrics(batch_id)
        
        # Enhance batch status with errors and metrics
        batch_status["errors"] = errors
        batch_status["metrics"] = metrics
        
        return batch_status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving batch status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving batch status: {str(e)}"
        )


@router.post(
    "/{batch_id}/retry", 
    response_model=BatchResponse,
    summary="Retry a failed batch",
    description="Retry a failed batch with new data or the same data"
)
async def retry_batch(
    batch_id: str,
    retry_data: BatchRetry = None,
    file: Optional[UploadFile] = File(None),
    current_user: User = Depends(operator_required)
):

    """
    Retry a failed batch with new data or the same data
    
    Args:
        batch_id: Batch ID to retry
        retry_data: Retry configuration
        file: Optional CSV file with new data
        current_user: Authenticated user with operator role
    
    Returns:
        BatchResponse: Information about the new retry batch
    """
    try:
        # Check if the batch exists and is in a failed state
        original_batch = batch_processor.get_batch_status(batch_id)
        
        if not original_batch or original_batch.get("status") == "not_found":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Batch with ID {batch_id} not found"
            )
        
        if original_batch["status"] not in ["failed", "error"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Can only retry failed batches. Current status: {original_batch['status']}"
            )
        
        # Handle data for retry
        df = None
        
        # If file is provided, use that
        if file:
            contents = await file.read()
            df = pd.read_csv(io.BytesIO(contents))
        else:
            # Otherwise, check for special retry options
            if not retry_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Either a file or retry configuration must be provided"
                )
            
            # TODO: Implement other retry options like:
            # - Retry with original data
            # - Retry with filtered data
            # This would depend on your system's capability to retrieve the original data
        
        if df is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid data available for retry"
            )
        
        # Start the retry process
        retry_result = batch_processor.retry_batch(
            batch_id=batch_id, 
            data_df=df,
            created_by=current_user.username
        )
        
        # Return information about the new batch
        return {
            "batch_id": retry_result["batch_id"],
            "source": f"retry_{original_batch['source']}",
            "status": "processing",
            "record_count": len(df),
            "created_by": current_user.username,
            "created_at": datetime.utcnow().isoformat(),
            # "parent_batch_id": batch_id,
            "message": "Retry batch created and processing started"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrying batch: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrying batch: {str(e)}"
        )


@router.delete(
    "/{batch_id}", 
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel a batch",
    description="Cancel a running batch or mark a completed batch for deletion"
)
async def cancel_batch(
    batch_id: str,
    current_user: User = Depends(admin_required)
):
    """
    Cancel a running batch or mark a completed batch for deletion
    
    Args:
        batch_id: Batch ID to cancel
        current_user: Authenticated user with admin role
    
    Returns:
        None
    """
    try:
        # Get batch status
        batch_status = batch_processor.get_batch_status(batch_id)
        
        if not batch_status or batch_status.get("status") == "not_found":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Batch with ID {batch_id} not found"
            )
        
        # If batch is running, try to cancel it
        if batch_status["status"] == "processing":
            # Attempt to cancel the batch
            success = batch_processor.cancel_batch(batch_id, current_user.username)
            
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to cancel the batch"
                )
        else:
            # Mark batch as cancelled
            batch_processor.update_batch_status(
                batch_id=batch_id,
                status="cancelled",
                updated_by=current_user.username,
                reason="Cancelled by administrator"
            )
        
        # Successfully cancelled
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling batch: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error cancelling batch: {str(e)}"
        )