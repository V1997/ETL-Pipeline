from fastapi import APIRouter, Depends, Query, HTTPException, File, UploadFile
from sqlalchemy.orm import Session
from typing import List, Optional
from sqlalchemy import func, and_
from ..db.database import get_db
from ..core.models.sales import ETLJob, SalesRecord, SalesAnalytics, ErrorLog
from ..core.schemas.sales import (
    SalesRecordCreate, SalesRecordInDB, ETLJobInDB, 
    SalesAnalyticsInDB, BatchUploadResponse, HealthCheckResponse
)
import psutil
import time
import uuid 
import os
import aiofiles
from ..core.etl.extract import DataExtractor
from ..kafka.producer import KafkaProducerService
from ..config.settings import settings
from datetime import datetime
from sqlalchemy import desc
from sqlalchemy.future import select
import logging
from sqlalchemy.ext.asyncio import AsyncSession


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ETL Job routes
 
router = APIRouter()

@router.post("/etl/batch-upload", response_model=BatchUploadResponse, tags=["ETL"])
async def upload_batch_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload a batch file (Excel or CSV)
    Starts an ETL job to process the file in the background.
    """
    # Validate file type
    if not file.filename.endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Invalid file format. Only CSV and Excel files are supported."
        )
    try:
        # Generate a unique job ID
        job_id = str(uuid.uuid4())[:8]
        upload_folder = settings.DATA_UPLOAD_FOLDER
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, f"Vasu_{file.filename}")

        # Save the file asynchronously
        async with aiofiles.open(file_path, 'wb') as out_file:
            content = await file.read()  # Ensure this is awaited
            await out_file.write(content)  # Write the bytes content

        # Start the ETL process
        extractor = DataExtractor()
        # f"{job_id}_{file.filename}"
        job_id = await extractor.extract_from_file(file, job_id, file_path, db)  # Ensure this is awaited 

        # Return the response
        return BatchUploadResponse(
            job_id=job_id,
            status="RUNNING",
            message="File uploaded and ETL job started",
            records_processed=0,
            records_failed=0
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"ETL job failed: {str(e)}"
        )
        
@router.post("/etl/stream-record", response_model=dict, tags=["ETL"])
def stream_single_record(
    record: SalesRecordCreate,
    db: Session = Depends(get_db)
):
    """
    Stream a single sales record through Kafka
    """
    try:
        # Convert to dict
        record_dict = record.dict()
        
        # Send to Kafka
        producer = KafkaProducerService()
        producer.send_sales_record(record_dict)
        
        return {
            "status": "success",
            "message": "Record sent to stream",
            "record_id": record_dict.get("order_id", "unknown")
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stream record: {str(e)}"
        )

@router.get("/etl/jobs", response_model=List[ETLJobInDB], tags=["ETL"])
async def get_etl_jobs(
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Get ETL job history with optional filtering
    """
    # Create a base query using `select`
    query = select(ETLJob)

    # Apply filters if specified
    if status:
        query = query.where(ETLJob.status == status)

    if job_type:
        query = query.where(ETLJob.job_type == job_type)

    # Add ordering, offset, and limit
    query = query.order_by(ETLJob.start_time.desc()).offset(skip).limit(limit)

    # Execute the query
    result = await db.execute(query)

    # Fetch all results as a list of ETLJob objects
    jobs = result.scalars().all()

    return jobs

@router.get("/etl/jobs/{job_id}", response_model=ETLJobInDB, tags=["ETL"])
async def get_etl_job(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific ETL job
    """
    # Create the query
    query = select(ETLJob).where(ETLJob.job_id == job_id)

    # Execute the query
    result = await db.execute(query)

    # Fetch the first result
    job = result.scalar_one_or_none()  # Fetch the ETLJob object or None if not found

    if not job:
        # Raise 404 if the job is not found
        raise HTTPException(status_code=404, detail="ETL job not found")

    return job

@router.get("/etl/errors", response_model=List[dict], tags=["ETL"])
async def get_etl_errors(
    job_id: Optional[str] = None,
    severity: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get ETL error logs with optional filtering
    """

    # Start with the base query
    query = select(ErrorLog)

    # Apply filters conditionally
    if job_id:
        query = query.where(ErrorLog.job_id == job_id)

    if severity:
        query = query.where(ErrorLog.severity == severity)

    # Add ordering, offset, and limit
    query = query.order_by(ErrorLog.created_at.desc()).offset(skip).limit(limit)

    # Execute the query
    result = await db.execute(query)

    # Fetch all results as a list of ErrorLog objects
    errors = result.scalars().all()

    return [
        {
            "error_id": error.error_id,
            "job_id": error.job_id,
            "error_type": error.error_type,
            "error_message": error.error_message,
            "severity": error.severity,
            "error_details": error.error_details,
            "created_at": error.created_at
        }
        for error in errors
    ]

# Sales Data Routes
@router.get("/sales", response_model=List[SalesRecordInDB], tags=["Sales"])
async def get_sales_records(
    region: Optional[str] = None,
    country: Optional[str] = None,
    item_type: Optional[str] = None,
    sales_channel: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get sales records with optional filtering
    """

    # Create a base query with `select()`
    query = select(SalesRecord)

    # Apply filters conditionally
    if region:
        query = query.where(SalesRecord.region == region)
    
    if country:
        query = query.where(SalesRecord.country == country)
    
    if item_type:
        query = query.where(SalesRecord.item_type == item_type)
    
    if sales_channel:
        query = query.where(SalesRecord.sales_channel == sales_channel)
    
    if start_date:
        query = query.where(SalesRecord.order_date >= start_date)
    
    if end_date:
        query = query.where(SalesRecord.order_date <= end_date)
    
    # Add ordering, offset, and limit
    query = query.order_by(SalesRecord.order_date.desc()).offset(skip).limit(limit)

    # Execute the query asynchronously
    result = await db.execute(query)

    # Fetch all results as a list of SalesRecord objects
    records = result.scalars().all()
    return records

@router.get("/sales/analytics", response_model=List[SalesAnalyticsInDB], tags=["Sales"])
async def get_sales_analytics(
    dimension_type: str = Query(..., description="Type of dimension: REGION, COUNTRY, ITEM_TYPE, SALES_CHANNEL, DATE"),
    time_period: str = Query("MONTHLY", description="Time period: DAILY, WEEKLY, MONTHLY, YEARLY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dimension_value: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get sales analytics data by dimension
    """

    # Create the base query using `select()`
    query = select(SalesAnalytics).where(
        and_(
            SalesAnalytics.dimension_type == dimension_type,
            SalesAnalytics.time_period == time_period
        )
    )
    
    # Add additional filters conditionally
    if dimension_value:
        query = query.where(SalesAnalytics.dimension_value == dimension_value)
    
    if start_date:
        query = query.where(SalesAnalytics.period_start >= start_date)
    
    if end_date:
        query = query.where(SalesAnalytics.period_start <= end_date)
    
    # Execute the query with ordering, offset, and limit
    result = await db.execute(
        query.order_by(SalesAnalytics.period_start.desc())
             .offset(skip)
             .limit(limit)
    )

    # Fetch all results
    analytics = result.scalars().all()
    return analytics


@router.get("/sales/overview", response_model=dict, tags=["Sales"])
async def get_sales_overview(db: Session = Depends(get_db)):
    """
    Get an overview of sales data with key metrics.
    """

    # Total Records
    result = await db.execute(select(func.count()).select_from(SalesRecord))
    total_records = result.scalar()  # Extracts the count value

    # Latest Updated Timestamp
    result = await db.execute(
        select(SalesRecord.updated_at).order_by(desc(SalesRecord.updated_at))
    )
    latest_record = result.scalar()  
    latest_timestamp = latest_record if latest_record else None

    # Total Revenue, Cost, Profit
    totals_result = await db.execute(
        select(
            func.sum(SalesRecord.total_revenue).label("total_revenue"),
            func.sum(SalesRecord.total_cost).label("total_cost"),
            func.sum(SalesRecord.total_profit).label("total_profit"),
            func.sum(SalesRecord.units_sold).label("total_units")
        )
    )
    totals = totals_result.one_or_none()

    # Top Regions
    top_regions_result = await db.execute(
        select(
            SalesRecord.region,
            func.sum(SalesRecord.total_profit).label("total_profit")
        )
        .group_by(SalesRecord.region)
        .order_by(func.sum(SalesRecord.total_profit).desc())
        .limit(5)
    )
    top_regions = top_regions_result.all()

    # Top Countries
    top_countries_result = await db.execute(
        select(
            SalesRecord.country,
            func.sum(SalesRecord.total_profit).label("total_profit")
        )
        .group_by(SalesRecord.country)
        .order_by(func.sum(SalesRecord.total_profit).desc())
        .limit(5)
    )
    top_countries = top_countries_result.all()

    # Top Item Types
    top_item_types_result = await db.execute(
        select(
            SalesRecord.item_type,
            func.sum(SalesRecord.total_profit).label("total_profit")
        )
        .group_by(SalesRecord.item_type)
        .order_by(func.sum(SalesRecord.total_profit).desc())
        .limit(5)
    )
    top_item_types = top_item_types_result.all()  # Correctly fetch all rows

    # Return the Sales Overview Data
    return {
        "total_records": total_records,
        "latest_update": latest_timestamp,
        "total_revenue": float(totals.total_revenue or 0),
        "total_cost": float(totals.total_cost or 0),
        "total_profit": float(totals.total_profit or 0),
        "total_units_sold": int(totals.total_units or 0),
        "top_regions": [
            {"region": region, "profit": float(profit)}
            for region, profit in top_regions
        ],
        "top_countries": [
            {"country": country, "profit": float(profit)}
            for country, profit in top_countries
        ],
        "top_item_types": [
            {"item_type": item_type, "profit": float(profit)}
            for item_type, profit in top_item_types
        ],
    }

# Health and Status Routes
@router.get("/health", response_model=HealthCheckResponse, tags=["System"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """
    Check the health status of the ETL system
    """

    # Check database connectivity
    db_status = "healthy"
    try:
        # Correctly await the query
        result = await db.execute("SELECT 1")
        await result.fetchone()  # Fetch result properly (if applicable)
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"

    # Check Kafka connectivity
    kafka_status = "healthy"
    try:
        producer = KafkaProducerService()  # Simple connectivity check
    except Exception as e:
        kafka_status = f"unhealthy: {str(e)}"

    # Get uptime
    process = psutil.Process(os.getpid())
    uptime_seconds = int(time.time() - process.create_time())

    # Get system load
    system_load = {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage('/').percent
    }

    return HealthCheckResponse(
        status="healthy" if db_status == "healthy" and kafka_status == "healthy" else "degraded",
        version=settings.APP_VERSION,
        timestamp=datetime.utcnow(),
        database_status=db_status,
        kafka_status=kafka_status,
        uptime_seconds=uptime_seconds,
        system_load=system_load
    )