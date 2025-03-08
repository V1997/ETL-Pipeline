from fastapi import APIRouter, Depends, HTTPException, File, UploadFile
from sqlalchemy.orm import Session
from typing import List, Optional
from sqlalchemy import func
from ..db.database import get_db
from ..core.models.sales import ETLJob, SalesRecord, SalesAnalytics, ErrorLog
from ..core.schemas.sales import (
    SalesRecordCreate, SalesRecordInDB, ETLJobInDB, 
    BatchUploadResponse, HealthCheckResponse
)
from ..core.etl.extract import DataExtractor
from ..kafka.producer import KafkaProducerService
from ..config.settings import settings
from datetime import datetime
router = APIRouter()

# ETL Job routes
@router.post("/etl/batch-upload", response_model=BatchUploadResponse, tags=["ETL"])
def upload_batch_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Upload a batch file (Excel or CSV)
    
    Starts an ETL job to process the file in the background
    """
    # Validate file type
    if not file.filename.endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Invalid file format. Only CSV and Excel files are supported."
        )
    
    try:
        # Start extraction process
        extractor = DataExtractor()
        job_id = extractor.extract_from_file(file, db)
        
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
def get_etl_jobs(
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get ETL job history with optional filtering
    """
    query = db.query(ETLJob)
    
    if status:
        query = query.filter(ETLJob.status == status)
    
    if job_type:
        query = query.filter(ETLJob.job_type == job_type)
    
    jobs = query.order_by(ETLJob.start_time.desc()).offset(skip).limit(limit).all()
    return jobs

@router.get("/etl/jobs/{job_id}", response_model=ETLJobInDB, tags=["ETL"])
def get_etl_job(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific ETL job
    """
    job = db.query(ETLJob).filter(ETLJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")
    return job

@router.get("/etl/errors", response_model=List[dict], tags=["ETL"])
def get_etl_errors(
    job_id: Optional[str] = None,
    severity: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get ETL error logs with optional filtering
    """
    query = db.query(ErrorLog)
    
    if job_id:
        query = query.filter(ErrorLog.job_id == job_id)
    
    if severity:
        query = query.filter(ErrorLog.severity == severity)
    
    errors = query.order_by(ErrorLog.created_at.desc()).offset(skip).limit(limit).all()
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
def get_sales_records(
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
    query = db.query(SalesRecord)
    
    if region:
        query = query.filter(SalesRecord.region == region)
    
    if country:
        query = query.filter(SalesRecord.country == country)
    
    if item_type:
        query = query.filter(SalesRecord.item_type == item_type)
    
    if sales_channel:
        query = query.filter(SalesRecord.sales_channel == sales_channel)
    
    if start_date:
        query = query.filter(SalesRecord.order_date >= start_date)
    
    if end_date:
        query = query.filter(SalesRecord.order_date <= end_date)
    
    records = query.order_by(SalesRecord.order_date.desc()).offset(skip).limit(limit).all()
    return records

@router.get("/sales/overview", response_model=dict, tags=["Sales"])
def get_sales_overview(db: Session = Depends(get_db)):
    """
    Get overview of sales data with key metrics
    """
    # Total records count
    total_records = db.query(SalesRecord).count()

    # Recent record timestamp
    latest_record = db.query(SalesRecord.updated_at).order_by(SalesRecord.updated_at.desc()).first()
    latest_timestamp = latest_record[0] if latest_record else None
    
    # Total revenue, cost, profit
    totals = db.query(
        func.sum(SalesRecord.total_revenue).label("total_revenue"),
        func.sum(SalesRecord.total_cost).label("total_cost"),
        func.sum(SalesRecord.total_profit).label("total_profit"),
        func.sum(SalesRecord.units_sold).label("total_units")
    ).first()
    
    # Top regions
    top_regions = db.query(
        SalesRecord.region,
        func.sum(SalesRecord.total_profit).label("total_profit")
    ).group_by(SalesRecord.region).order_by(func.sum(SalesRecord.total_profit).desc()).limit(5).all()
    
    # Top countries
    top_countries = db.query(
        SalesRecord.country,
        func.sum(SalesRecord.total_profit).label("total_profit")
    ).group_by(SalesRecord.country).order_by(func.sum(SalesRecord.total_profit).desc()).limit(5).all()
    
    # Top item types
    top_item_types = db.query(
        SalesRecord.item_type,
        func.sum(SalesRecord.total_profit).label("total_profit")
    ).group_by(SalesRecord.item_type).order_by(func.sum(SalesRecord.total_profit).desc()).limit(5).all()
    
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
        ]
    }

# Health and Status Routes
@router.get("/health", response_model=HealthCheckResponse, tags=["System"])
def health_check(db: Session = Depends(get_db)):
    """
    Check the health status of the ETL system
    """
    from ..kafka.producer import KafkaProducerService
    import psutil
    import time
    
    # Check database connectivity
    db_status = "healthy"
    try:
        # Try simple query
        db.execute("SELECT 1").fetchone()
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    # Check Kafka connectivity
    kafka_status = "healthy"
    try:
        producer = KafkaProducerService()
        # Simple connectivity check is performed in constructor
    except Exception as e:
        kafka_status = f"unhealthy: {str(e)}"
    
    # Get uptime
    import os
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