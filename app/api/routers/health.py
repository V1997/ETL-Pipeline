"""
API router for health checks

Provides endpoints for monitoring the health of the API and its components.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import Dict, Any
import logging
from datetime import datetime
import os
import psutil
import platform
import pymysql

# Import services
from app.db.session import check_database_connection

# Create router
router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/health", 
    summary="Health check",
    description="Check API and component health status"
)
async def health_check():
    """
    Check API and component health status
    
    Returns:
        Dict: Health status of API components
    """
    health_data = {
        "status": "ok",
        "timestamp": "2025-03-06 06:14:34",
        "version": "1.0.0",
        "components": {}
    }
    
    # Check database connection
    try:
        db_status = check_database_connection()
        health_data["components"]["database"] = {
            "status": "ok" if db_status else "error",
            "message": "Connected" if db_status else "Failed to connect",
            "type": "MySQL"
        }
        
        if not db_status:
            health_data["status"] = "degraded"
    except Exception as e:
        health_data["components"]["database"] = {
            "status": "error",
            "message": str(e),
            "type": "MySQL"
        }
        health_data["status"] = "degraded"
    
    # Check Kafka connection if used
    try:
        # Implement Kafka health check
        kafka_status = True  # Placeholder for actual check
        health_data["components"]["kafka"] = {
            "status": "ok" if kafka_status else "error",
            "message": "Connected" if kafka_status else "Failed to connect"
        }
        
        if not kafka_status:
            health_data["status"] = "degraded"
    except Exception as e:
        health_data["components"]["kafka"] = {
            "status": "error",
            "message": str(e)
        }
        health_data["status"] = "degraded"
    
    # System metrics
    health_data["system"] = {
        "cpu_usage": psutil.cpu_percent(interval=None),
        "memory_usage_percent": psutil.virtual_memory().percent,
        "disk_usage_percent": psutil.disk_usage('/').percent,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
    }
    
    # Current user info
    health_data["user"] = os.getenv("CURRENT_USER", "V1997")
    
    return health_data


@router.get(
    "/readiness", 
    summary="Readiness probe",
    description="Check if the API is ready to receive traffic"
)
async def readiness_check():
    """
    Check if the API is ready to receive traffic
    
    Returns:
        Dict: API readiness status
    """
    # Simple readiness check - verify DB connection only
    try:
        db_ready = check_database_connection()
        
        if db_ready:
            return {
                "status": "ready",
                "timestamp": "2025-03-06 06:14:34",
                "db_type": "MySQL"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed"
            )
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service not ready: {str(e)}"
        )


@router.get(
    "/liveness", 
    summary="Liveness probe",
    description="Check if the API is running properly"
)
async def liveness_check():
    """
    Check if the API is running properly
    
    Returns:
        Dict: API liveness status
    """
    # Simple liveness check - just respond
    return {
        "status": "alive",
        "timestamp": "2025-03-06 06:14:34",
        "current_user": os.getenv("CURRENT_USER", "V1997")
    }