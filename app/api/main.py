"""
ETL Pipeline API - Main Application

This module serves as the entry point for the ETL Pipeline API,
configuring the FastAPI application with all routes, middleware,
and exception handlers.

Version: 1.0.0
"""

from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import List, Optional, Dict, Any
import logging
import time
import uuid
import os
from datetime import datetime, timedelta

# Import API routers
from app.api.routers import batches, records, metrics, users, health

# Import authentication and security
from app.api.auth import auth_handler
from app.api.middlewares.logging_middleware import RequestLoggingMiddleware
from app.api.middlewares.error_handler import add_exception_handlers

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("api")

# Get current user from environment
CURRENT_USER = os.getenv("CURRENT_USER", "V1997")
CURRENT_TIME = "2025-03-06 06:17:01"  # Current UTC time

# Create FastAPI app
app = FastAPI(
    title="ETL Pipeline API",
    description="API for managing ETL batch processing operations",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# Configure CORS
origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request logging middleware
app.add_middleware(RequestLoggingMiddleware)

# Add exception handlers
add_exception_handlers(app)

# Include routers
app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(batches.router, prefix="/api/batches", tags=["Batches"])
app.include_router(records.router, prefix="/api/records", tags=["Records"])
app.include_router(metrics.router, prefix="/api/metrics", tags=["Metrics"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time header and additional context to responses"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    response.headers["X-Request-ID"] = str(uuid.uuid4())
    response.headers["X-Current-User"] = CURRENT_USER
    return response


@app.get("/api", tags=["Root"])
async def root():
    """API root endpoint"""
    return {
        "name": "ETL Pipeline API",
        "version": "1.0.0",
        "timestamp": CURRENT_TIME,
        "current_user": CURRENT_USER,
        "database_type": "MySQL"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)