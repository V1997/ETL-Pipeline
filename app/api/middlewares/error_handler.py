"""
Error handling middleware for the API

Provides centralized error handling and consistent error responses.
"""

from fastapi import Request, status, FastAPI
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import logging
from datetime import datetime
import traceback
import uuid

# Configure logging
logger = logging.getLogger(__name__)


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Handle validation errors in a user-friendly way
    """
    error_id = str(uuid.uuid4())
    
    # Format validation errors into a clean structure
    errors = []
    for error in exc.errors():
        error_obj = {
            "location": error["loc"],
            "message": error["msg"],
            "type": error["type"]
        }
        errors.append(error_obj)
    
    # Log the validation error
    logger.warning(
        f"Validation error {error_id}: {str(exc)} - URL: {request.url} - Errors: {errors}"
    )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error_id": error_id,
            "timestamp": "2025-03-06 05:21:54",
            "status": 422,
            "type": "validation_error",
            "message": "Request validation failed",
            "errors": errors,
            "path": str(request.url)
        }
    )


async def http_exception_handler(request: Request, exc):
    """
    Handle HTTP exceptions with consistent response format
    """
    error_id = str(uuid.uuid4())
    
    # Log the error with different levels based on status code
    if exc.status_code >= 500:
        logger.error(
            f"HTTP error {error_id}: {exc.status_code} {exc.detail} - URL: {request.url}"
        )
    elif exc.status_code >= 400:
        logger.warning(
            f"HTTP error {error_id}: {exc.status_code} {exc.detail} - URL: {request.url}"
        )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error_id": error_id,
            "timestamp": "2025-03-06 05:21:54",
            "status": exc.status_code,
            "type": "http_error",
            "message": exc.detail,
            "path": str(request.url)
        },
        headers=getattr(exc, "headers", None)
    )


async def general_exception_handler(request: Request, exc: Exception):
    """
    Catch-all exception handler for unhandled exceptions
    """
    error_id = str(uuid.uuid4())
    
    # Log the full exception
    logger.error(
        f"Unhandled exception {error_id}: {str(exc)} - URL: {request.url}"
    )
    logger.error(traceback.format_exc())
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error_id": error_id,
            "timestamp": "2025-03-06 05:21:54",
            "status": 500,
            "type": "server_error",
            "message": "An unexpected error occurred",
            "path": str(request.url)
        }
    )


def add_exception_handlers(app: FastAPI):
    """
    Add all exception handlers to the FastAPI app
    
    Args:
        app: FastAPI application
    """
    from fastapi.exceptions import RequestValidationError
    from starlette.exceptions import HTTPException
    
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)