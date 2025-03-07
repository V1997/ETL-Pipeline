"""
Request logging middleware for the API

Provides request/response logging for monitoring and debugging.
"""

import time
import uuid
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

# Configure logging
logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for logging requests and responses
    """
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next):
        """
        Log request and response information
        
        Args:
            request: Incoming request
            call_next: Next middleware in the chain
        
        Returns:
            Response from downstream middleware
        """
        # Generate unique request ID
        request_id = str(uuid.uuid4())
        
        # Extract client information
        client_host = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        # Start timing the request
        start_time = time.time()
        
        # Log the request
        logger.info(
            f"Request {request_id} started: {request.method} {request.url.path} "
            f"from {client_host} - User-Agent: {user_agent}"
        )
        
        # Process the request
        try:
            response = await call_next(request)
            
            # Calculate request duration
            process_time = time.time() - start_time
            
            # Log the response
            logger.info(
                f"Request {request_id} completed: {response.status_code} "
                f"in {process_time:.3f}s"
            )
            
            # Add request ID to response headers
            response.headers["X-Request-ID"] = request_id
            
            return response
        except Exception as e:
            # Calculate request duration even for errors
            process_time = time.time() - start_time
            
            # Log the error
            logger.error(
                f"Request {request_id} failed: {str(e)} in {process_time:.3f}s"
            )
            
            # Re-raise the exception
            raise