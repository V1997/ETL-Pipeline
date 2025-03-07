"""
Utilities for enhancing API documentation
"""

from fastapi.openapi.utils import get_openapi
from app.api.main import app


def custom_openapi():
    """
    Customize the OpenAPI documentation
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="ETL Pipeline API",
        version="1.0.0",
        description="""
        # ETL Pipeline API Documentation
        
        This API provides endpoints for managing ETL batch processing operations.
        
        ## Features
        
        - **Batch Management**: Create, monitor, and manage ETL batches
        - **Record Access**: Query and export processed records
        - **Metrics & Analytics**: Access processing metrics and quality statistics
        - **User Management**: Authentication and role-based access control
        
        ## Authentication
        
        All endpoints except health checks require authentication using JWT tokens.
        
        To authenticate:
        1. Use the `/api/users/login` endpoint to obtain a token
        2. Include the token in the Authorization header: `Authorization: Bearer {token}`
        
        ## Roles and Permissions
        
        The API uses role-based access control with these roles:
        
        - **admin**: Full access to all endpoints, including user management
        - **operator**: Can create, monitor, and manage batches
        - **viewer**: Read-only access to batches, records, and metrics
        
        ## Database
        
        This API uses MySQL as its backend database.
        """,
        routes=app.routes,
    )
    
    # Add security scheme to OpenAPI schema
    openapi_schema["components"]["securitySchemes"] = {
        "bearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
    }
    
    # Add global security requirement
    openapi_schema["security"] = [{"bearerAuth": []}]
    
    # Customize tags
    openapi_schema["tags"] = [
        {
            "name": "Batches",
            "description": "Operations for creating and managing ETL batches"
        },
        {
            "name": "Records",
            "description": "Access and export processed sales records"
        },
        {
            "name": "Metrics",
            "description": "Access ETL metrics and analytics"
        },
        {
            "name": "Users",
            "description": "User management and authentication"
        },
        {
            "name": "Health",
            "description": "Health check endpoints for monitoring"
        },
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema


# Apply custom OpenAPI schema
app.openapi = custom_openapi