import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os

from .api.routes import router as api_router
from .config.settings import settings
from .db.database import get_db
from .kafka.consumer import KafkaConsumerService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_app.log"),
    ]
)

logger = logging.getLogger(__name__)

# Lifespan manager for startup/shutdown tasks
@asynccontextmanager
def lifespan(app: FastAPI):
    """
    Manage the lifecycle of the application
    """
    # Startup: Initialize Kafka consumer
    kafka_consumer = None
    try:
        kafka_consumer = KafkaConsumerService()
        kafka_consumer.start()
        logger.info("Started Kafka consumer for sales data stream")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {str(e)}")
    
    # Run the application
    yield
    
    # Shutdown: Clean up resources
    if kafka_consumer:
        try:
            kafka_consumer.stop()
            logger.info("Stopped Kafka consumer")
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer: {str(e)}")

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="ETL pipeline for sales data processing",
    version=settings.APP_VERSION,
    lifespan=lifespan
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
# app.include_router(api_router, prefix=settings.API_PREFIX)
app.include_router(api_router)

# Root endpoint
@app.get("/")
def root():
    return {
        "message": "Sales ETL Pipeline API",
        "version": settings.APP_VERSION,
        "docs_url": f"{settings.API_PREFIX}/docs"
    }

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )