from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.config.settings import settings
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.api.routes import router as api_router
from app.db.database import initialize_db_connection, close_db_connection, get_db

from app.kafka.consumer import KafkaConsumerService
import logging
logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI
from contextlib import asynccontextmanager

# Assuming the logging and services are already properly set up
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_consumer = None
    try:
        # Initialize database connection
        try:
            await initialize_db_connection()
            logging.info("Database ------ Connected")
        except Exception as e:
            logging.error(f"Database ------ failed: {str(e)}")
        
        # Kafka Consumer Initialization
        try:
            kafka_consumer = KafkaConsumerService()
            await kafka_consumer.start()  # If start() is async
            logging.info("STARTED ------ Kafka Consumer")
        except AttributeError:
            # Handling for non-async start() method (if KafkaConsumerService.start() is sync)
            if kafka_consumer:
                from asyncio import loop
                loop.run_in_executor(None, kafka_consumer.start)  # Run sync in background thread
                logging.info("STARTED ------ Kafka Consumer in background thread")
        except Exception as e:
            logging.error(f"Failed to start Kafka consumer: {str(e)}")

        # Keep the app active while resources are running
        yield

    finally:
        # Stopping Kafka consumer (ensure stop method exists)
        if kafka_consumer:
            try:
                await kafka_consumer.stop()  # If stop is async
                logging.info("Stopped Kafka consumer")
            except Exception as e:
                logging.error(f"Error stopping Kafka consumer: {str(e)}")
        
        # Close database connection
        try:
            await close_db_connection()
            logging.info("Database closed successfully.")
        except Exception as e:
            logging.error(f"Failed to close database connection: {str(e)}")

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

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
app.include_router(api_router, prefix=settings.API_PREFIX)

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
# import uvicorn
# from fastapi import FastAPI, Depends, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from contextlib import asynccontextmanager
# import logging
# import os
# from app.config.settings import settings  # Import your settings object
# from app.api.routes import router as api_router
# from app.config.settings import settings
# from app.db.database import get_db
# from app.kafka.consumer import KafkaConsumerService

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler(),
#         logging.FileHandler("etl_app.log"),
#     ]
# )

# logger = logging.getLogger(__name__)

# # Lifespan manager for startup/shutdown tasks
# @asynccontextmanager
# def lifespan(app: FastAPI):
#     """
#     Manage the lifecycle of the application
#     """
#     # Startup: Initialize Kafka consumer
#     kafka_consumer = None
#     try:
#         kafka_consumer = KafkaConsumerService()
#         kafka_consumer.start()
#         logger.info("Started Kafka consumer for sales data stream")
#     except Exception as e:
#         logger.error(f"Failed to start Kafka consumer: {str(e)}")
    
#     try:
#         db.execute("SELECT 1")
#         logger.info("Databse is connected")
#     except Exception as e:
#         logger.error(f"Failed to Connect to Database: {str(e)}")

#     # Run the application
#     yield
    
#     # Shutdown: Clean up resources
#     if kafka_consumer:
#         try:
#             kafka_consumer.stop()
#             logger.info("Stopped Kafka consumer")
#         except Exception as e:
#             logger.error(f"Error stopping Kafka consumer: {str(e)}")

# # Create FastAPI app
# app = FastAPI(
#     title=settings.APP_NAME,
#     description="ETL pipeline for sales data processing",
#     version=settings.APP_VERSION,
#     lifespan=lifespan
# )

# # Enable CORS
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # In production, restrict to your frontend domain
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# # Include API router
# app.include_router(api_router, prefix=settings.API_PREFIX)

# # Root endpoint
# @app.get("/")
# def root():
#     return {
#         "message": "Sales ETL Pipeline API",
#         "version": settings.APP_VERSION,
#         "docs_url": f"{settings.API_PREFIX}/docs"
#     }

# if __name__ == "__main__":
#     uvicorn.run(
#         "app.main:app",
#         host="0.0.0.0",
#         port=8000,
#         reload=settings.DEBUG
#     )