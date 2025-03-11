import os
from pydantic_settings import BaseSettings
from functools import lru_cache
from dotenv import load_dotenv
load_dotenv()

class Settings(BaseSettings):
    # Application Settings
    APP_NAME: str = "Sales ETL Pipeline"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("DEBUG", False)
    
    # Database Settings
    DB_HOST: str = os.getenv("DB_HOST", "local")
    DB_PORT: int = os.getenv("DB_PORT", 3306)
    DB_USER: str = os.getenv("DB_USER", "root")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "root")
    DB_NAME: str = os.getenv("DB_NAME", "sales_etl")
    
    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_SALES: str = os.getenv("KAFKA_TOPIC_SALES", "sales-records")
    KAFKA_TOPIC_ERRORS: str = os.getenv("KAFKA_TOPIC_ERRORS", "etl-errors")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "sales-etl-group")
    
    # ETL Settings
    BATCH_SIZE: int = os.getenv("BATCH_SIZE", 1000)
    MAX_WORKERS: int = os.getenv("MAX_WORKERS", 4)
    DATA_UPLOAD_FOLDER: str = os.getenv("DATA_UPLOAD_FOLDER", "")

    # API Settings
    API_PREFIX: str = "/api/v1"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()