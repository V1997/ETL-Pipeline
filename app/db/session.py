from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
import os
from dotenv import load_dotenv
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get database connection details from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_password")
DB_NAME = os.getenv("DB_NAME", "sales_etl")

# Construct database URL
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "").lower() == "true",  # Set to True to log SQL queries
    pool_pre_ping=True,  # Verify connections before usage
    pool_recycle=3600,   # Recycle connections after 1 hour
    pool_size=10,        # Connection pool size
    max_overflow=20      # Max additional connections
)

# Create a thread-local session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
ScopedSession = scoped_session(SessionLocal)

# Base class for models
Base = declarative_base()

@contextmanager
def get_db_session():
    """Provide a transactional scope around a series of operations."""
    session = ScopedSession()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database transaction error: {str(e)}")
        raise
    finally:
        session.close()
        ScopedSession.remove()

def get_session():
    """Get a database session for use with FastAPI dependencies."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        
def check_database_connection() -> bool:
    """
    Check if database connection works
    
    Returns:
        bool: True if connection is working
    """
    try:
        # Execute a simple query
        with get_db_session() as session:
            session.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database connection check failed: {str(e)}")
        return False