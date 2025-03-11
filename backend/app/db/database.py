from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from contextlib import asynccontextmanager
from app.config.settings import settings
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
import logging

# Configure Logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# SQLAlchemy Database URL Configuration (Ensure settings is correctly imported)
SQLALCHEMY_DATABASE_URL = f"mysql+asyncmy://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

# Create the asynchronous database engine
async_engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=10,
    max_overflow=20,
    echo=False  # Remove 'future=True' (deprecated)
)

# Create Async Session Factory
AsyncSessionLocal = sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)

# Base class for ORM models
class Base(DeclarativeBase):
    pass

# Dependency to get the database session
async def get_db() -> AsyncSession:
    session = AsyncSessionLocal()
    try:
        yield session
        await session.commit()  # Commit changes if no errors occur
    except SQLAlchemyError as e:
        await session.rollback()  # Rollback on error
        logger.error(f"Database error: {e}")  # Proper logging instead of print
    finally:
        await session.close()  # Ensure session is always closed

 
async def initialize_db_connection():
    """
    Initializes the database connection by creating tables or performing setup tasks.
    This method can also be used to test the connection at startup.
    """
    async with async_engine.begin() as conn:
        print("Initializing database connection...")
        # Automatically create tables based on ORM models (use migrations for production)
        await conn.run_sync(Base.metadata.create_all)
    print("Database initialized successfully.")

async def close_db_connection():
    """
    Cleans up and disposes of the database engine.
    This is typically used during application shutdown.
    """
    await async_engine.dispose()
    print("Database connection closed.")