from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from confluent_kafka import Producer
from ..db.database import get_db
from ..core.models.sales import SalesRecord
from ..config.settings import settings


router = APIRouter()

@router.get("/health", tags=["System"])
def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint to verify the service is running and can connect to the database and Kafka.
    """
    try:
        # Check database connection
        db.execute("SELECT 1")

        # Check Kafka connection
        producer = Producer({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS})
        producer.produce(settings.KAFKA_TOPIC_SALES, value='health_check')
        producer.flush()

        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "details": str(e)}