import pandas as pd
import os
import uuid
from datetime import datetime
import asyncio
import logging
from typing import Dict, List, Tuple, Any
import aiofiles
from fastapi import UploadFile
from sqlalchemy.orm import Session        

from ...db.database import get_db
from ...core.models.sales import ETLJob, ErrorLog
from ...config.settings import settings

logger = logging.getLogger(__name__)

class DataExtractor:
    """Handles extraction of data from various sources"""
    
    def extract_from_file(self, file: UploadFile, db: Session) -> str:
        """
        Extract data from uploaded Excel or CSV file
        Returns the job_id for tracking
        """
        # Create ETL job record
        job_id = str(uuid.uuid4())
        etl_job = ETLJob(
            job_id=job_id,
            job_type="BATCH",
            status="RUNNING",
            source_file=file.filename,
            start_time=datetime.utcnow()
        )
        db.add(etl_job)
        db.commit()
        
        try:
            # Ensure upload directory exists
            os.makedirs(settings.DATA_UPLOAD_FOLDER, exist_ok=True)
            
            # Save file to disk
            file_path = os.path.join(settings.DATA_UPLOAD_FOLDER, f"{job_id}_{file.filename}")
            with aiofiles.open(file_path, 'wb') as out_file:
                content = file.read()
                out_file.write(content)
            
            # Schedule extraction task
            asyncio.create_task(self._process_file(file_path, job_id))
            
            return job_id
        
        except Exception as e:
            # Update job status to failed
            db.query(ETLJob).filter(ETLJob.job_id == job_id).update({
                "status": "FAILED",
                "end_time": datetime.utcnow(),
                "error_message": str(e)
            })
            db.commit()
            
            # Log error
            error_log = ErrorLog(
                job_id=job_id,
                error_type="EXTRACTION_ERROR",
                error_message=f"Failed to extract data: {str(e)}",
                severity="HIGH",
                error_details={"exception": str(e)}
            )
            db.add(error_log)
            db.commit()
            
            logger.error(f"Extraction failed for job {job_id}: {str(e)}")
            raise
    
    def _process_file(self, file_path: str, job_id: str) -> None:
        """Process the extracted file in the background"""
        try:
            # Determine file type and read
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
            
            # Basic validation
            required_columns = [
                "Region", "Country", "Item Type", "Sales Channel", 
                "Order Priority", "Order Date", "Order ID", "Ship Date",
                "Units Sold", "Unit Price", "Unit Cost", "Total Revenue",
                "Total Cost", "Total Profit"
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Clean column names (remove spaces, lowercase)
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            # Convert to records
            records = df.to_dict('records')
            
            # Save to temporary storage for transformer
            temp_path = os.path.join(settings.DATA_UPLOAD_FOLDER, f"{job_id}_processed.csv")
            df.to_csv(temp_path, index=False)
            
            # Update job status
            with get_db() as db:
                db.query(ETLJob).filter(ETLJob.job_id == job_id).update({
                    "records_processed": len(records),
                    "status": "COMPLETED",
                    "end_time": datetime.utcnow()
                })
                db.commit()
            
            logger.info(f"Extraction completed for job {job_id}: {len(records)} records extracted")
            
            # Trigger transformation (would typically use Kafka here)
            transformer = DataTransformer()
            transformer.transform_batch(job_id, temp_path)
            
        except Exception as e:
            with get_db() as db:
                # Update job status
                db.query(ETLJob).filter(ETLJob.job_id == job_id).update({
                    "status": "FAILED",
                    "end_time": datetime.utcnow(),
                    "error_message": str(e)
                })
                
                # Log error
                error_log = ErrorLog(
                    job_id=job_id,
                    error_type="EXTRACTION_PROCESSING_ERROR",
                    error_message=f"Failed to process extracted data: {str(e)}",
                    severity="HIGH",
                    error_details={"exception": str(e)}
                )
                db.add(error_log)
                db.commit()
            
            logger.error(f"Processing failed for job {job_id}: {str(e)}")
            raise
        finally:
            # Clean up temporary files
            
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    db.close()
            except Exception as e:
                logger.warning(f"Failed to clean up file {file_path}: {str(e)}")
            