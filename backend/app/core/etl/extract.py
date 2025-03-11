import pandas as pd
import os
import uuid
from datetime import datetime
import asyncio
import logging
from typing import Dict, List, Tuple, Any
import aiofiles
from fastapi import UploadFile, File, Depends
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.core.models.sales import ETLJob, ErrorLog
from app.config.settings import settings
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from app.core.etl.transform import DataTransformer

logger = logging.getLogger(__name__)

class DataExtractor:
    """Handles extraction of data from various sources"""
    
    async def extract_from_file(self, file, job_id, file_path, db: AsyncSession = Depends(get_db)) -> str:
        """
        Extract data from uploaded Excel or CSV file
        Returns the job_id for tracking
        """

        # Create ETL job record
        # job_id = str(uuid.uuid4())[:8]
        etl_job = ETLJob(
            job_id=job_id,
            job_type="BATCH",
            status="RUNNING",
            source_file=file.filename,
            start_time=datetime.utcnow()
        )
        db.add(etl_job)
        await db.commit()
        
        try:

            # Schedule extraction task
            asyncio.create_task(self._process_file(file_path, job_id, db))
            return job_id
        
        except Exception as e:

            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == job_id)
                .values(
                    status="FAILED",
                    end_time=datetime.utcnow(),
                    error_message=str(e)
                )
            )
            await db.execute(stmt)
            await db.commit()
            
            # Log error
            error_log = ErrorLog(
                job_id=job_id,
                error_type="EXTRACTION_ERROR",
                error_message=f"Failed to extract data: {str(e)}",
                severity="HIGH",
                error_details={"exception": str(e)}
            )
            db.add(error_log)
            await db.commit()
            
            logger.error(f"Extraction failed for job {job_id}: {str(e)}")
            raise
    
    async def _process_file(self, file_path: str, job_id: str, db: AsyncSession = Depends(get_db)) -> None:
        """Process the extracted file in the background"""
        try:
            print("------------------", file_path)
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, engine="openpyxl")
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
            temp_path = os.path.join(settings.DATA_UPLOAD_FOLDER, f"{job_id}_processed.xlsx")
            df.to_excel(temp_path, index=False)
                        
            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == job_id)
                .values(
                    records_processed=len(records),
                    status="COMPLETED",
                    end_time=datetime.utcnow()
                )
            )

            # Execute the update query
            await db.execute(stmt)
            await db.commit()  # Commit the changes
            
            logger.info(f"Extraction completed for job {job_id}: {len(records)} records extracted")
            
            # Trigger transformation (would typically use Kafka here)
            transformer = DataTransformer()
            await transformer.transform_batch(job_id, temp_path, db)
            
        except Exception as e:
            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == job_id)
                .values(
                    status="FAILED",
                    end_time=datetime.utcnow(),
                    error_message=str(e)
                )
            )

            # Execute the update query
            await db.execute(stmt)
            await db.commit()  # Commit the transaction to apply changes

            # Log error
            error_log = ErrorLog(
                job_id=job_id,
                error_type="EXTRACTION_PROCESSING_ERROR",
                error_message=f"Failed to process extracted data: {str(e)}",
                severity="HIGH",
                error_details={"exception": str(e)}
            )
            db.add(error_log)
            await db.commit()
            
            logger.error(f"Processing failed for job {job_id}: {str(e)}")
            raise
        finally:
            # Clean up temporary files
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                logger.warning(f"Failed to clean up file {file_path}: {str(e)}")