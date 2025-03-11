import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
from typing import List, Dict, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import update
from app.core.models.sales import ETLJob, ErrorLog
from app.config.settings import settings
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db

from contextlib import asynccontextmanager
from app.core.etl.load import DataLoader


logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles transformation of extracted data"""
    
    async def transform_batch(self, job_id: str, file_path: str, db: AsyncSession = Depends(get_db)) -> str:
        """
        Transform data from a batch extraction job
        Returns the job_id for tracking
        """
        # Update job status
        # async with get_db() as db:
        transform_job = ETLJob(
            job_id=f"{job_id}_transform",
            job_type="BATCH",
            status="RUNNING",
            source_file=file_path,
            start_time=datetime.utcnow()
        )
        db.add(transform_job)
        await db.commit()
        
        try:
            # Read the extracted data
            df = pd.read_excel(file_path, engine="openpyxl")
            
            # Process in chunks for large datasets
            chunk_size = settings.BATCH_SIZE
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            
            # Process chunks in parallel
            with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
                futures = [
                    executor.submit(self._transform_chunk, chunk, i, job_id)
                    for i, chunk in enumerate(chunks)
                ]
                
                results = []
                for future in futures:
                    results.append(future.result())
            
            # Combine results
            transformed_df = pd.concat(results)
            
            # Save transformed data
            output_path = os.path.join(settings.DATA_UPLOAD_FOLDER, f"{job_id}_transformed.xlsx")
            transformed_df.to_excel(output_path, index=False)
        
            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == f"{job_id}_transform")
                .values(
                    records_processed=len(transformed_df),
                    status="COMPLETED",
                    end_time=datetime.utcnow()
                )
            )

            # Execute the statement and commit the transaction
            await db.execute(stmt)
            await db.commit()

            logger.info(f"Transformation completed for job {job_id}: {len(transformed_df)} records transformed")
            
            # Trigger loading (would typically use Kafka here)
            loader = DataLoader()
            await loader.load_batch(job_id, output_path, db)
            
            return f"{job_id}_transform"
            
        except Exception as e:
            # Update job status
            # db.query(ETLJob).filter(ETLJob.job_id == f"{job_id}_transform").update({
            #     "status": "FAILED",
            #     "end_time": datetime.utcnow(),
            #     "error_message": str(e)
            # })
            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == f"{job_id}_transform")
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
                job_id=f"{job_id}_transform",
                error_type="TRANSFORMATION_ERROR",
                error_message=f"Failed to transform data: {str(e)}",
                severity="HIGH",
                error_details={"exception": str(e)}
            )
            db.add(error_log)
            db.commit()
        
            logger.error(f"Transformation failed for job {job_id}: {str(e)}")
            raise
    
    def _transform_chunk(self, chunk: pd.DataFrame, chunk_id: int, job_id: str) -> pd.DataFrame:
        """Transform a chunk of data"""
        try:
            # Make a copy to avoid modifying the original
            df = chunk.copy()
            
            # Data type conversions
            df['order_date'] = pd.to_datetime(df['order_date']).dt.date
            df['ship_date'] = pd.to_datetime(df['ship_date']).dt.date
            
            # Convert numeric columns
            numeric_cols = ['units_sold', 'unit_price', 'unit_cost', 'total_revenue', 'total_cost', 'total_profit']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Handle missing values
            df['region'] = df['region'].fillna('Unknown')
            df['country'] = df['country'].fillna('Unknown')
            df['item_type'] = df['item_type'].fillna('Unknown')
            df['sales_channel'] = df['sales_channel'].fillna('Unknown')
            df['order_priority'] = df['order_priority'].fillna('M')  # Default to Medium priority
            
            # Standardize text data
            df['region'] = df['region'].str.strip().str.title()
            df['country'] = df['country'].str.strip().str.title()
            df['item_type'] = df['item_type'].str.strip().str.title()
            df['sales_channel'] = df['sales_channel'].str.strip().str.title()
            df['order_priority'] = df['order_priority'].str.strip().str.upper()
            
            # Validate and standardize order priority
            valid_priorities = ['L', 'M', 'H', 'C']
            df['order_priority'] = df['order_priority'].apply(
                lambda x: x[0] if x and x[0] in valid_priorities else 'M'
            )
            
            # Recalculate derived fields
            df['total_revenue'] = df['units_sold'] * df['unit_price']
            df['total_cost'] = df['units_sold'] * df['unit_cost']
            df['total_profit'] = df['total_revenue'] - df['total_cost']
            
            # Remove duplicates based on order_id
            df = df.drop_duplicates(subset=['order_id'])
            
            # Add data quality flags
            df['data_quality_issues'] = False
            
            # Check for unreasonable values
            df.loc[df['units_sold'] <= 0, 'data_quality_issues'] = True
            df.loc[df['unit_price'] <= 0, 'data_quality_issues'] = True
            df.loc[df['unit_cost'] <= 0, 'data_quality_issues'] = True
            df.loc[df['ship_date'] < df['order_date'], 'data_quality_issues'] = True
            
            # Add transformation metadata
            df['etl_job_id'] = job_id
            df['etl_chunk_id'] = chunk_id
            df['etl_transformed_at'] = datetime.now().isoformat()

            # Log any rows with quality issues
            quality_issues = df[df['data_quality_issues'] == True]
            if len(quality_issues) > 0:
                with get_db() as db:
                    for _, row in quality_issues.iterrows():
                        error_log = ErrorLog(
                            job_id=job_id,
                            error_type="DATA_QUALITY_ISSUE",
                            error_message=f"Data quality issue in record with order_id {row['order_id']}",
                            severity="MEDIUM",
                            record_data=row.to_dict()
                        )
                        db.add(error_log)
                    db.commit()

            logger.info(f"Transformed chunk {chunk_id} for job {job_id}: {len(df)} records, {len(quality_issues)} with quality issues")
            
            return df
            
        except Exception as e:
            logger.error(f"Error transforming chunk {chunk_id} for job {job_id}: {str(e)}")
            with get_db() as db:
                error_log = ErrorLog(
                    job_id=job_id,
                    error_type="CHUNK_TRANSFORMATION_ERROR",
                    error_message=f"Failed to transform chunk {chunk_id}: {str(e)}",
                    severity="HIGH",
                    error_details={"exception": str(e), "chunk_id": chunk_id}
                )
                db.add(error_log)
                db.commit()
            
            # Return empty DataFrame to not block processing of other chunks
            return pd.DataFrame()