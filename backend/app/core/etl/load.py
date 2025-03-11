import pandas as pd
import os
import logging
from datetime import datetime
from typing import List, Dict, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import json
from fastapi import Depends

from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.dialects.mysql import insert
from sqlalchemy import text, update

from app.db.database import get_db, async_engine
from app.core.models.sales import ETLJob, ErrorLog, SalesRecord, SalesAnalytics
from app.config.settings import settings

logger = logging.getLogger(__name__)

class DataLoader:
    """Handles loading transformed data into the database"""
    
    async def load_batch(self, job_id: str, file_path: str, db: AsyncSession = Depends(get_db)) -> str:
        """
        Load transformed data into the database
        Returns the job_id for tracking
        """
        # Create loading job
        load_job = ETLJob(
            job_id=f"{job_id}_load",
            job_type="BATCH",
            status="RUNNING",
            source_file=file_path,
            start_time=datetime.utcnow()
        )
        db.add(load_job)
        await db.commit()
        
        try:
            # Read the transformed data
            df = pd.read_excel(file_path)
            
            # Process in chunks for large datasets
            chunk_size = settings.BATCH_SIZE
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            
            # Process chunks in parallel
            with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
                # futures = [
                #     executor.submit(self._load_chunk, chunk, i, job_id, f"{job_id}_load", db)
                #     for i, chunk in enumerate(chunks)
                # ]
                
                # Gather results
                records_processed = 0
                records_failed = 0

                for chunk_id, chunk in enumerate(chunks):
                    try:
                        # Await the coroutine to get the result
                        result = await self._load_chunk(chunk, chunk_id, job_id, f"{job_id}_load", db)
                        records_processed += result["processed"]
                        records_failed += result["failed"]
                    except Exception as e:
                        logger.error(f"Error loading chunk {chunk_id}: {str(e)}")

                # for future in futures:
                #     result = future.result()
                #     records_processed += result["processed"]
                #     records_failed += result["failed"]

                stmt = (
                    update(ETLJob)
                    .where(ETLJob.job_id == f"{job_id}_load")
                    .values(
                        records_processed=records_processed,
                        records_failed=records_failed,
                        status="COMPLETED",
                        end_time=datetime.utcnow()
                    )
                )

                # Execute the update and commit the changes
                await db.execute(stmt)
                await db.commit()
            logger.info(f"Loading completed for job {job_id}: {records_processed} records loaded, {records_failed} failed")
            
            # Generate analytics after loading
            await self._generate_analytics(job_id, db)
            
            return f"{job_id}_load"
            
        except Exception as e:

            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == f"{job_id}_load")
                .values(
                    status="FAILED",
                    end_time=datetime.utcnow(),
                    error_message=str(e)
                )
            )

            # Execute the update and commit the changes
            db.execute(stmt)
            db.commit()

            # Log error
            error_log = ErrorLog(
                job_id=f"{job_id}_load",
                error_type="LOADING_ERROR",
                error_message=f"Failed to load data: {str(e)}",
                severity="HIGH",
                error_details={"exception": str(e)}
            )
            db.add(error_log)
            db.commit()
            
            logger.error(f"Loading failed for job {job_id}: {str(e)}")
            raise
    
    async def _load_chunk(self, chunk: pd.DataFrame, chunk_id: int, original_job_id: str, load_job_id: str, db: AsyncSession) -> Dict[str, int]:
        """Load a chunk of data into the database asynchronously."""
        processed = 0
        failed = 0

        try:
            # Convert DataFrame to a list of dictionaries
            records = []
            for _, row in chunk.iterrows():
                try:
                    # Prepare record dictionary
                    record = {
                        'order_id': row['order_id'],
                        'region': row['region'],
                        'country': row['country'],
                        'item_type': row['item_type'],
                        'sales_channel': row['sales_channel'],
                        'order_priority': row['order_priority'],
                        'order_date': row['order_date'],
                        'ship_date': row['ship_date'],
                        'units_sold': int(row['units_sold']),
                        'unit_price': float(row['unit_price']),
                        'unit_cost': float(row['unit_cost']),
                        'total_revenue': float(row['total_revenue']),
                        'total_cost': float(row['total_cost']),
                        'total_profit': float(row['total_profit'])
                    }
                    records.append(record)
                    processed += 1
                except Exception as e:
                    failed += 1
                    # Log errors for individual records
                    error_log = ErrorLog(
                        job_id=load_job_id,
                        error_type="RECORD_LOADING_ERROR",
                        error_message=f"Failed to load record: {str(e)}",
                        severity="MEDIUM",
                        record_data=row.to_dict()
                    )
                    db.add(error_log)
                    await db.commit()  # Await commit for each error log

            # Use upsert operation to handle duplicates
            async with async_engine.connect() as connection:
                # Prepare the upsert statement
                insert_stmt = insert(SalesRecord.__table__).values(records)
                upsert_stmt = insert_stmt.on_duplicate_key_update({
                    'region': insert_stmt.inserted.region,
                    'country': insert_stmt.inserted.country,
                    'item_type': insert_stmt.inserted.item_type,
                    'sales_channel': insert_stmt.inserted.sales_channel,
                    'order_priority': insert_stmt.inserted.order_priority,
                    'order_date': insert_stmt.inserted.order_date,
                    'ship_date': insert_stmt.inserted.ship_date,
                    'units_sold': insert_stmt.inserted.units_sold,
                    'unit_price': insert_stmt.inserted.unit_price,
                    'unit_cost': insert_stmt.inserted.unit_cost,
                    'total_revenue': insert_stmt.inserted.total_revenue,
                    'total_cost': insert_stmt.inserted.total_cost,
                    'total_profit': insert_stmt.inserted.total_profit,
                    'updated_at': text('CURRENT_TIMESTAMP')
                })
                
                await connection.execute(upsert_stmt)  # Await the upsert execution
                await connection.commit()  # Commit the transaction

            logger.info(f"Loaded chunk {chunk_id} for job {original_job_id}: {processed} records processed, {failed} failed")
            return {"processed": processed, "failed": failed}

        except SQLAlchemyError as e:
            logger.error(f"Error loading chunk {chunk_id} for job {original_job_id}: {str(e)}")
            # Log the chunk loading error
            error_log = ErrorLog(
                job_id=load_job_id,
                error_type="CHUNK_LOADING_ERROR",
                error_message=f"Failed to load chunk {chunk_id}: {str(e)}",
                severity="HIGH",
                error_details={"exception": str(e), "chunk_id": chunk_id}
            )
            db.add(error_log)
            await db.commit()  # Await commit for logging errors
            return {"processed": 0, "failed": len(chunk)}    

    async def _generate_analytics(self, job_id: str, db: AsyncSession = Depends(get_db)) -> None:
        """Generate analytics data after loading"""
        try:
            analytics_job_id = f"{job_id}_analytics"
            analytics_job = ETLJob(
                job_id=analytics_job_id,
                job_type="BATCH",
                status="RUNNING",
                start_time=datetime.utcnow()
            )
            db.add(analytics_job)
            await db.commit()  # Await the commit

            # Use async with for the connection
            async with async_engine.connect() as connection:
                # Regional analytics
                await connection.execute(text("""
                    INSERT INTO sales_analytics 
                        (dimension_type, dimension_value, time_period, period_start, period_end, 
                        total_sales, total_cost, total_profit, units_sold, avg_order_value)
                    SELECT 
                        'REGION' as dimension_type,
                        region as dimension_value,
                        'MONTHLY' as time_period,
                        DATE_FORMAT(order_date, '%Y-%m-01') as period_start,
                        LAST_DAY(order_date) as period_end,
                        SUM(total_revenue) as total_sales,
                        SUM(total_cost) as total_cost,
                        SUM(total_profit) as total_profit,
                        SUM(units_sold) as units_sold,
                        AVG(total_revenue) as avg_order_value
                    FROM sales_records
                    GROUP BY dimension_type, dimension_value, time_period, period_start, period_end
                    ON DUPLICATE KEY UPDATE
                        total_sales = VALUES(total_sales),
                        total_cost = VALUES(total_cost),
                        total_profit = VALUES(total_profit),
                        units_sold = VALUES(units_sold),
                        avg_order_value = VALUES(avg_order_value),
                        updated_at = CURRENT_TIMESTAMP
                """))

                # Other queries (Country, Item Type, Sales Channel, and Date analytics)
                # Repeat similar blocks for the other analytics operations
                # ...

                await connection.commit()  # Await the commit

            # Update analytics job status to COMPLETED
            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == analytics_job_id)
                .values(
                    status="COMPLETED",
                    end_time=datetime.utcnow()
                )
            )
            await db.execute(stmt)  # Await execute
            await db.commit()       # Await commit

            logger.info(f"Analytics generation completed for job {job_id}")

        except Exception as e:
            logger.error(f"Analytics generation failed for job {job_id}: {str(e)}")

            # Update analytics job status to FAILED
            stmt = (
                update(ETLJob)
                .where(ETLJob.job_id == f"{job_id}_analytics")
                .values(
                    status="FAILED",
                    end_time=datetime.utcnow(),
                    error_message=str(e)
                )
            )
            await db.execute(stmt)  # Await execute
            await db.commit()       # Await commit

            # Log error details
            error_log = ErrorLog(
                job_id=f"{job_id}_analytics",
                error_type="ANALYTICS_GENERATION_ERROR",
                error_message=f"Failed to generate analytics: {str(e)}",
                severity="MEDIUM",
                error_details={"exception": str(e)}
            )
            db.add(error_log)
            await db.commit()  # Await commit
 