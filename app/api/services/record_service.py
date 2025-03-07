"""
Service layer for sales records operations

Provides functions for querying and exporting sales records.
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime

from app.db.session import get_db_session
from app.models.models import SalesRecord, SalesBatch
from sqlalchemy import and_, or_, func

# Configure logging
logger = logging.getLogger(__name__)
"""
Service layer for sales records operations

Provides functions for querying and exporting sales records.
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime

from app.db.session import get_db_session
from app.models.models import SalesRecord, SalesBatch
from sqlalchemy import and_, or_, func

# Configure logging
logger = logging.getLogger(__name__)


def get_batch_records(
    filters: Dict[str, Any],
    limit: int = 100,
    offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get records with filtering
    
    Args:
        filters: Dictionary of filter criteria
        limit: Maximum number of records to return
        offset: Number of records to skip
        
    Returns:
        Tuple[List[Dict], int]: List of records and total count
    """
    try:
        with get_db_session() as session:
            # Build query
            query = session.query(SalesRecord)
            
            # Apply filters
            filter_conditions = []
            
            if "batch_id" in filters:
                filter_conditions.append(SalesRecord.batch_id == filters["batch_id"])
                
            if "region" in filters:
                filter_conditions.append(SalesRecord.region == filters["region"])
                
            if "country" in filters:
                filter_conditions.append(SalesRecord.country == filters["country"])
                
            if "item_type" in filters:
                filter_conditions.append(SalesRecord.item_type == filters["item_type"])
                
            if "sales_channel" in filters:
                filter_conditions.append(SalesRecord.sales_channel == filters["sales_channel"])
                
            if "order_date_from" in filters:
                filter_conditions.append(SalesRecord.order_date >= filters["order_date_from"])
                
            if "order_date_to" in filters:
                filter_conditions.append(SalesRecord.order_date <= filters["order_date_to"])
                
            if "min_units" in filters:
                filter_conditions.append(SalesRecord.units_sold >= filters["min_units"])
                
            if "max_units" in filters:
                filter_conditions.append(SalesRecord.units_sold <= filters["max_units"])
                
            if "min_revenue" in filters:
                filter_conditions.append(SalesRecord.total_revenue >= filters["min_revenue"])
                
            if "max_revenue" in filters:
                filter_conditions.append(SalesRecord.total_revenue <= filters["max_revenue"])
            
            # Apply all filters
            if filter_conditions:
                query = query.filter(and_(*filter_conditions))
            
            # Get total count (before pagination)
            total_count = query.count()
            
            # Apply pagination
            query = query.order_by(SalesRecord.order_date.desc())
            query = query.limit(limit).offset(offset)
            
            # Execute query
            records = query.all()
            
            # Convert to dictionaries
            record_list = []
            for record in records:
                record_dict = {
                    "order_id": record.order_id,
                    "region": record.region,
                    "country": record.country,
                    "item_type": record.item_type,
                    "sales_channel": record.sales_channel,
                    "order_priority": record.order_priority,
                    "order_date": record.order_date,
                    "ship_date": record.ship_date,
                    "units_sold": record.units_sold,
                    "unit_price": record.unit_price,
                    "unit_cost": record.unit_cost,
                    "total_revenue": record.total_revenue,
                    "total_cost": record.total_cost,
                    "total_profit": record.total_profit,
                    "batch_id": record.batch_id
                }
                
                record_list.append(record_dict)
            
            return record_list, total_count
            
    except Exception as e:
        logger.error(f"Error querying records: {str(e)}")
        raise


def get_record_by_id(record_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a sales record by its ID
    
    Args:
        record_id: Record ID to retrieve
        
    Returns:
        Optional[Dict]: Record information or None if not found
    """
    try:
        with get_db_session() as session:
            record = session.query(SalesRecord).filter_by(order_id=record_id).first()
            
            if not record:
                return None
            
            # Convert to dictionary
            record_dict = {
                "order_id": record.order_id,
                "region": record.region,
                "country": record.country,
                "item_type": record.item_type,
                "sales_channel": record.sales_channel,
                "order_priority": record.order_priority,
                "order_date": record.order_date,
                "ship_date": record.ship_date,
                "units_sold": record.units_sold,
                "unit_price": record.unit_price,
                "unit_cost": record.unit_cost,
                "total_revenue": record.total_revenue,
                "total_cost": record.total_cost,
                "total_profit": record.total_profit,
                "batch_id": record.batch_id
            }
            
            return record_dict
            
    except Exception as e:
        logger.error(f"Error retrieving record {record_id}: {str(e)}")
        raise


def get_batch_records(
    filters: Dict[str, Any],
    limit: int = 100,
    offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get records with filtering
    
    Args:
        filters: Dictionary of filter criteria
        limit: Maximum number of records to return
        offset: Number of records to skip
        
    Returns:
        Tuple[List[Dict], int]: List of records and total count
    """
    try:
        with get_db_session() as session:
            # Build query
            query = session.query(SalesRecord)
            
            # Apply filters
            filter_conditions = []
            
            if "batch_id" in filters:
                filter_conditions.append(SalesRecord.batch_id == filters["batch_id"])
                
            if "region" in filters:
                filter_conditions.append(SalesRecord.region == filters["region"])
                
            if "country" in filters:
                filter_conditions.append(SalesRecord.country == filters["country"])
                
            if "item_type" in filters:
                filter_conditions.append(SalesRecord.item_type == filters["item_type"])
                
            if "sales_channel" in filters:
                filter_conditions.append(SalesRecord.sales_channel == filters["sales_channel"])
                
            if "order_date_from" in filters:
                filter_conditions.append(SalesRecord.order_date >= filters["order_date_from"])
                
            if "order_date_to" in filters:
                filter_conditions.append(SalesRecord.order_date <= filters["order_date_to"])
                
            if "min_units" in filters:
                filter_conditions.append(SalesRecord.units_sold >= filters["min_units"])
                
            if "max_units" in filters:
                filter_conditions.append(SalesRecord.units_sold <= filters["max_units"])
                
            if "min_revenue" in filters:
                filter_conditions.append(SalesRecord.total_revenue >= filters["min_revenue"])
                
            if "max_revenue" in filters:
                filter_conditions.append(SalesRecord.total_revenue <= filters["max_revenue"])
            
            # Apply all filters
            if filter_conditions:
                query = query.filter(and_(*filter_conditions))
            
            # Get total count (before pagination)
            total_count = query.count()
            
            # Apply pagination
            query = query.order_by(SalesRecord.order_date.desc())
            query = query.limit(limit).offset(offset)
            
            # Execute query
            records = query.all()
            
            # Convert to dictionaries
            record_list = []
            for record in records:
                record_dict = {
                    "order_id": record.order_id,
                    "region": record.region,
                    "country": record.country,
                    "item_type": record.item_type,
                    "sales_channel": record.sales_channel,
                    "order_priority": record.order_priority,
                    "order_date": record.order_date,
                    "ship_date": record.ship_date,
                    "units_sold": record.units_sold,
                    "unit_price": record.unit_price,
                    "unit_cost": record.unit_cost,
                    "total_revenue": record.total_revenue,
                    "total_cost": record.total_cost,
                    "total_profit": record.total_profit,
                    "batch_id": record.batch_id
                }
                
                record_list.append(record_dict)
            
            return record_list, total_count
            
    except Exception as e:
        logger.error(f"Error querying records: {str(e)}")
        raise


def get_record_by_id(record_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a sales record by its ID
    
    Args:
        record_id: Record ID to retrieve
        
    Returns:
        Optional[Dict]: Record information or None if not found
    """
    try:
        with get_db_session() as session:
            record = session.query(SalesRecord).filter_by(order_id=record_id).first()
            
            if not record:
                return None
            
            # Convert to dictionary
            record_dict = {
                "order_id": record.order_id,
                "region": record.region,
                "country": record.country,
                "item_type": record.item_type,
                "sales_channel": record.sales_channel,
                "order_priority": record.order_priority,
                "order_date": record.order_date,
                "ship_date": record.ship_date,
                "units_sold": record.units_sold,
                "unit_price": record.unit_price,
                "unit_cost": record.unit_cost,
                "total_revenue": record.total_revenue,
                "total_cost": record.total_cost,
                "total_profit": record.total_profit,
                "batch_id": record.batch_id
            }
            
            return record_dict
            
    except Exception as e:
        logger.error(f"Error retrieving record {record_id}: {str(e)}")
        raise