from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, date
from sqlalchemy import func

from ..db.repository import SalesRecordRepository
from ..db.transaction import transaction_scope

# Set up logging
logger = logging.getLogger(__name__)

class SalesService:
    """
    Service for sales record operations.
    
    This service provides business logic for working with sales records,
    including queries, aggregations, and analytics.
    """
    
    def __init__(self):
        """Initialize the service with repositories."""
        self.record_repo = SalesRecordRepository()
    
    def get_sales_record(self, order_id: str) -> Dict[str, Any]:
        """
        Get a sales record by order ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Dict: Sales record data or None if not found
        """
        with transaction_scope() as session:
            record = self.record_repo.get_by_order_id(session, order_id)
            if not record:
                return None
            
            return {
                "id": record.id,
                "order_id": record.order_id,
                "region": record.region,
                "country": record.country,
                "item_type": record.item_type,
                "sales_channel": record.sales_channel,
                "order_priority": record.order_priority,
                "order_date": record.order_date.isoformat() if record.order_date else None,
                "ship_date": record.ship_date.isoformat() if record.ship_date else None,
                "units_sold": record.units_sold,
                "unit_price": float(record.unit_price),
                "unit_cost": float(record.unit_cost),
                "total_revenue": float(record.total_revenue),
                "total_cost": float(record.total_cost),
                "total_profit": float(record.total_profit),
                "processed_at": record.processed_at.isoformat(),
                "is_valid": record.is_valid,
                "batch_id": record.batch_id
            }
    
    def get_sales_by_region(self) -> List[Dict[str, Any]]:
        """
        Get sales data aggregated by region.
        
        Returns:
            List[Dict]: Sales summary by region
        """
        with transaction_scope() as session:
            return self.record_repo.get_sales_summary_by_region(session)
    
    def get_sales_by_date_range(
        self, start_date: date, end_date: date, group_by: str = "day"
    ) -> List[Dict[str, Any]]:
        """
        Get sales data aggregated by date.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            group_by: Grouping level ('day', 'month', 'year')
            
        Returns:
            List[Dict]: Sales data by date
        """
        with transaction_scope() as session:
            from sqlalchemy import func, desc
            
            query = session.query(self.record_repo.model)
            
            # Apply date range filter
            query = query.filter(
                self.record_repo.model.order_date >= start_date,
                self.record_repo.model.order_date <= end_date
            )
            
            # Apply grouping
            if group_by == "day":
                date_field = self.record_repo.model.order_date
            elif group_by == "month":
                date_field = func.date_format(self.record_repo.model.order_date, '%Y-%m-01')
            elif group_by == "year":
                date_field = func.date_format(self.record_repo.model.order_date, '%Y-01-01')
            else:
                raise ValueError(f"Invalid group_by value: {group_by}")
            
            query = session.query(
                date_field.label("date"),
                func.count().label("order_count"),
                func.sum(self.record_repo.model.units_sold).label("total_units"),
                func.sum(self.record_repo.model.total_revenue).label("total_revenue"),
                func.sum(self.record_repo.model.total_profit).label("total_profit")
            ).group_by("date").order_by("date")
            
            result = []
            for row in query.all():
                result.append({
                    "date": row.date.isoformat() if hasattr(row.date, 'isoformat') else str(row.date),
                    "order_count": row.order_count,
                    "total_units": int(row.total_units) if row.total_units else 0,
                    "total_revenue": float(row.total_revenue) if row.total_revenue else 0.0,
                    "total_profit": float(row.total_profit) if row.total_profit else 0.0
                })
            
            return result
    
    def get_sales_by_item_type(self) -> List[Dict[str, Any]]:
        """
        Get sales data aggregated by item type.
        
        Returns:
            List[Dict]: Sales summary by item type
        """
        with transaction_scope() as session:
            from sqlalchemy import func, desc
            
            query = session.query(
                self.record_repo.model.item_type,
                func.count().label("order_count"),
                func.sum(self.record_repo.model.units_sold).label("total_units"),
                func.sum(self.record_repo.model.total_revenue).label("total_revenue"),
                func.sum(self.record_repo.model.total_profit).label("total_profit")
            ).group_by(self.record_repo.model.item_type).order_by(desc("total_profit"))
            
            result = []
            for row in query.all():
                result.append({
                    "item_type": row.item_type,
                    "order_count": row.order_count,
                    "total_units": int(row.total_units) if row.total_units else 0,
                    "total_revenue": float(row.total_revenue) if row.total_revenue else 0.0,
                    "total_profit": float(row.total_profit) if row.total_profit else 0.0
                })
            
            return result
    
    def search_sales_records(
        self, search_term: str = None, region: str = None, country: str = None,
        item_type: str = None, start_date: date = None, end_date: date = None,
        skip: int = 0, limit: int = 100
    ) -> Dict[str, Any]:
        """
        Search sales records with various filters.
        
        Args:
            search_term: Text to search for in order ID
            region: Filter by region
            country: Filter by country
            item_type: Filter by item type
            start_date: Filter by order date (start)
            end_date: Filter by order date (end)
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            Dict: Search results with count and data
        """
        with transaction_scope() as session:
            from sqlalchemy import or_
            
            query = session.query(self.record_repo.model)
            
            # Apply filters
            if search_term:
                query = query.filter(self.record_repo.model.order_id.ilike(f"%{search_term}%"))
            
            if region:
                query = query.filter(self.record_repo.model.region == region)
            
            if country:
                query = query.filter(self.record_repo.model.country == country)
            
            if item_type:
                query = query.filter(self.record_repo.model.item_type == item_type)
            
            if start_date:
                query = query.filter(self.record_repo.model.order_date >= start_date)
            
            if end_date:
                query = query.filter(self.record_repo.model.order_date <= end_date)
            
            # Get total count
            total_count = query.count()
            
            # Apply pagination
            query = query.order_by(self.record_repo.model.order_date.desc())
            query = query.offset(skip).limit(limit)
            
            # Execute query
            records = query.all()

            # Format results
            result_data = []
            for record in records:
                result_data.append({
                    "id": record.id,
                    "order_id": record.order_id,
                    "region": record.region,
                    "country": record.country,
                    "item_type": record.item_type,
                    "sales_channel": record.sales_channel,
                    "order_priority": record.order_priority,
                    "order_date": record.order_date.isoformat() if record.order_date else None,
                    "ship_date": record.ship_date.isoformat() if record.ship_date else None,
                    "units_sold": record.units_sold,
                    "unit_price": float(record.unit_price),
                    "unit_cost": float(record.unit_cost),
                    "total_revenue": float(record.total_revenue),
                    "total_cost": float(record.total_cost),
                    "total_profit": float(record.total_profit),
                    "batch_id": record.batch_id
                })
            
            return {
                "total": total_count,
                "page": skip // limit + 1 if limit > 0 else 1,
                "pages": (total_count + limit - 1) // limit if limit > 0 else 1,
                "limit": limit,
                "data": result_data
            }