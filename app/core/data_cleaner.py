import pandas as pd
import numpy as np
from datetime import datetime
import logging
import re

# Set up logging
logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Handles data validation and cleaning for the ETL pipeline.
    
    This class is responsible for:
    - Validating input data against defined rules
    - Cleaning and normalizing data
    - Transforming data to the correct formats
    - Calculating derived fields
    """
    
    def __init__(self, validation_rules=None):
        self.validation_errors = []
        self.required_fields = [
            "region", "country", "item_type", "sales_channel", 
            "order_priority", "order_date", "order_id", 
            "ship_date", "units_sold", "unit_price", "unit_cost"
        ]
        
        # Define validation rules
        self.validation_rules = {
            "region": {
                "type": "string", 
                "max_length": 100,
                "valid_values": ["North America", "Europe", "Asia", "Africa", 
                                "Middle East", "Australia", "Central America", 
                                "South America"]
            },
            "country": {"type": "string", "max_length": 100},
            "item_type": {"type": "string", "max_length": 100},
            "sales_channel": {
                "type": "string", 
                "max_length": 50,
                "valid_values": ["Online", "Offline"]
            },
            "order_priority": {
                "type": "string", 
                "max_length": 1,
                "valid_values": ["H", "M", "L", "C"]
            },
            "order_date": {"type": "date"},
            "order_id": {
                "type": "string", 
                "max_length": 50, 
                "pattern": r"^[A-Z]{2}-\d{4}-\d{7}$"
            },
            "ship_date": {"type": "date"},
            "units_sold": {"type": "integer", "min_value": 1},
            "unit_price": {"type": "decimal", "min_value": 0.01},
            "unit_cost": {"type": "decimal", "min_value": 0.01},
        }
    
    def validate_record(self, record):
        """
        Validate a single record against defined rules.
        
        Args:
            record: Dictionary containing record data
            
        Returns:
            tuple: (is_valid, errors)
        """
        errors = []
        
        # Check for required fields
        for field in self.required_fields:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: {field}")
        
        if errors:
            return False, errors

        # Validate each field according to rules
        for field, value in record.items():
            if field in self.validation_rules:
                rules = self.validation_rules[field]
                
                # Type validation
                if rules["type"] == "string":
                    if not isinstance(value, str):
                        errors.append(f"Field {field} must be a string")
                    elif "max_length" in rules and len(value) > rules["max_length"]:
                        errors.append(f"Field {field} exceeds maximum length of {rules['max_length']}")
                    elif "pattern" in rules and not re.match(rules["pattern"], value):
                        errors.append(f"Field {field} does not match required pattern")
                    elif "valid_values" in rules and value not in rules["valid_values"]:
                        errors.append(f"Field {field} has invalid value. Expected one of: {', '.join(rules['valid_values'])}")
                
                elif rules["type"] == "integer":
                    try:
                        int_value = int(value)
                        if "min_value" in rules and int_value < rules["min_value"]:
                            errors.append(f"Field {field} must be at least {rules['min_value']}")
                    except (ValueError, TypeError):
                        errors.append(f"Field {field} must be an integer")
                
                elif rules["type"] == "decimal":
                    try:
                        float_value = float(value)
                        if "min_value" in rules and float_value < rules["min_value"]:
                            errors.append(f"Field {field} must be at least {rules['min_value']}")
                    except (ValueError, TypeError):
                        errors.append(f"Field {field} must be a decimal number")
                
                elif rules["type"] == "date":
                    if isinstance(value, str):
                        try:
                            # Try to parse date string
                            datetime.strptime(value, "%Y-%m-%d")
                        except ValueError:
                            errors.append(f"Field {field} must be a valid date in YYYY-MM-DD format")
                    elif not isinstance(value, (datetime, pd.Timestamp)):
                        errors.append(f"Field {field} must be a valid date")
        
        # Business logic validations
        if "order_date" in record and "ship_date" in record:
            order_date = record["order_date"]
            ship_date = record["ship_date"]
            
            # Convert string dates to datetime if needed
            if isinstance(order_date, str):
                try:
                    order_date = datetime.strptime(order_date, "%Y-%m-%d")
                except ValueError:
                    # Already logged in the date validation above
                    pass
            
            if isinstance(ship_date, str):
                try:
                    ship_date = datetime.strptime(ship_date, "%Y-%m-%d")
                except ValueError:
                    # Already logged in the date validation above
                    pass
            
            if isinstance(order_date, datetime) and isinstance(ship_date, datetime):
                if ship_date < order_date:
                    errors.append("Ship date cannot be earlier than order date")
        
        return len(errors) == 0, errors
    
    def clean_record(self, record):
        """
        Clean and normalize a single record.
        
        Args:
            record: Dictionary containing record data
            
        Returns:
            tuple: (cleaned_record, is_modified)
        """
        cleaned = record.copy()
        modified = False
        
        # Convert and clean string fields
        for field in ["region", "country", "item_type", "sales_channel", "order_priority"]:
            if field in cleaned and isinstance(cleaned[field], str):
                original = cleaned[field]
                # Trim whitespace
                cleaned[field] = cleaned[field].strip()
                # Convert to title case for consistency
                if field in ["region", "country", "item_type"]:
                    cleaned[field] = cleaned[field].title()
                # Check if modified
                if original != cleaned[field]:
                    modified = True
        
        # Convert date fields
        for field in ["order_date", "ship_date"]:
            if field in cleaned:
                original = cleaned[field]
                if isinstance(cleaned[field], str):
                    try:
                        cleaned[field] = datetime.strptime(cleaned[field], "%Y-%m-%d").date()
                        modified = True
                    except ValueError:
                        # Keep original if parsing fails
                        pass
                elif isinstance(cleaned[field], datetime):
                    cleaned[field] = cleaned[field].date()
                    modified = True
        
        # Convert numeric fields
        for field in ["units_sold"]:
            if field in cleaned:
                original = cleaned[field]
                try:
                    cleaned[field] = int(cleaned[field])
                    if original != cleaned[field]:
                        modified = True
                except (ValueError, TypeError):
                    # Keep original if conversion fails
                    pass
        
        for field in ["unit_price", "unit_cost"]:
            if field in cleaned:
                original = cleaned[field]
                try:
                    cleaned[field] = round(float(cleaned[field]), 2)
                    if original != cleaned[field]:
                        modified = True
                except (ValueError, TypeError):
                    # Keep original if conversion fails
                    pass
        
        # Calculate derived fields if needed
        if "units_sold" in cleaned and "unit_price" in cleaned:
            try:
                total_revenue = round(cleaned["units_sold"] * cleaned["unit_price"], 2)
                if "total_revenue" not in cleaned or cleaned["total_revenue"] != total_revenue:
                    cleaned["total_revenue"] = total_revenue
                    modified = True
            except (ValueError, TypeError):
                # Skip calculation if conversion fails
                pass
        
        if "units_sold" in cleaned and "unit_cost" in cleaned:
            try:
                total_cost = round(cleaned["units_sold"] * cleaned["unit_cost"], 2)
                if "total_cost" not in cleaned or cleaned["total_cost"] != total_cost:
                    cleaned["total_cost"] = total_cost
                    modified = True
            except (ValueError, TypeError):
                # Skip calculation if conversion fails
                pass
        
        if "total_revenue" in cleaned and "total_cost" in cleaned:
            try:
                total_profit = round(cleaned["total_revenue"] - cleaned["total_cost"], 2)
                if "total_profit" not in cleaned or cleaned["total_profit"] != total_profit:
                    cleaned["total_profit"] = total_profit
                    modified = True
            except (ValueError, TypeError):
                # Skip calculation if conversion fails
                pass
        
        return cleaned, modified
    
    def process_dataframe(self, df):
        """
        Process a pandas DataFrame with cleaning and validation.
        
        Args:
            df: Input pandas DataFrame
            
        Returns:
            tuple: (cleaned_df, validation_results)
        """
        if df.empty:
            return df, {"total": 0, "valid": 0, "invalid": 0, "modified": 0, "errors": []}
        
        # Make a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Initialize tracking columns
        df_copy['_is_valid'] = False
        df_copy['_errors'] = None
        df_copy['_modified'] = False
        
        # Clean and validate each record
        for idx, row in df_copy.iterrows():
            record = row.to_dict()
            
            # Clean the record
            cleaned_record, was_modified = self.clean_record(record)
            df_copy.loc[idx, '_modified'] = was_modified
            
            # Update the row with cleaned values
            for field, value in cleaned_record.items():
                if field in df_copy.columns:
                    df_copy.loc[idx, field] = value
            
            # Validate the cleaned record
            is_valid, errors = self.validate_record(cleaned_record)
            df_copy.loc[idx, '_is_valid'] = is_valid
            if errors:
                df_copy.loc[idx, '_errors'] = '; '.join(errors)
        
        # Collect validation results
        total_records = len(df_copy)
        valid_records = df_copy['_is_valid'].sum()
        invalid_records = total_records - valid_records
        modified_records = df_copy['_modified'].sum()
        
        # Collect sample errors for reporting
        error_samples = []
        if invalid_records > 0:
            error_df = df_copy[df_copy['_is_valid'] == False].head(10)  # Sample up to 10 errors
            for idx, row in error_df.iterrows():
                if pd.notna(row['_errors']):
                    error_samples.append({
                        "record_id": row.get("order_id", f"Row {idx}"),
                        "errors": row['_errors']
                    })
        
        # Prepare validation results
        validation_results = {
            "total": total_records,
            "valid": int(valid_records),
            "invalid": int(invalid_records),
            "modified": int(modified_records),
            "errors": error_samples
        }
        
        # Drop tracking columns from final output
        cleaned_df = df_copy.drop(columns=['_is_valid', '_errors', '_modified'])
        
        return cleaned_df, validation_results