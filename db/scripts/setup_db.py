# db/scripts/setup_db.py

import pymysql
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER', 'etl_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'etl_password')
DB_NAME = os.getenv('DB_NAME', 'sales_etl')
DB_ROOT_PASSWORD = os.getenv('DB_ROOT_PASSWORD', '')

def create_database():
    """Create database if it doesn't exist."""
    try:
        # Connect to MySQL server as root
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user='root',
            password=DB_ROOT_PASSWORD
        )
        
        with conn.cursor() as cursor:
            # Create database if not exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"Database '{DB_NAME}' created or already exists")
            
            # Create user if not exists
            try:
                cursor.execute(f"CREATE USER IF NOT EXISTS '{DB_USER}'@'%' IDENTIFIED BY '{DB_PASSWORD}'")
                logger.info(f"User '{DB_USER}' created or already exists")
            except pymysql.err.MySQLError:
                logger.warning(f"User '{DB_USER}' may already exist with different privileges")
            
            # Grant privileges
            cursor.execute(f"GRANT ALL PRIVILEGES ON {DB_NAME}.* TO '{DB_USER}'@'%'")
            cursor.execute("FLUSH PRIVILEGES")
            logger.info(f"Privileges granted to user '{DB_USER}' on database '{DB_NAME}'")
            
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error creating database: {str(e)}")
        return False

def run_schema_script():
    """Run the schema SQL script to create tables."""
    try:
        # Connect to the database
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        # Read the schema SQL file
        schema_path = os.path.join(os.path.dirname(__file__), '../migrations/V1__initial_schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
            
        # Execute the SQL script
        with conn.cursor() as cursor:
            # Split the script by semicolon to execute multiple statements
            statements = schema_sql.split(';')
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
            conn.commit()
            
        logger.info("Schema created successfully")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error running schema script: {str(e)}")
        return False

def create_indexes():
    """Create additional indexes for performance optimization."""
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        with conn.cursor() as cursor:
            # Check and create index for sales_records table
            cursor.execute("SHOW INDEX FROM sales_records WHERE Key_name = 'idx_sales_profit';")
            if not cursor.fetchone():
                cursor.execute("CREATE INDEX idx_sales_profit ON sales_records(total_profit)")
                logger.info("Index idx_sales_profit created successfully.")
            
            cursor.execute("SHOW INDEX FROM sales_records WHERE Key_name = 'idx_sales_channel';")
            if not cursor.fetchone():
                cursor.execute("CREATE INDEX idx_sales_channel ON sales_records(sales_channel)")
                logger.info("Index idx_sales_channel created successfully.")
            
            cursor.execute("SHOW INDEX FROM sales_records WHERE Key_name = 'idx_processed_valid';")
            if not cursor.fetchone():
                cursor.execute("CREATE INDEX idx_processed_valid ON sales_records(processed_at, is_valid)")
                logger.info("Index idx_processed_valid created successfully.")
            
            # Check and create index for etl_metrics table
            cursor.execute("SHOW INDEX FROM etl_metrics WHERE Key_name = 'idx_metric_component';")
            if not cursor.fetchone():
                cursor.execute("CREATE INDEX idx_metric_component ON etl_metrics(component)")
                logger.info("Index idx_metric_component created successfully.")
            
            cursor.execute("SHOW INDEX FROM etl_metrics WHERE Key_name = 'idx_metric_name_component';")
            if not cursor.fetchone():
                cursor.execute("CREATE INDEX idx_metric_name_component ON etl_metrics(metric_name, component)")
                logger.info("Index idx_metric_name_component created successfully.")
            
            # Check and create index for sales_batches table
            cursor.execute("SHOW INDEX FROM sales_batches WHERE Key_name = 'idx_batch_status_time';")
            if not cursor.fetchone():
                cursor.execute("CREATE INDEX idx_batch_status_time ON sales_batches(status, start_time)")
                logger.info("Index idx_batch_status_time created successfully.")
            
            # Check and create full-text index for etl_errors table
            cursor.execute("SHOW INDEX FROM etl_errors WHERE Key_name = 'idx_error_message';")
            if not cursor.fetchone():
                cursor.execute("CREATE FULLTEXT INDEX idx_error_message ON etl_errors(error_message)")
                logger.info("Full-text index idx_error_message created successfully.")
            
            conn.commit()
            logger.info("Additional indexes created successfully")
        
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")
        return False

def create_views():
    """Create database views for common queries."""
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        with conn.cursor() as cursor:
            # Sales summary by region view
            cursor.execute("""
            CREATE OR REPLACE VIEW sales_by_region AS
            SELECT 
                region,
                COUNT(*) as total_orders,
                SUM(units_sold) as total_units,
                SUM(total_revenue) as total_revenue,
                SUM(total_profit) as total_profit,
                AVG(total_profit) as avg_profit_per_order
            FROM sales_records
            WHERE is_valid = TRUE
            GROUP BY region
            """)
            
            # Daily sales performance view
            cursor.execute("""
            CREATE OR REPLACE VIEW daily_sales AS
            SELECT 
                order_date,
                COUNT(*) as orders_count,
                SUM(units_sold) as units_sold,
                SUM(total_revenue) as revenue,
                SUM(total_profit) as profit
            FROM sales_records
            WHERE is_valid = TRUE
            GROUP BY order_date
            ORDER BY order_date DESC
            """)
            
            # Batch processing performance view
            cursor.execute("""
            CREATE OR REPLACE VIEW batch_performance AS
            SELECT 
                b.batch_id,
                b.source,
                b.record_count,
                b.start_time,
                b.end_time,
                b.status,
                b.error_count,
                TIMESTAMPDIFF(SECOND, b.start_time, COALESCE(b.end_time, NOW())) as processing_time_seconds,
                CASE WHEN b.status = 'completed' THEN
                    b.record_count / NULLIF(TIMESTAMPDIFF(SECOND, b.start_time, b.end_time), 0)
                ELSE NULL END as records_per_second
            FROM sales_batches b
            """)
            
            # Recent errors view
            cursor.execute("""
            CREATE OR REPLACE VIEW recent_errors AS
            SELECT 
                e.id,
                e.error_type,
                e.error_message,
                e.component,
                e.severity,
                e.record_id,
                e.batch_id,
                b.source as batch_source,
                e.timestamp,
                e.resolved
            FROM etl_errors e
            LEFT JOIN sales_batches b ON e.batch_id = b.batch_id
            ORDER BY e.timestamp DESC
            LIMIT 100
            """)
            
            conn.commit()
            logger.info("Database views created successfully")
        
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error creating views: {str(e)}")
        return False

def create_stored_procedures():
    """Create stored procedures for common operations."""
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        with conn.cursor() as cursor:
            # Drop the procedure if it exists and create the new procedure for batch statistics
            cursor.execute("DROP PROCEDURE IF EXISTS get_batch_statistics;")
            cursor.execute("""
            CREATE PROCEDURE get_batch_statistics(IN p_batch_id VARCHAR(50))
            BEGIN
                SELECT 
                    b.batch_id,
                    b.source,
                    b.record_count,
                    b.start_time,
                    b.end_time,
                    TIMESTAMPDIFF(SECOND, b.start_time, COALESCE(b.end_time, NOW())) as processing_time_seconds,
                    b.status,
                    b.error_count,
                    COUNT(DISTINCT sr.id) as processed_records,
                    SUM(CASE WHEN sr.is_valid = TRUE THEN 1 ELSE 0 END) as valid_records,
                    SUM(CASE WHEN sr.is_valid = FALSE THEN 1 ELSE 0 END) as invalid_records
                FROM sales_batches b
                LEFT JOIN sales_records sr ON b.batch_id = sr.batch_id
                WHERE b.batch_id = p_batch_id
                GROUP BY b.batch_id, b.source, b.record_count, b.start_time, b.end_time, b.status, b.error_count;
            END
            """)
            
            # Drop and recreate the procedure for cleaning up old data
            cursor.execute("DROP PROCEDURE IF EXISTS cleanup_old_data;")
            cursor.execute("""
            CREATE PROCEDURE cleanup_old_data(IN p_days_to_keep INT)
            BEGIN
                DECLARE cutoff_date DATETIME;
                SET cutoff_date = DATE_SUB(NOW(), INTERVAL p_days_to_keep DAY);
                
                -- Delete old metrics
                DELETE FROM etl_metrics WHERE timestamp < cutoff_date;
                
                -- Archive old batches with status 'completed' or 'failed'
                -- In a real system, this would move data to an archive table first
                DELETE FROM sales_batches 
                WHERE created_at < cutoff_date 
                AND status IN ('completed', 'failed');
                
                -- Return count of deleted records
                SELECT 
                    ROW_COUNT() as deleted_count,
                    NOW() as execution_time,
                    p_days_to_keep as retention_days;
            END
            """)
            
            # Drop and recreate the procedure for resolving errors by type
            cursor.execute("DROP PROCEDURE IF EXISTS resolve_errors_by_type;")
            cursor.execute("""
            CREATE PROCEDURE resolve_errors_by_type(
                IN p_error_type VARCHAR(100),
                IN p_resolution_note TEXT,
                IN p_resolved_by VARCHAR(100)
            )
            BEGIN
                UPDATE etl_errors 
                SET resolved = TRUE,
                    resolution_note = CONCAT(
                        IFNULL(resolution_note, ''), 
                        IF(resolution_note IS NULL, '', '\n'),
                        'Bulk resolved by ', p_resolved_by, ' at ', NOW(), ': ', p_resolution_note
                    )
                WHERE error_type = p_error_type
                AND resolved = FALSE;
                
                SELECT ROW_COUNT() as resolved_count;
                
                -- Add audit entry
                INSERT INTO etl_audit (action, component, details, user_id)
                VALUES ('bulk_resolve_errors', 'error_management', 
                        CONCAT('Bulk resolved errors of type: ', p_error_type), p_resolved_by);
            END
            """)
            
            conn.commit()
            logger.info("Stored procedures created successfully")
        
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error creating stored procedures: {str(e)}")
        return False

def setup_database():
    """Main function to set up the database."""
    logger.info("Starting database setup...")
    
    # Create database and user
    if not create_database():
        logger.error("Failed to create database. Exiting...")
        return False
    
    # Run schema script to create tables
    if not run_schema_script():
        logger.error("Failed to create schema. Exiting...")
        return False
    
    # Create additional indexes
    if not create_indexes():
        logger.warning("Failed to create some indexes, but continuing...")
    
    # Create views
    if not create_views():
        logger.warning("Failed to create views, but continuing...")
    
    # Create stored procedures
    if not create_stored_procedures():
        logger.warning("Failed to create stored procedures, but continuing...")
    
    logger.info("Database setup completed successfully")
    return True

if __name__ == "__main__":
    setup_database()