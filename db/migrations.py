# app/db/migrations.py

import os
import logging
import pymysql
from pathlib import Path
from dotenv import load_dotenv

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

class Migrator:
    """Utility class to run database migrations."""
    
    def __init__(self):
        self.migrations_dir = Path(__file__).parent.parent.parent / 'db' / 'scripts'
        self.conn = None
    
    def connect(self):
        """Connect to the database."""
        try:
            self.conn = pymysql.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            logger.info(f"Connected to database {DB_NAME}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def _create_migration_table(self):
        """Create migration tracking table if it doesn't exist."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                version VARCHAR(100) NOT NULL UNIQUE,
                description VARCHAR(255) NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            self.conn.commit()
            logger.info("Migration tracking table created or already exists")
    
    def _get_applied_migrations(self):
        """Get list of already applied migrations."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT version FROM migrations ORDER BY id")
            return [row[0] for row in cursor.fetchall()]
    
    def _get_migration_files(self):
        """Get list of migration files."""
        migrations = []
        for file in self.migrations_dir.glob('../migrations/V*__*.sql'):
            # Extract version and description from filename (e.g., V1__initial_schema.sql)
            parts = file.name.split('__')
            if len(parts) >= 2:
                version = parts[0][1:]  # Remove 'V' prefix
                description = parts[1].replace('.sql', '')
                migrations.append({
                    'version': version,
                    'description': description,
                    'file': file
                })
        
        # Sort migrations by version number
        return sorted(migrations, key=lambda m: float(m['version']))
    
    def run_migrations(self):
        """Run all pending migrations."""
        if not self.connect():
            return False
        
        try:
            self._create_migration_table()
            applied_migrations = set(self._get_applied_migrations())
            migration_files = self._get_migration_files()
            
            for migration in migration_files:
                if migration['version'] not in applied_migrations:
                    logger.info(f"Applying migration {migration['version']}: {migration['description']}")
                    
                    # Read migration script
                    with open(migration['file'], 'r') as f:
                        sql = f.read()
                    
                    # Execute migration script
                    with self.conn.cursor() as cursor:
                        # Split the script by semicolon to execute multiple statements
                        statements = sql.split(';')
                        for statement in statements:
                            if statement.strip():
                                cursor.execute(statement)
                        
                        # Record the migration
                        cursor.execute(
                            "INSERT INTO migrations (version, description) VALUES (%s, %s)",
                            (migration['version'], migration['description'])
                        )
                        
                        self.conn.commit()
                        logger.info(f"Migration {migration['version']} applied successfully")
            
            logger.info("All migrations have been applied")
            return True
            
        except Exception as e:
            logger.error(f"Error running migrations: {str(e)}")
            self.conn.rollback()
            return False
        finally:
            self.close()

def run_migrations():
    """Run database migrations."""
    migrator = Migrator()
    return migrator.run_migrations()

if __name__ == "__main__":
    run_migrations()