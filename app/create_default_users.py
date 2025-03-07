
import sys
import os

# Add the project directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.api.services.user_service import create_user
from app.db.session import get_db_session, Base, engine
from app.models.models import User

def create_default_users():
    # Create database tables if they don't exist
    Base.metadata.create_all(bind=engine)
    
    # Create default users
    admin_user = {
        "username": "admin",
        "email": "admin@example.com",
        "full_name": "Administrator",
        "password": "admin123",
        "roles": ["admin", "operator", "viewer"]
    }
    
    operator_user = {
        "username": "operator",
        "email": "operator@example.com",
        "full_name": "ETL Operator",
        "password": "operator123",
        "roles": ["operator", "viewer"]
    }
    
    viewer_user = {
        "username": "viewer",
        "email": "viewer@example.com",
        "full_name": "ETL Viewer",
        "password": "viewer123",
        "roles": ["viewer"]
    }
    
    current_user = {
        "username": "V1997",
        "email": "v1997@example.com",
        "full_name": "Current Active User",
        "password": "admin123",
        "roles": ["admin", "operator", "viewer"]
    }
    
    # Create each user, handling potential errors if they already exist
    for user_data in [admin_user, operator_user, viewer_user, current_user]:
        try:
            create_user(user_data)
            print(f"Created user: {user_data['username']}")
        except ValueError as e:
            print(f"Note: {e}")
    
    print("Default users created or already exist.")

if __name__ == "__main__":
    create_default_users()