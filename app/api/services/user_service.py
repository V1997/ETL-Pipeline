"""
Service layer for user management

Provides functions for user authentication, creation, and management.
"""

from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import json

from sqlalchemy.exc import IntegrityError
from sqlalchemy import select

from app.db.session import get_db_session
from app.models.models import User
from app.api.auth.auth_handler import hash_password, verify_password

# Configure logging
logger = logging.getLogger(__name__)

# Current date and time
CURRENT_TIME = "2025-03-07 06:24:25"


def _serialize_user(user: User) -> Dict[str, Any]:
    """
    Serialize a User model instance to a dictionary
    
    Args:
        user: User model instance
        
    Returns:
        Dict: Serialized user
    """
    return {
        "username": user.username,
        "email": user.email,
        "full_name": user.full_name,
        "disabled": user.disabled,
        "roles": user.roles.split(","),
        "created_at": user.created_at.strftime("%Y-%m-%d %H:%M:%S") if user.created_at else None,
        "last_login": user.last_login.strftime("%Y-%m-%d %H:%M:%S") if user.last_login else None,
    }


def ensure_default_users_exist():
    """
    Ensure that default users exist in the database
    """
    default_users = [
        # {
        #     "username": "admin",
        #     "email": "admin@example.com",
        #     "full_name": "Administrator",
        #     "password": "admin123",
        #     "disabled": False,
        #     "roles": ["admin", "operator", "viewer"],
        # },
        # {
        #     "username": "operator",
        #     "email": "operator@example.com",
        #     "full_name": "ETL Operator",
        #     "password": "operator123",
        #     "disabled": False,
        #     "roles": ["operator", "viewer"],
        # },
        # {
        #     "username": "viewer",
        #     "email": "viewer@example.com",
        #     "full_name": "ETL Viewer",
        #     "password": "viewer123",
        #     "disabled": False,
        #     "roles": ["viewer"],
        # },
        # {
        #     "username": "V1997",
        #     "email": "v1997@example.com",
        #     "full_name": "Current Active User",
        #     "password": "admin123",
        #     "disabled": False,
        #     "roles": ["admin", "operator", "viewer"],
        # }
    ]
    
    for user_data in default_users:
        try:
            # Check if user already exists
            user = get_user_by_username(user_data["username"])
            if not user:
                create_user(user_data)
                logger.info(f"Created default user: {user_data['username']}")
        except Exception as e:
            logger.warning(f"Could not create default user {user_data['username']}: {str(e)}")


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """
    Get a user by username
    
    Args:
        username: Username to retrieve
        
    Returns:
        Optional[Dict]: User information or None if not found
    """
    try:
        with get_db_session() as session:
            # Query the database for the user
            stmt = select(User).where(User.username == username)
            user = session.execute(stmt).scalar_one_or_none()
            
            if not user:
                return None
            
            # Return serialized user (without password)
            return _serialize_user(user)
            
    except Exception as e:
        logger.error(f"Error retrieving user {username}: {str(e)}")
        raise


def list_users() -> List[Dict[str, Any]]:
    """
    List all users
    
    Returns:
        List[Dict]: List of users
    """
    try:
        with get_db_session() as session:
            # Query all users
            stmt = select(User)
            result = session.execute(stmt)
            users = []
            
            for row in result.scalars():
                # Serialize user (without password)
                users.append(_serialize_user(row))
            
            return users
            
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        raise


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate a user with username and password
    
    Args:
        username: Username to authenticate
        password: Password to verify
        
    Returns:
        Optional[Dict]: User information if authentication succeeds, None otherwise
    """
    try:
        with get_db_session() as session:
            # Query the user
            stmt = select(User).where(User.username == username)
            user = session.execute(stmt).scalar_one_or_none()
            
            if not user:
                return None
            
            # Verify the password
            if not verify_password(password, user.hashed_password):
                return None
            
            # Update last login time
            user.last_login = datetime.now()
            session.commit()
            
            # Return serialized user
            return _serialize_user(user)
            
    except Exception as e:
        logger.error(f"Error authenticating user {username}: {str(e)}")
        raise


def create_user(user_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new user
    
    Args:
        user_data: User information
        
    Returns:
        Dict: Created user information
    """
    try:
        with get_db_session() as session:
            # Check if username already exists
            stmt = select(User).where(User.username == user_data["username"])
            existing_user = session.execute(stmt).scalar_one_or_none()
            
            if existing_user:
                raise ValueError(f"User with username '{user_data['username']}' already exists")
            
            # Hash the password
            hashed_password = hash_password(user_data["password"])
            
            # Format roles as comma-separated string
            roles_str = ",".join(user_data.get("roles", ["viewer"]))
            
            # Create new user
            new_user = User(
                username=user_data["username"],
                email=user_data["email"],
                full_name=user_data.get("full_name"),
                hashed_password=hashed_password,
                disabled=user_data.get("disabled", False),
                roles=roles_str,
                created_at=datetime.now()
            )
            
            # Add to database
            session.add(new_user)
            session.commit()
            
            # Return serialized user
            return _serialize_user(new_user)
            
    except IntegrityError:
        logger.error(f"Integrity error creating user '{user_data['username']}' - duplicate entry")
        raise ValueError(f"User with username '{user_data['username']}' or email already exists")
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise


def update_user(username: str, user_data: Dict[str, Any], by_admin: bool) -> Dict[str, Any]:
    """
    Update a user
    
    Args:
        username: Username to update
        user_data: User information to update
        by_admin: Whether the update is performed by an admin
        
    Returns:
        Dict: Updated user information
    """
    try:
        with get_db_session() as session:
            # Query the user
            stmt = select(User).where(User.username == username)
            user = session.execute(stmt).scalar_one_or_none()
            
            if not user:
                raise ValueError(f"User '{username}' not found")
            
            # Update user data
            if "email" in user_data and user_data["email"]:
                user.email = user_data["email"]
                
            if "full_name" in user_data:
                user.full_name = user_data["full_name"]
                
            if "disabled" in user_data:
                user.disabled = user_data["disabled"]
                
            # Only admins can update roles
            if by_admin and "roles" in user_data:
                user.roles = ",".join(user_data["roles"])
            
            # Commit changes
            session.commit()
            
            # Return serialized user
            return _serialize_user(user)
            
    except Exception as e:
        logger.error(f"Error updating user {username}: {str(e)}")
        raise


def change_user_password(username: str, new_password: str) -> bool:
    """
    Change a user's password
    
    Args:
        username: Username to update
        new_password: New password
        
    Returns:
        bool: True if successful
    """
    try:
        with get_db_session() as session:
            # Query the user
            stmt = select(User).where(User.username == username)
            user = session.execute(stmt).scalar_one_or_none()
            
            if not user:
                raise ValueError(f"User '{username}' not found")
            
            # Hash the new password
            hashed_password = hash_password(new_password)
            
            # Update the password
            user.hashed_password = hashed_password
            
            # Commit changes
            session.commit()
            
            return True
            
    except Exception as e:
        logger.error(f"Error changing password for {username}: {str(e)}")
        raise


def delete_user(username: str) -> bool:
    """
    Delete a user
    
    Args:
        username: Username to delete
        
    Returns:
        bool: True if successful
    """
    try:
        with get_db_session() as session:
            # Query the user
            stmt = select(User).where(User.username == username)
            user = session.execute(stmt).scalar_one_or_none()
            
            if not user:
                raise ValueError(f"User '{username}' not found")
            
            # Delete the user
            session.delete(user)
            session.commit()
            
            return True
            
    except Exception as e:
        logger.error(f"Error deleting user {username}: {str(e)}")
        raise