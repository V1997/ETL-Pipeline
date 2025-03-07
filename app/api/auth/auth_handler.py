"""
Authentication and authorization handling

Provides functions for JWT token generation, hashing/verification,
and dependency functions for endpoint protection.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from jose import JWTError, jwt
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Configure JWT
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change_me_in_production")
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60  # 60 minutes

# Configure password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Configure OAuth2 with JWT
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/users/login")

# Get current user from environment
CURRENT_USER = os.getenv("CURRENT_USER", "V1997")


class Token(BaseModel):
    """Token response model"""
    access_token: str
    token_type: str


class User(BaseModel):
    """User model for authentication purposes"""
    username: str
    email: str
    full_name: Optional[str] = None
    disabled: bool = False
    roles: List[str] = []
    created_at: str = "2025-03-06 06:14:34"
    last_login: Optional[str] = None


def verify_password(plain_password, hashed_password):
    """Verify a password against a hash"""
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password):
    """Hash a password for storing"""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: timedelta = None):
    """Create a JWT access token"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
        
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Get the current authenticated user from token"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
            
        # Import here to avoid circular imports
        from app.api.services.user_service import get_user_by_username
        
        user_data = get_user_by_username(username)
        if user_data is None:
            raise credentials_exception
            
        return User(**user_data)
        
    except JWTError:
        raise credentials_exception


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    """Get current active user (not disabled)"""
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


async def admin_required(current_user: User = Depends(get_current_active_user)):
    """Dependency for admin-only routes"""
    if "admin" not in current_user.roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    return current_user


async def operator_required(current_user: User = Depends(get_current_active_user)):
    """Dependency for operator routes"""
    if not any(role in current_user.roles for role in ["admin", "operator"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    return current_user


async def viewer_required(current_user: User = Depends(get_current_active_user)):
    """Dependency for viewer routes"""
    # All active users have at least viewer permissions
    return current_user


def get_test_token(test_user=None):
    """Generate a token for testing purposes"""
    if test_user is None:
        # Use environment variable or default
        test_user = CURRENT_USER
    
    # Create a token valid for 1 day
    access_token_expires = timedelta(days=1)
    
    # Import here to avoid circular imports
    from app.api.services.user_service import get_user_by_username
    
    user_data = get_user_by_username(test_user)
    if not user_data:
        raise ValueError(f"Test user {test_user} not found")
    
    return create_access_token(
        data={"sub": test_user, "roles": user_data["roles"]},
        expires_delta=access_token_expires
    )