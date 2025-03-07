"""
API router for user management and authentication

Provides endpoints for user authentication, registration, and management.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import List, Optional
import logging
from datetime import datetime, timedelta
 
# Import models and services
from app.api.models.user import (
    UserCreate,
    UserResponse,
    UserUpdate,
    UserList,
    UserRole,
    ChangePasswordRequest
)
from app.api.auth.auth_handler import (
    User,
    Token,
    admin_required,
    get_current_active_user,
    create_access_token,
    hash_password,
    verify_password
)
from app.api.services.user_service import (
    create_user,
    get_user_by_username,
    update_user,
    delete_user,
    list_users,
    authenticate_user,
    change_user_password
)

# Create router
router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "/login", 
    response_model=Token,
    summary="Login",
    description="Authenticate user and return access token"
)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends()
):
    """
    Authenticate user and return access token
    
    Args:
        form_data: Form with username and password
    
    Returns:
        Token: JWT access token
    """
    # Authenticate user
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Generate access token
    access_token_expires = timedelta(minutes=60)  # 60 minutes
    access_token = create_access_token(
        data={"sub": user["username"], "roles": user["roles"]},
        expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}


@router.get(
    "/me", 
    response_model=UserResponse,
    summary="Get current user",
    description="Get details of the currently authenticated user"
)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """
    Get details of the currently authenticated user
    
    Args:
        current_user: Currently authenticated user
    
    Returns:
        UserResponse: User details
    """
    return current_user


@router.patch(
    "/me",
    response_model=UserResponse,
    summary="Update my profile",
    description="Update current user's profile information"
)
async def update_me(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user)
):
    """
    Update current user's profile information
    
    Args:
        user_update: User information to update
        current_user: Currently authenticated user
    
    Returns:
        UserResponse: Updated user details
    """
    try:
        # Do not allow role updates via this endpoint
        if hasattr(user_update, "roles"):
            delattr(user_update, "roles")
        
        # Update user
        updated_user = update_user(
            username=current_user.username,
            user_data=user_update,
            by_admin=False
        )
        
        return updated_user
        
    except Exception as e:
        logger.error(f"Error updating user profile: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating user profile: {str(e)}"
        )


@router.post(
    "/me/change-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Change password",
    description="Change current user's password"
)
async def change_password(
    password_change: ChangePasswordRequest,
    current_user: User = Depends(get_current_active_user)
):
    """
    Change current user's password
    
    Args:
        password_change: Current and new password
        current_user: Currently authenticated user
    
    Returns:
        None
    """
    try:
        # Verify current password
        user = authenticate_user(current_user.username, password_change.current_password)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is incorrect",
            )
        
        # Change password
        change_user_password(
            username=current_user.username,
            new_password=password_change.new_password
        )
        
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error changing password: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error changing password: {str(e)}"
        )


@router.post(
    "/", 
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create user",
    description="Create a new user (admin only)"
)
async def create_new_user(
    user_create: UserCreate,
    current_user: User = Depends(admin_required)
):
    """
    Create a new user (admin only)
    
    Args:
        user_create: User data for creation
        current_user: Currently authenticated admin user
    
    Returns:
        UserResponse: Created user details
    """
    try:
        # Check if username already exists
        existing_user = get_user_by_username(user_create.username)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"User with username '{user_create.username}' already exists"
            )
        
        # Create user
        new_user = create_user(user_data=user_create)
        
        return new_user
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating user: {str(e)}"
        )


@router.get(
    "/", 
    response_model=UserList,
    summary="List users",
    description="List all users (admin only)"
)
async def list_all_users(
    current_user: User = Depends(admin_required)
):
    """
    List all users (admin only)
    
    Args:
        current_user: Currently authenticated admin user
    
    Returns:
        UserList: List of users
    """
    try:
        users = list_users()
        return {"users": users, "total_count": len(users)}
        
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing users: {str(e)}"
        )


@router.get(
    "/{username}", 
    response_model=UserResponse,
    summary="Get user",
    description="Get user by username (admin only)"
)
async def get_user(
    username: str,
    current_user: User = Depends(admin_required)
):
    """
    Get user by username (admin only)
    
    Args:
        username: Username to retrieve
        current_user: Currently authenticated admin user
    
    Returns:
        UserResponse: User details
    """
    try:
        user = get_user_by_username(username)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User '{username}' not found"
            )
        
        return user
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving user: {str(e)}"
        )


@router.patch(
    "/{username}", 
    response_model=UserResponse,
    summary="Update user",
    description="Update user information (admin only)"
)
async def update_user_admin(
    username: str,
    user_update: UserUpdate,
    current_user: User = Depends(admin_required)
):
    """
    Update user information (admin only)
    
    Args:
        username: Username to update
        user_update: User data to update
        current_user: Currently authenticated admin user
    
    Returns:
        UserResponse: Updated user details
    """
    try:
        # Check if user exists
        existing_user = get_user_by_username(username)
        if not existing_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User '{username}' not found"
            )
        
        # Update user
        updated_user = update_user(
            username=username,
            user_data=user_update,
            by_admin=True
        )
        
        return updated_user
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating user: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating user: {str(e)}"
        )


@router.delete(
    "/{username}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete user",
    description="Delete a user (admin only)"
)
async def delete_user_by_username(
    username: str,
    current_user: User = Depends(admin_required)
):
    """
    Delete a user (admin only)
    
    Args:
        username: Username to delete
        current_user: Currently authenticated admin user
    
    Returns:
        None
    """
    try:
        # Prevent self-deletion
        if username == current_user.username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete your own account"
            )
        
        # Check if user exists
        existing_user = get_user_by_username(username)
        if not existing_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User '{username}' not found"
            )
        
        # Delete user
        success = delete_user(username)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete user '{username}'"
            )
        
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting user: {str(e)}"
        )