"""
API data models for user management
"""

from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional
from enum import Enum

class UserRole(str, Enum):
    """User roles in the system"""
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"


class UserBase(BaseModel):
    """Base user model with common attributes"""
    username: str = Field(..., description="Unique username")
    email: EmailStr = Field(..., description="User email address")
    full_name: Optional[str] = Field(None, description="Full name of the user")
    disabled: bool = Field(False, description="Whether the user is disabled")


class UserCreate(UserBase):
    """Model for user creation"""
    password: str = Field(..., description="User password", min_length=8)
    roles: List[UserRole] = Field([UserRole.VIEWER], description="User roles")


class UserUpdate(BaseModel):
    """Model for user updates (all fields optional)"""
    email: Optional[EmailStr] = Field(None, description="User email address")
    full_name: Optional[str] = Field(None, description="Full name of the user")
    disabled: Optional[bool] = Field(None, description="Whether the user is disabled")
    roles: Optional[List[UserRole]] = Field(None, description="User roles")


class UserResponse(UserBase):
    """Model for user response"""
    roles: List[UserRole] = Field(..., description="User roles")
    created_at: str = Field("2025-03-06 05:21:54", description="When the user was created")
    last_login: Optional[str] = Field(None, description="When the user last logged in")

    class Config:
        orm_mode = True


class UserList(BaseModel):
    """Model for user list response"""
    users: List[UserResponse] = Field(..., description="List of users")
    total_count: int = Field(..., description="Total count of users")


class ChangePasswordRequest(BaseModel):
    """Model for password change request"""
    current_password: str = Field(..., description="Current password")
    new_password: str = Field(..., description="New password", min_length=8)