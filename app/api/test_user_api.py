"""
Tests for user management API endpoints
"""

import pytest
from fastapi.testclient import TestClient

from app.api.main import app

client = TestClient(app)


def get_auth_headers(token):
    """Helper to create auth headers"""
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def admin_token():
    """Get admin auth token"""
    response = client.post(
        "/api/users/login",
        data={"username": "admin", "password": "admin123"}
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.fixture
def operator_token():
    """Get operator auth token"""
    response = client.post(
        "/api/users/login",
        data={"username": "operator", "password": "operator123"}
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.fixture
def viewer_token():
    """Get viewer auth token"""
    response = client.post(
        "/api/users/login",
        data={"username": "viewer", "password": "viewer123"}
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def test_login_success():
    """Test successful login"""
    response = client.post(
        "/api/users/login",
        data={"username": "admin", "password": "admin123"}
    )
    assert response.status_code == 200
    assert "access_token" in response.json()
    assert response.json()["token_type"].lower() == "bearer"


def test_login_invalid_credentials():
    """Test login with invalid credentials"""
    response = client.post(
        "/api/users/login",
        data={"username": "admin", "password": "wrong_password"}
    )
    assert response.status_code == 401


def test_get_current_user(admin_token):
    """Test getting current user info"""
    response = client.get(
        "/api/users/me",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 200
    assert response.json()["username"] == "admin"
    assert "admin" in response.json()["roles"]


def test_update_current_user(operator_token):
    """Test updating current user info"""
    update_data = {
        "full_name": "Updated ETL Operator",
        "email": "operator@example.com"
    }
    response = client.patch(
        "/api/users/me",
        json=update_data,
        headers=get_auth_headers(operator_token)
    )
    assert response.status_code == 200
    assert response.json()["full_name"] == "Updated ETL Operator"


def test_change_password(viewer_token):
    """Test changing password"""
    change_data = {
        "current_password": "viewer123",
        "new_password": "newviewer123"
    }
    response = client.post(
        "/api/users/me/change-password",
        json=change_data,
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 204
    
    # Test login with new password
    response = client.post(
        "/api/users/login",
        data={"username": "viewer", "password": "newviewer123"}
    )
    assert response.status_code == 200


def test_change_password_wrong_current(viewer_token):
    """Test changing password with wrong current password"""
    change_data = {
        "current_password": "wrong_password",
        "new_password": "newviewer123"
    }
    response = client.post(
        "/api/users/me/change-password",
        json=change_data,
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 400


def test_create_user_as_admin(admin_token):
    """Test creating a user as admin"""
    user_data = {
        "username": "testuser",
        "email": "test@example.com",
        "full_name": "Test User",
        "password": "testuser123",
        "roles": ["viewer"]
    }
    response = client.post(
        "/api/users/",
        json=user_data,
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 201
    assert response.json()["username"] == "testuser"
    assert response.json()["roles"] == ["viewer"]


def test_create_user_unauthorized(viewer_token):
    """Test creating a user without admin rights"""
    user_data = {
        "username": "testuser2",
        "email": "test2@example.com",
        "full_name": "Test User 2",
        "password": "testuser123",
        "roles": ["viewer"]
    }
    response = client.post(
        "/api/users/",
        json=user_data,
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 403


def test_list_users_as_admin(admin_token):
    """Test listing users as admin"""
    response = client.get(
        "/api/users/",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 200
    assert "users" in response.json()
    assert len(response.json()["users"]) > 0


def test_list_users_unauthorized(viewer_token):
    """Test listing users without admin rights"""
    response = client.get(
        "/api/users/",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 403


def test_get_specific_user(admin_token):
    """Test getting a specific user as admin"""
    response = client.get(
        "/api/users/operator",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 200
    assert response.json()["username"] == "operator"


def test_update_user_as_admin(admin_token):
    """Test updating a user as admin"""
    update_data = {
        "roles": ["operator", "viewer", "admin"],
        "full_name": "Updated Viewer"
    }
    response = client.patch(
        "/api/users/viewer",
        json=update_data,
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 200
    assert response.json()["full_name"] == "Updated Viewer"
    assert set(response.json()["roles"]) == set(["operator", "viewer", "admin"])


def test_delete_user_as_admin(admin_token):
    """Test deleting a user as admin"""
    # First create a user to delete
    user_data = {
        "username": "user_to_delete",
        "email": "delete@example.com",
        "full_name": "User To Delete",
        "password": "delete123",
        "roles": ["viewer"]
    }
    response = client.post(
        "/api/users/",
        json=user_data,
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 201
    
    # Now delete the user
    response = client.delete(
        "/api/users/user_to_delete",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 204
    
    # Verify it's deleted
    response = client.get(
        "/api/users/user_to_delete",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 404


def test_prevent_self_deletion(admin_token):
    """Test that a user cannot delete themselves"""
    # Try to delete own account
    response = client.delete(
        "/api/users/admin",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 400