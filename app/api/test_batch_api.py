"""
Tests for batch API endpoints
"""

import pytest
from fastapi.testclient import TestClient
import pandas as pd
import io
from datetime import datetime
import os

from app.api.main import app
from app.api.auth.auth_handler import get_test_token

client = TestClient(app)

# Get current user from environment or use default
CURRENT_USER = os.getenv("CURRENT_USER", "V1997")
CURRENT_TIME = "2025-03-06 06:17:01"  # Current UTC time


def get_auth_headers(token):
    """Helper to create auth headers"""
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def admin_token():
    """Get admin auth token"""
    # For tests, we'll use the current user which has admin rights
    return get_test_token(CURRENT_USER)


@pytest.fixture
def operator_token():
    """Get operator auth token"""
    # Use actual operator role, or the current user if it has operator rights
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


@pytest.fixture
def sample_csv():
    """Create sample CSV data"""
    data = {
        "order_id": ["ABC-123", "ABC-124", "ABC-125"],
        "region": ["North America", "Europe", "Asia"],
        "country": ["USA", "Germany", "Japan"],
        "item_type": ["Electronics", "Furniture", "Clothing"],
        "sales_channel": ["Online", "Offline", "Online"],
        "order_priority": ["H", "M", "L"],
        "order_date": ["2025-01-01", "2025-01-02", "2025-01-03"],
        "ship_date": ["2025-01-05", "2025-01-06", "2025-01-07"],
        "units_sold": [100, 50, 75],
        "unit_price": [10.99, 200.50, 25.75],
        "unit_cost": [8.00, 150.00, 15.00],
        "total_revenue": [1099.00, 10025.00, 1931.25],
        "total_cost": [800.00, 7500.00, 1125.00],
        "total_profit": [299.00, 2525.00, 806.25]
    }
    df = pd.DataFrame(data)
    csv_data = io.StringIO()
    df.to_csv(csv_data, index=False)
    return csv_data.getvalue()


def test_create_batch_unauthorized():
    """Test creating batch without auth"""
    files = {"file": ("data.csv", "dummy,data", "text/csv")}
    response = client.post(
        "/api/batches/",
        params={"source": "test-source"},
        files=files
    )
    assert response.status_code == 401


def test_create_batch_as_viewer(viewer_token, sample_csv):
    """Test creating batch with viewer role"""
    files = {"file": ("data.csv", sample_csv, "text/csv")}
    response = client.post(
        "/api/batches/",
        params={"source": "test-source"},
        files=files,
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 403


def test_create_batch_as_operator(operator_token, sample_csv):
    """Test creating batch with operator role"""
    files = {"file": ("data.csv", sample_csv, "text/csv")}
    response = client.post(
        "/api/batches/",
        params={"source": "test-source"},
        files=files,
        headers=get_auth_headers(operator_token)
    )
    assert response.status_code == 201
    assert "batch_id" in response.json()
    assert response.json()["source"] == "test-source"
    assert response.json()["status"] in ["pending", "processing"]
    assert response.json()["created_by"] in ["operator", CURRENT_USER]
    return response.json()["batch_id"]


def test_list_batches(viewer_token):
    """Test listing batches"""
    response = client.get(
        "/api/batches/",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert "batches" in response.json()


def test_filter_batches_by_status(viewer_token):
    """Test filtering batches by status"""
    response = client.get(
        "/api/batches/?status=completed",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert "batches" in response.json()
    for batch in response.json()["batches"]:
        assert batch["status"] == "completed"


def test_get_batch_by_id(admin_token, sample_csv):
    """Test getting a batch by ID"""
    # First create a batch using the current user (admin)
    files = {"file": ("data.csv", sample_csv, "text/csv")}
    create_response = client.post(
        "/api/batches/",
        params={"source": "test-source"},
        files=files,
        headers=get_auth_headers(admin_token)
    )
    assert create_response.status_code == 201
    batch_id = create_response.json()["batch_id"]
    
    # Now get the batch
    response = client.get(
        f"/api/batches/{batch_id}",
        headers=get_auth_headers(admin_token)
    )
    assert response.status_code == 200
    assert response.json()["batch_id"] == batch_id
    assert "metrics" in response.json()
    assert "errors" in response.json()
    assert response.json()["created_by"] == CURRENT_USER


def test_retry_batch(admin_token, sample_csv):
    """Test retrying a batch"""
    # First create a batch - in a real test we'd create one that's failed
    files = {"file": ("data.csv", sample_csv, "text/csv")}
    create_response = client.post(
        "/api/batches/",
        params={"source": "test-source"},
        files=files,
        headers=get_auth_headers(admin_token)
    )
    assert create_response.status_code == 201
    batch_id = create_response.json()["batch_id"]
    
    # In a real test, we'd wait for it to fail, but we'll skip that
    # For now, we'll just try to retry it and expect failure since it's not failed yet
    retry_files = {"file": ("retry_data.csv", sample_csv, "text/csv")}
    retry_response = client.post(
        f"/api/batches/{batch_id}/retry",
        files=retry_files,
        headers=get_auth_headers(admin_token)
    )
    # Should fail since batch isn't in failed state
    assert retry_response.status_code == 400


def test_cancel_batch(admin_token, operator_token, sample_csv):
    """Test cancelling a batch"""
    # First create a batch as the current user (admin)
    files = {"file": ("data.csv", sample_csv, "text/csv")}
    create_response = client.post(
        "/api/batches/",
        params={"source": "test-source"},
        files=files,
        headers=get_auth_headers(admin_token)
    )
    assert create_response.status_code == 201
    batch_id = create_response.json()["batch_id"]
    
    # Try to cancel as operator (should fail)
    cancel_response = client.delete(
        f"/api/batches/{batch_id}",
        headers=get_auth_headers(operator_token)
    )
    assert cancel_response.status_code == 403
    
    # Cancel as admin
    cancel_response = client.delete(
        f"/api/batches/{batch_id}",
        headers=get_auth_headers(admin_token)
    )
    assert cancel_response.status_code == 204
    
    # Verify it's cancelled
    get_response = client.get(
        f"/api/batches/{batch_id}",
        headers=get_auth_headers(admin_token)
    )
    assert get_response.status_code == 200
    assert get_response.json()["status"] == "cancelled"