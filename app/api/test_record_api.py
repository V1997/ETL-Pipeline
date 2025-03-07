"""
Tests for records API endpoints
"""

import pytest
from fastapi.testclient import TestClient
import pandas as pd
import io
from datetime import datetime

from app.api.main import app

client = TestClient(app)


def get_auth_headers(token):
    """Helper to create auth headers"""
    return {"Authorization": f"Bearer {token}"}


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
def operator_token():
    """Get operator auth token"""
    response = client.post(
        "/api/users/login",
        data={"username": "operator", "password": "operator123"}
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.fixture
def sample_batch_with_records(operator_token):
    """Create a sample batch with records"""
    # Create sample CSV data
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
    
    # Create batch
    files = {"file": ("data.csv", csv_data.getvalue(), "text/csv")}
    response = client.post(
        "/api/batches/",
        params={"source": "test-records"},
        files=files,
        headers=get_auth_headers(operator_token)
    )
    assert response.status_code == 201
    return response.json()["batch_id"]


def test_query_records_unauthorized():
    """Test querying records without auth"""
    response = client.get("/api/records/")
    assert response.status_code == 401


def test_query_records(viewer_token, sample_batch_with_records):
    """Test querying records"""
    response = client.get(
        "/api/records/",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert "records" in response.json()


def test_filter_records_by_region(viewer_token, sample_batch_with_records):
    """Test filtering records by region"""
    response = client.get(
        "/api/records/?region=North%20America",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert "records" in response.json()
    for record in response.json()["records"]:
        assert record["region"] == "North America"


def test_filter_records_by_batch_id(viewer_token, sample_batch_with_records):
    """Test filtering records by batch_id"""
    batch_id = sample_batch_with_records
    response = client.get(
        f"/api/records/?batch_id={batch_id}",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert "records" in response.json()
    for record in response.json()["records"]:
        assert record["batch_id"] == batch_id


def test_export_records_csv(viewer_token, sample_batch_with_records):
    """Test exporting records as CSV"""
    response = client.get(
        "/api/records/export?format=csv",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "text/csv"
    assert "Content-Disposition" in response.headers
    assert "attachment" in response.headers["Content-Disposition"]
    assert ".csv" in response.headers["Content-Disposition"]


def test_export_records_excel(viewer_token, sample_batch_with_records):
    """Test exporting records as Excel"""
    response = client.get(
        "/api/records/export?format=excel",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "Content-Disposition" in response.headers
    assert "attachment" in response.headers["Content-Disposition"]
    assert ".xlsx" in response.headers["Content-Disposition"]


def test_get_record_by_id(viewer_token, sample_batch_with_records):
    """Test getting a record by ID"""
    # First get records to get an ID
    response = client.get(
        "/api/records/",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    records = response.json()["records"]
    assert len(records) > 0
    record_id = records[0]["order_id"]
    
    # Now get the specific record
    response = client.get(
        f"/api/records/{record_id}",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert response.json()["order_id"] == record_id