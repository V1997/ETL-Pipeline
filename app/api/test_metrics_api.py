"""
Tests for metrics API endpoints
"""

import pytest
from fastapi.testclient import TestClient
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


def test_get_system_metrics(viewer_token):
    """Test getting system metrics"""
    response = client.get(
        "/api/metrics/system",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert "metrics" in response.json()
    assert "system" in response.json()["metrics"]
    assert "database" in response.json()["metrics"]


def test_get_batch_stats(viewer_token):
    """Test getting batch statistics"""
    response = client.get(
        "/api/metrics/batches/stats",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert "metric_name" in response.json()[0]
    assert "data_points" in response.json()[0]


def test_get_batch_stats_with_grouping(viewer_token):
    """Test getting batch statistics with different groupings"""
    for group_by in ["hour", "day", "week", "month"]:
        response = client.get(
            f"/api/metrics/batches/stats?group_by={group_by}",
            headers=get_auth_headers(viewer_token)
        )
        assert response.status_code == 200


def test_get_batch_stats_with_date_range(viewer_token):
    """Test getting batch statistics with date range"""
    response = client.get(
        "/api/metrics/batches/stats?from_date=2025-01-01&to_date=2025-03-06",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200


def test_get_data_quality_metrics(viewer_token):
    """Test getting data quality metrics"""
    response = client.get(
        "/api/metrics/quality",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    
    # Even if no metrics yet, should return valid (possibly empty) response
    assert isinstance(response.json(), list)


def test_get_data_quality_metrics_with_source(viewer_token):
    """Test getting data quality metrics filtered by source"""
    response = client.get(
        "/api/metrics/quality?source=test-source",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200


def test_get_performance_metrics(viewer_token):
    """Test getting performance metrics"""
    response = client.get(
        "/api/metrics/performance",
        headers=get_auth_headers(viewer_token)
    )
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert "metric_name" in response.json()[0]
    assert "data_points" in response.json()[0]


def test_get_performance_metrics_with_type(viewer_token):
    """Test getting performance metrics with specific type"""
    for metric_type in ["processing_time", "records_per_second", "error_rate"]:
        response = client.get(
            f"/api/metrics/performance?metric_type={metric_type}",
            headers=get_auth_headers(viewer_token)
        )
        assert response.status_code == 200
        assert response.json()[0]["metric_name"] == metric_type


def test_get_performance_metrics_with_aggregation(viewer_token):
    """Test getting performance metrics with different aggregations"""
    for aggregation in ["avg", "max", "min", "sum"]:
        response = client.get(
            f"/api/metrics/performance?aggregation={aggregation}",
            headers=get_auth_headers(viewer_token)
        )
        assert response.status_code == 200
        assert aggregation in response.json()[0]["aggregation"]