"""
Tests for health check API endpoints
"""

import pytest
from fastapi.testclient import TestClient

from api.main

client = TestClient(app)


def test_health_check():
    """Test health check endpoint"""
    response = client.get("/api/health")
    assert response.status_code == 200
    assert "status" in response.json()
    assert "components" in response.json()
    assert "system" in response.json()
    
    # Check response structure
    health_data = response.json()
    assert health_data["status"] in ["ok", "degraded", "error"]
    assert "database" in health_data["components"]


def test_readiness_check():
    """Test readiness probe endpoint"""
    response = client.get("/api/readiness")
    assert response.status_code in [200, 503]  # Could be 503 if service not ready
    
    if response.status_code == 200:
        assert response.json()["status"] == "ready"


def test_liveness_check():
    """Test liveness probe endpoint"""
    response = client.get("/api/liveness")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"