import pytest
from fastapi.testclient import TestClient

import app.main as main


def test_health_check_success():
    """Both MongoDB and Redis are healthy."""

    with TestClient(main.app) as client:
        response = client.get("/health")

        assert response.status_code == 200

        data = response.json()

        assert data["status"] == "healthy"
        assert data["checks"]["mongodb"] == "ok"
        assert data["checks"]["redis"] == "ok"


def test_health_check_mongodb_failure(monkeypatch):
    """Health check should fail when MongoDB is unavailable."""

    class BrokenMongoDB:
        async def command(self, command):
            raise RuntimeError("MongoDB unavailable")

    class HealthyRedis:
        async def ping(self):
            return True

    monkeypatch.setattr(
        main,
        "get_database",
        lambda: BrokenMongoDB(),
    )

    monkeypatch.setattr(
        main,
        "get_redis",
        lambda: HealthyRedis(),
    )

    with TestClient(main.app) as client:
        response = client.get("/health")

        assert response.status_code == 503

        data = response.json()

        assert data["status"] == "unhealthy"
        assert data["checks"]["mongodb"] == "error"
        assert data["checks"]["redis"] == "ok"


def test_health_check_redis_failure(monkeypatch):
    """Health check should fail when Redis is unavailable."""

    class HealthyMongoDB:
        async def command(self, command):
            return {"ok": 1}

    class BrokenRedis:
        async def ping(self):
            raise RuntimeError("Redis unavailable")

    monkeypatch.setattr(
        main,
        "get_database",
        lambda: HealthyMongoDB(),
    )

    monkeypatch.setattr(
        main,
        "get_redis",
        lambda: BrokenRedis(),
    )

    with TestClient(main.app) as client:
        response = client.get("/health")

        assert response.status_code == 503

        data = response.json()

        assert data["status"] == "unhealthy"
        assert data["checks"]["mongodb"] == "ok"
        assert data["checks"]["redis"] == "error"