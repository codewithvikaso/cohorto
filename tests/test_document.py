import uuid

import pytest
import redis
from pymongo import MongoClient
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import settings
from app.services.queue_service import STREAM_NAME
import redis

from app.core.config import settings
from bson import ObjectId
from datetime import datetime, timezone
def cleanup_test_data(user_id: str):
    """
    Remove test documents, reservations and queued messages.
    """

    mongo_client = MongoClient(settings.MONGODB_URL)
    redis_client = redis.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
    )

    try:
        db = mongo_client[settings.MONGODB_DATABASE]
        documents = db["documents"]

        existing_docs = list(
            documents.find(
                {"user_id": user_id},
                {"_id": 1},
            )
        )

        document_ids = {
            str(doc["_id"])
            for doc in existing_docs
        }

        # Remove test messages from Redis Stream.
        if document_ids:
            messages = redis_client.xrange(
                STREAM_NAME,
                min="-",
                max="+",
            )

            for message_id, message in messages:
                if message.get("document_id") in document_ids:
                    redis_client.xdel(STREAM_NAME, message_id)

        # Remove reservations.
        for document_id in document_ids:
            reservation_key = (
                f"cohort:reservation:{user_id}:{document_id}"
            )
            redis_client.delete(reservation_key)

        # Delete MongoDB test documents.
        documents.delete_many({"user_id": user_id})

        # Remove active counter.
        redis_client.delete(f"cohort:active:{user_id}")

    finally:
        redis_client.close()
        mongo_client.close()


@pytest.fixture
def test_environment():
    """
    Start FastAPI application and generate an isolated test user.
    """

    user_id = f"pytest_user_{uuid.uuid4().hex}"

    with TestClient(app) as client:
        yield client, user_id

    cleanup_test_data(user_id)


def test_create_get_and_document_ownership(test_environment):
    client, user_id = test_environment

    headers = {
        "X-User-ID": user_id
    }

    payload = {
        "user_id": user_id,
        "title": "Pytest Document",
        "content": (
            f"Unique integration test content {uuid.uuid4().hex}"
        ),
    }

    # 1. Create document
    response = client.post(
        "/documents",
        json=payload,
        headers=headers,
    )

    assert response.status_code == 201

    created_document = response.json()

    document_id = created_document["document_id"]

    assert created_document["status"] == "queued"
    assert created_document["content_version"] == 1

    # 2. Get created document
    get_response = client.get(
        f"/documents/{document_id}",
        headers=headers,
    )

    assert get_response.status_code == 200

    document = get_response.json()

    assert document["document_id"] == document_id
    assert document["user_id"] == user_id
    assert document["content_version"] == 1
    assert document["status"] == "queued"

    # 3. Another user should not access this document.
    other_user_headers = {
        "X-User-ID": "another_test_user"
    }

    unauthorized_response = client.get(
        f"/documents/{document_id}",
        headers=other_user_headers,
    )

    assert unauthorized_response.status_code == 404

    # 4. Invalid document ID
    invalid_response = client.get(
        "/documents/invalid_document_id",
        headers=headers,
    )

    assert invalid_response.status_code == 404


def test_create_document_for_another_user_is_rejected(
    test_environment,
):
    client, authenticated_user = test_environment

    headers = {
        "X-User-ID": authenticated_user
    }

    payload = {
        "user_id": "different_user",
        "title": "Unauthorized Document",
        "content": "Testing user ownership validation",
    }

    response = client.post(
        "/documents",
        json=payload,
        headers=headers,
    )

    assert response.status_code == 403

    assert response.json()["detail"] == (
        "You cannot create document for another user"
    )
    
    
def test_patch_document_and_optimistic_version_conflict(
    test_environment,
):
    client, user_id = test_environment

    headers = {
        "X-User-ID": user_id
    }

    # Step 1: Create document
    create_payload = {
        "user_id": user_id,
        "title": "PATCH Integration Test",
        "content": "Original content for PATCH testing",
    }

    create_response = client.post(
        "/documents",
        json=create_payload,
        headers=headers,
    )

    assert create_response.status_code == 201

    document_id = create_response.json()["document_id"]

    # Step 2: Update document using expected_version = 1
    patch_payload = {
        "expected_version": 1,
        "content": "Updated content for PATCH version 2",
    }

    patch_response = client.patch(
        f"/documents/{document_id}",
        json=patch_payload,
        headers=headers,
    )

    assert patch_response.status_code == 200

    updated_document = patch_response.json()

    assert updated_document["document_id"] == document_id
    assert updated_document["content_version"] == 2
    assert updated_document["published_version"] is None
    assert updated_document["status"] == "queued"
    assert updated_document["is_current"] is False

    # Step 3: Try updating with an old version.
    stale_patch_payload = {
        "expected_version": 1,
        "content": "Trying to update using an outdated version",
    }

    stale_response = client.patch(
        f"/documents/{document_id}",
        json=stale_patch_payload,
        headers=headers,
    )

    assert stale_response.status_code == 409

    # Step 4: Confirm the latest version remains unchanged.
    get_response = client.get(
        f"/documents/{document_id}",
        headers=headers,
    )

    assert get_response.status_code == 200
    assert get_response.json()["content_version"] == 2
    assert get_response.json()["status"] == "queued"
    
def test_post_rate_limit_max_three_active_jobs(test_environment):
    client, user_id = test_environment

    headers = {
        "X-User-ID": user_id
    }

    response_codes = []

    # Create 4 documents for the same user.
    for i in range(1, 5):

        payload = {
            "user_id": user_id,
            "title": f"Rate Limit Test Document {i}",
            "content": (
                f"Unique rate limit test content document {i}"
            ),
        }

        response = client.post(
            "/documents",
            json=payload,
            headers=headers,
        )

        response_codes.append(response.status_code)

    # First 3 requests should succeed.
    assert response_codes[:3] == [201, 201, 201]

    # Fourth request should be rejected.
    assert response_codes[3] == 429

    # Verify Redis active counter.
    redis_client = redis.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
    )

    try:
        active_count = redis_client.get(
            f"cohort:active:{user_id}"
        )

        assert int(active_count or 0) == 3

    finally:
        redis_client.close()
        
        
def test_patch_rate_limit_when_three_jobs_active(test_environment):
    client, user_id = test_environment

    headers = {
        "X-User-ID": user_id
    }

    # Step 1: Create 3 active documents.
    for i in range(1, 4):

        payload = {
            "user_id": user_id,
            "title": f"Active Document {i}",
            "content": (
                f"Unique PATCH rate limit content "
                f"{i} {uuid.uuid4().hex}"
            ),
        }

        response = client.post(
            "/documents",
            json=payload,
            headers=headers,
        )

        assert response.status_code == 201

    # Step 2: Insert a previously completed document.
    # This represents a document created and completed earlier.
    mongo_client = MongoClient(settings.MONGODB_URL)

    try:
        db = mongo_client[settings.MONGODB_DATABASE]
        documents = db["documents"]

        completed_document_id = ObjectId()
        now = datetime.now(timezone.utc)

        completed_document = {
            "_id": completed_document_id,
            "user_id": user_id,
            "title": "Previously Completed Document",
            "content": "Original completed content",
            "content_hash": "test_completed_hash",
            "content_version": 1,
            "published_version": 1,
            "status": "completed",
            "versions": {
                "1": {
                    "content": "Original completed content",
                    "content_hash": "test_completed_hash",
                    "processing": {
                        "status": "completed",
                        "attempts": 1,
                        "summary": "Original completed content",
                        "error": None,
                    },
                    "enriching": {
                        "status": "completed",
                        "attempts": 1,
                        "tags": ["completed"],
                        "error": None,
                    },
                }
            },
            "created_at": now,
            "updated_at": now,
        }

        documents.insert_one(completed_document)

        # Step 3: Try to PATCH the completed document.
        # User already has 3 active reservations.
        patch_payload = {
            "expected_version": 1,
            "content": "Updated content for rate limit testing",
        }

        patch_response = client.patch(
            f"/documents/{completed_document_id}",
            json=patch_payload,
            headers=headers,
        )

        assert patch_response.status_code == 429

        assert patch_response.json()["detail"] == (
            "Maximum 3 active documents allowed per user"
        )

        # Step 4: Verify Redis counter is still 3.
        redis_client = redis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
        )

        try:
            active_count = redis_client.get(
                f"cohort:active:{user_id}"
            )

            assert int(active_count or 0) == 3

        finally:
            redis_client.close()

        # Step 5: Verify the completed document was not modified.
        unchanged_document = documents.find_one({
            "_id": completed_document_id
        })

        assert unchanged_document["content_version"] == 1
        assert unchanged_document["status"] == "completed"

    finally:
        mongo_client.close()