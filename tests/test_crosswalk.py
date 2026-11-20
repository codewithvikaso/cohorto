import uuid

import pytest
import redis

from pymongo import MongoClient
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import settings
from app.services.queue_service import STREAM_NAME


def cleanup_crosswalk_data(user_id: str):
    """Clean test documents and Redis reservations."""

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
            str(doc["_id"]) for doc in existing_docs
        }

        messages = redis_client.xrange(
            STREAM_NAME,
            min="-",
            max="+",
        )

        for message_id, message in messages:
            if message.get("document_id") in document_ids:
                redis_client.xdel(STREAM_NAME, message_id)

        for document_id in document_ids:
            redis_client.delete(
                f"cohort:reservation:{user_id}:{document_id}"
            )

        documents.delete_many({"user_id": user_id})

        redis_client.delete(f"cohort:active:{user_id}")

    finally:
        redis_client.close()
        mongo_client.close()


@pytest.fixture
def crosswalk_environment():

    user_id = f"crosswalk_user_{uuid.uuid4().hex}"

    with TestClient(app) as client:
        yield client, user_id

    cleanup_crosswalk_data(user_id)


def test_same_ref_same_content_returns_existing_document(
    crosswalk_environment,
):
    client, user_id = crosswalk_environment

    headers = {"X-User-ID": user_id}

    client_doc_ref = f"INV-{uuid.uuid4().hex}"

    content = (
        f"Invoice amount is 50000 {uuid.uuid4().hex}"
    )

    payload = {
        "user_id": user_id,
        "title": "Crosswalk Test",
        "content": content,
        "client_doc_ref": client_doc_ref,
    }

    # First request
    first_response = client.post(
        "/documents",
        json=payload,
        headers=headers,
    )

    assert first_response.status_code == 201

    first_document = first_response.json()

    # Second request with same reference and same content
    second_response = client.post(
        "/documents",
        json=payload,
        headers=headers,
    )

    assert second_response.status_code == 201

    second_document = second_response.json()

    # Both requests should return the same document.
    assert (
        first_document["document_id"]
        == second_document["document_id"]
    )

    assert second_document["content_version"] == 1

    # Verify only one document exists.
    mongo_client = MongoClient(settings.MONGODB_URL)

    try:
        db = mongo_client[settings.MONGODB_DATABASE]

        count = db["documents"].count_documents({
            "user_id": user_id,
            "client_doc_ref": client_doc_ref,
        })

        assert count == 1

    finally:
        mongo_client.close()


def test_same_ref_different_content_updates_existing_document(
    crosswalk_environment,
):
    client, user_id = crosswalk_environment

    headers = {"X-User-ID": user_id}

    client_doc_ref = f"INV-{uuid.uuid4().hex}"

    first_payload = {
        "user_id": user_id,
        "title": "Original Invoice",
        "content": f"Invoice amount is 50000 {uuid.uuid4().hex}",
        "client_doc_ref": client_doc_ref,
    }

    # Create first document.
    first_response = client.post(
        "/documents",
        json=first_payload,
        headers=headers,
    )

    assert first_response.status_code == 201

    first_document = first_response.json()

    # Same client reference, different content.
    second_payload = {
        "user_id": user_id,
        "title": "Updated Invoice",
        "content": f"Invoice amount is 75000 {uuid.uuid4().hex}",
        "client_doc_ref": client_doc_ref,
    }

    second_response = client.post(
        "/documents",
        json=second_payload,
        headers=headers,
    )

    assert second_response.status_code == 201

    second_document = second_response.json()

    # Document ID must remain the same.
    assert (
        first_document["document_id"]
        == second_document["document_id"]
    )

    # Version must increase.
    assert second_document["content_version"] == 2

    # Verify MongoDB state.
    mongo_client = MongoClient(settings.MONGODB_URL)

    try:
        db = mongo_client[settings.MONGODB_DATABASE]

        document = db["documents"].find_one({
            "user_id": user_id,
            "client_doc_ref": client_doc_ref,
        })

        assert document is not None
        assert document["content_version"] == 2
        assert document["content"] == second_payload["content"]
        assert document["status"] == "queued"

    finally:
        mongo_client.close()


def test_same_ref_different_user_returns_409(
    crosswalk_environment,
):
    client, owner_user_id = crosswalk_environment

    other_user_id = f"other_user_{uuid.uuid4().hex}"

    client_doc_ref = f"INV-{uuid.uuid4().hex}"

    # First user creates document.
    first_response = client.post(
        "/documents",
        headers={"X-User-ID": owner_user_id},
        json={
            "user_id": owner_user_id,
            "title": "Owner Invoice",
            "content": "Original invoice content",
            "client_doc_ref": client_doc_ref,
        },
    )

    assert first_response.status_code == 201

    # Second user tries to reuse the same reference.
    second_response = client.post(
        "/documents",
        headers={"X-User-ID": other_user_id},
        json={
            "user_id": other_user_id,
            "title": "Unauthorized Invoice",
            "content": "Different invoice content",
            "client_doc_ref": client_doc_ref,
        },
    )

    assert second_response.status_code == 409

    assert second_response.json()["detail"] == (
        "client_doc_ref is already associated with another user"
    )