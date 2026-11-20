import json
import uuid

import redis
from pymongo import MongoClient
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import settings
from app.services.document_service import generate_content_hash
from app.services.cache_service import get_cache_key


def test_document_uses_cached_result():
    user_id = f"cache_test_{uuid.uuid4().hex}"

    content = f"Unique cached content {uuid.uuid4().hex}"

    summary = "This is a cached document summary"
    tags = ["python", "fastapi", "mongodb"]

    content_hash = generate_content_hash(content)
    cache_key = get_cache_key(content_hash)

    redis_client = redis.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
    )

    mongo_client = MongoClient(settings.MONGODB_URL)

    document_id = None

    try:
        # Step 1: Prepare an existing cache entry.
        redis_client.set(
            cache_key,
            json.dumps({
                "summary": summary,
                "tags": tags,
            }),
            ex=settings.CACHE_TTL,
        )

        # Step 2: Submit document through API.
        with TestClient(app) as client:

            response = client.post(
                "/documents",
                headers={"X-User-ID": user_id},
                json={
                    "user_id": user_id,
                    "title": "Cached Document Test",
                    "content": content,
                },
            )

            assert response.status_code == 201

            created = response.json()

            document_id = created["document_id"]

            # Cache hit should immediately complete the document.
            assert created["status"] == "completed"
            assert created["content_version"] == 1

            # Step 3: Verify document through GET API.
            get_response = client.get(
                f"/documents/{document_id}",
                headers={"X-User-ID": user_id},
            )

            assert get_response.status_code == 200

            document = get_response.json()

            assert document["status"] == "completed"
            assert document["published_version"] == 1
            assert document["is_current"] is True

            assert document["processing"]["summary"] == summary
            assert document["processing"]["attempts"] == 0

            assert document["enriching"]["tags"] == tags
            assert document["enriching"]["attempts"] == 0

            assert document["current_result"]["summary"] == summary
            assert document["current_result"]["tags"] == tags

        # Step 4: Cache hit should not reserve a worker slot.
        reservation_key = (
            f"cohort:reservation:{user_id}:{document_id}"
        )

        assert redis_client.exists(reservation_key) == 0

    finally:
        # Cleanup test data.
        db = mongo_client[settings.MONGODB_DATABASE]

        if document_id:
            db["documents"].delete_one({
                "_id": __import__("bson").ObjectId(document_id)
            })

        redis_client.delete(cache_key)
        redis_client.delete(f"cohort:active:{user_id}")

        redis_client.close()
        mongo_client.close()