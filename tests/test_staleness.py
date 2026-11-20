import uuid

import pytest

from app.db.mongodb import (
    connect_mongodb,
    close_mongodb,
    get_database,
)
from app.db.redis import (
    connect_redis,
    close_redis,
    get_redis,
)

from app.schemas.document import DocumentCreate, DocumentUpdate

from app.services.document_service import (
    create_document,
    update_document,
)

from app.services.rate_limit_service import release_job
from app.services.queue_service import STREAM_NAME

from app.worker.worker import (
    update_stage,
    mark_document_failed,
)


@pytest.mark.asyncio
async def test_old_version_worker_update_is_rejected():

    user_id = f"staleness_test_{uuid.uuid4().hex}"

    document_id = None
    mongo_connected = False
    redis_connected = False

    try:
        # Step 1: Connect MongoDB and Redis
        await connect_mongodb()
        mongo_connected = True

        await connect_redis()
        redis_connected = True

        db = get_database()
        documents = db["documents"]

        # Step 2: Create Version 1
        created = await create_document(
            DocumentCreate(
                user_id=user_id,
                title="Staleness Test",
                content=f"Original content {uuid.uuid4().hex}",
            )
        )

        document_id = created["document_id"]

        assert created["content_version"] == 1

        # Step 3: Update document to Version 2
        await update_document(
            document_id=document_id,
            user_id=user_id,
            data=DocumentUpdate(
                expected_version=1,
                content=f"Updated content {uuid.uuid4().hex}",
            ),
        )

        latest = await documents.find_one({
            "_id": __import__(
                "bson"
            ).ObjectId(document_id)
        })

        assert latest["content_version"] == 2
        assert latest["published_version"] is None
        assert latest["status"] == "queued"

        # Step 4: Simulate old Worker completing Version 1.
        stale_stage_data = {
            "status": "completed",
            "attempts": 1,
            "summary": "Old version summary",
            "error": None,
        }

        update_result = await update_stage(
            document_id=document_id,
            version=1,
            stage="processing",
            stage_data=stale_stage_data,
            document_status="processing",
        )

        # Old version must not be updated.
        assert update_result is False

        # Step 5: Simulate old Worker reporting failure.
        await mark_document_failed(
            document_id=document_id,
            version=1,
            stage="processing",
            error_message="Old version failure",
            attempts=3,
        )

        # Step 6: Verify MongoDB state.
        latest = await documents.find_one({
            "_id": __import__(
                "bson"
            ).ObjectId(document_id)
        })

        assert latest["content_version"] == 2
        assert latest["published_version"] is None
        assert latest["status"] == "queued"

        # Old version must remain unchanged.
        old_version = latest["versions"]["1"]

        assert old_version["processing"]["status"] == "queued"
        assert old_version["processing"]["summary"] is None
        assert old_version["processing"]["error"] is None

        # Current version must remain queued.
        current_version = latest["versions"]["2"]

        assert current_version["processing"]["status"] == "queued"
        assert current_version["enriching"]["status"] == "queued"

    finally:
        # Cleanup test data and Redis reservations.
        if mongo_connected and redis_connected:
            db = get_database()
            redis_client = get_redis()

            if document_id:

                messages = await redis_client.xrange(
                    STREAM_NAME,
                    min="-",
                    max="+",
                )

                for message_id, message in messages:
                    if message.get("document_id") == document_id:
                        await redis_client.xdel(
                            STREAM_NAME,
                            message_id,
                        )

                await release_job(
                    user_id=user_id,
                    document_id=document_id,
                )

                await db["documents"].delete_one({
                    "_id": __import__(
                        "bson"
                    ).ObjectId(document_id)
                })

            await close_redis()

        if mongo_connected:
            await close_mongodb()