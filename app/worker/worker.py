import asyncio
import random
import logging
import socket
from datetime import datetime, timezone
from bson import ObjectId
from app.services.rate_limit_service import release_job
from app.db.mongodb import connect_mongodb, close_mongodb, get_database
from app.db.redis import connect_redis, close_redis, get_redis
from app.services.queue_service import STREAM_NAME, GROUP_NAME
from app.services.cache_service import set_cached_result
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONSUMER_NAME =f"worker-{socket.gethostname()}"
MAX_RETRIES = 3


async def create_consumer_group():
    redis_client = get_redis()

    try:
        await redis_client.xgroup_create(
            STREAM_NAME,
            GROUP_NAME,
            id="0",
            mkstream=True,
        )
        logger.info("Redis consumer group created")

    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.info("Consumer group already exists")
        else:
            raise


async def generate_summary(content: str) -> str:
    """
    Mock summary generation.
    Later, replace this with an actual AI/LLM service.
    """

    await asyncio.sleep(random.uniform(10, 20))

    if random.random() < 0.10:
        raise RuntimeError("Simulated summary generation failure")

    words = content.split()

    summary = " ".join(words[:30])

    return summary


async def generate_tags(summary: str) -> list[str]:
    """
    Mock tag generation.
    Later, replace this with an actual AI/LLM service.
    """

    await asyncio.sleep(random.uniform(5, 15))

    if random.random() < 0.10:
        raise RuntimeError("Simulated tag generation failure")

    words = summary.lower().split()

    tags = list(dict.fromkeys(
        word.strip(".,!?;:") for word in words
        if len(word.strip(".,!?;:")) > 3
    ))

    return tags[:5]


async def update_stage(
    document_id: str,
    version: int,
    stage: str,
    stage_data: dict,
    document_status: str,
):
    db = get_database()

    result = await db["documents"].update_one(
        {
            "_id": ObjectId(document_id),
            "content_version": version,
        },
        {
            "$set": {
                f"versions.{version}.{stage}": stage_data,
                "status": document_status,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    return result.modified_count == 1


async def mark_document_failed(
    document_id: str,
    version: int,
    stage: str,
    error_message: str,
    attempts: int,
):
    db = get_database()

    await db["documents"].update_one(
        {
            "_id": ObjectId(document_id),
            "content_version": version,
        },
        {
            "$set": {
                f"versions.{version}.{stage}.status": "failed",
                f"versions.{version}.{stage}.error": error_message,
                f"versions.{version}.{stage}.attempts": attempts,
                "status": "failed",
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )


async def process_stage(
    document_id: str,
    version: int,
    stage: str,
    operation,
    content: str,
):
    """
    Retry only the stage that failed.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        stage_data = {
            "status": "processing",
            "attempts": attempt,
        }

        document_status = (
            "processing" if stage == "processing"
            else "enriching"
        )

        updated = await update_stage(
            document_id,
            version,
            stage,
            stage_data,
            document_status,
        )

        if not updated:
            logger.info(
                "Document version changed. Skipping old job: %s",
                document_id,
            )
            return None

        try:
            result = await operation(content)

            if stage == "processing":
                stage_data = {
                    "status": "completed",
                    "attempts": attempt,
                    "summary": result,
                    "error": None,
                }
            else:
                stage_data = {
                    "status": "completed",
                    "attempts": attempt,
                    "tags": result,
                    "error": None,
                }

            updated = await update_stage(
                document_id,
                version,
                stage,
                stage_data,
                document_status,
            )

            if not updated:
                logger.info(
                    "Version changed while stage was running: %s",
                    document_id,
                )
                return None

            return result

        except Exception as exc:
            logger.exception(
                "%s stage failed. Attempt %s/%s",
                stage,
                attempt,
                MAX_RETRIES,
            )

            if attempt == MAX_RETRIES:
                await mark_document_failed(
                    document_id,
                    version,
                    stage,
                    str(exc),
                    attempt,
                )
                return None

            await asyncio.sleep(2 ** attempt)

    return None


async def process_document(message: dict):
    document_id = message["document_id"]
    version = int(message["version"])

    db = get_database()
    documents = db["documents"]

    # Step 1: Get current document version
    document = await documents.find_one({
        "_id": ObjectId(document_id),
        "content_version": version,
    })

    if document is None:
        logger.info(
            "Document missing or version outdated: %s",
            document_id,
        )
        return "stale"

    version_key = str(version)
    version_data = document["versions"][version_key]

    content = version_data["content"]
    content_hash = version_data["content_hash"]

    # Step 2: Generate summary
    processing = version_data["processing"]

    if processing["status"] != "completed":

        summary = await process_stage(
            document_id=document_id,
            version=version,
            stage="processing",
            operation=generate_summary,
            content=content,
        )

        if summary is None:

            latest = await documents.find_one({
                "_id": ObjectId(document_id),
                "content_version": version,
            })

            if latest is None:
                return "stale"

            if latest["status"] == "failed":
                return "failed"

            return "pending"

    else:
        summary = processing["summary"]

    # Step 3: Check current version again
    latest = await documents.find_one({
        "_id": ObjectId(document_id),
        "content_version": version,
    })

    if latest is None:
        return "stale"

    latest_version_data = latest["versions"][version_key]

    # Step 4: Generate tags
    enriching = latest_version_data["enriching"]

    if enriching["status"] != "completed":

        tags = await process_stage(
            document_id=document_id,
            version=version,
            stage="enriching",
            operation=generate_tags,
            content=summary,
        )

        if tags is None:

            latest = await documents.find_one({
                "_id": ObjectId(document_id),
                "content_version": version,
            })

            if latest is None:
                return "stale"

            if latest["status"] == "failed":
                return "failed"

            return "pending"

    else:
        tags = enriching["tags"]

    # Step 5: Publish completed result
    result = await documents.update_one(
        {
            "_id": ObjectId(document_id),
            "content_version": version,
            f"versions.{version_key}.processing.status": "completed",
            f"versions.{version_key}.enriching.status": "completed",
        },
        {
            "$set": {
                "published_version": version,
                "status": "completed",
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    # Step 6: Save cache only after successful publishing
    if result.modified_count == 1:

        await set_cached_result(
            content_hash=content_hash,
            summary=summary,
            tags=tags,
        )

        logger.info(
            "Document processing completed and cache updated: %s",
            document_id,
        )

        return "completed"

    # Step 7: Check latest document state
    latest = await documents.find_one({
        "_id": ObjectId(document_id),
    })

    if latest is None:
        return "stale"

    if latest["content_version"] != version:
        return "stale"

    # Another worker may already have published this version.
    if (
        latest["status"] == "completed"
        and latest.get("published_version") == version
    ):
        published_data = latest["versions"][version_key]

        await set_cached_result(
            content_hash=published_data["content_hash"],
            summary=published_data["processing"]["summary"],
            tags=published_data["enriching"]["tags"],
        )

        return "completed"

    if latest["status"] == "failed":
        return "failed"

    return "pending"
async def recover_pending_messages():
    redis_client = get_redis()

    result = await redis_client.xautoclaim(
        STREAM_NAME,
        GROUP_NAME,
        CONSUMER_NAME,
        min_idle_time=60000,
        start_id="0-0",
        count=10,
    )

    next_id, entries = result[0], result[1]

    logger.info("Recovering pending messages")

    for message_id, message in entries:
        try:
            await process_document(message)

            await redis_client.xack(
                STREAM_NAME,
                GROUP_NAME,
                message_id,
            )

        except Exception:
            logger.exception(
                "Pending job recovery failed: %s",
                message_id,
            )

async def consume_messages():
    redis_client = get_redis()

    while True:
        messages = await redis_client.xreadgroup(
            GROUP_NAME,
            CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=1,
            block=5000,
        )

        for stream_name, entries in messages:
            for message_id, message in entries:

                try:
                    outcome = await process_document(message)

                    # Release reservation only for terminal states.
                    if outcome in ("completed", "failed"):
                        released = await release_job(
                            user_id=message["user_id"],
                            document_id=message["document_id"],
                        )

                        logger.info(
                            "Job reservation released: %s, released=%s",
                            message["document_id"],
                            released,
                        )

                    # Acknowledge only terminal or stale messages.
                    if outcome in ("completed", "failed", "stale"):
                        await redis_client.xack(
                            STREAM_NAME,
                            GROUP_NAME,
                            message_id,
                        )

                    else:
                        logger.warning(
                            "Job is not in terminal state. "
                            "Leaving message pending: %s",
                            message_id,
                        )

                except Exception:
                    logger.exception(
                        "Job failed. Leaving message pending: %s",
                        message_id,
                    )
async def main():
    await connect_mongodb()
    await connect_redis()

    try:
        await create_consumer_group()
        logger.info("Background worker started")
        await recover_pending_messages()

        await consume_messages()

    finally:
        await close_redis()
        await close_mongodb()


if __name__ == "__main__":
    asyncio.run(main())

