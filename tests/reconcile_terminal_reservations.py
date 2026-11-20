import asyncio

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
from app.services.rate_limit_service import release_job


async def main():
    try:
        await connect_mongodb()
        await connect_redis()

        db = get_database()
        redis_client = get_redis()

        cursor = db["documents"].find({
            "status": {"$in": ["completed", "failed"]}
        })

        released_count = 0

        async for document in cursor:
            user_id = document["user_id"]
            document_id = str(document["_id"])

            # Completed document must have its current version published.
            if document["status"] == "completed":
                if document.get("published_version") != document.get(
                    "content_version"
                ):
                    continue

            reservation_key = (
                f"cohort:reservation:{user_id}:{document_id}"
            )

            exists = await redis_client.exists(reservation_key)

            if exists:
                released = await release_job(
                    user_id=user_id,
                    document_id=document_id,
                )

                if released:
                    released_count += 1
                    print(
                        f"Released: user={user_id}, "
                        f"document={document_id}"
                    )

        print(f"\nTotal reservations released: {released_count}")

    finally:
        await close_redis()
        await close_mongodb()


if __name__ == "__main__":
    asyncio.run(main())