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


USER_ID = "user7"


async def main():
    try:
        await connect_mongodb()
        await connect_redis()

        db = get_database()
        redis_client = get_redis()

        active_key = f"cohort:active:{USER_ID}"
        active_count = await redis_client.get(active_key)

        print(f"\nUser ID: {USER_ID}")
        print(f"Active counter: {active_count or 0}")

        print("\nMongoDB documents:")

        cursor = db["documents"].find(
            {"user_id": USER_ID},
            {
                "_id": 1,
                "status": 1,
                "content_version": 1,
                "published_version": 1,
            },
        )

        async for doc in cursor:
            document_id = str(doc["_id"])

            reservation_key = (
                f"cohort:reservation:{USER_ID}:{document_id}"
            )

            reservation_exists = await redis_client.exists(
                reservation_key
            )

            print({
                "document_id": document_id,
                "status": doc.get("status"),
                "content_version": doc.get("content_version"),
                "published_version": doc.get("published_version"),
                "reservation_exists": bool(reservation_exists),
            })

    finally:
        await close_redis()
        await close_mongodb()


if __name__ == "__main__":
    asyncio.run(main())