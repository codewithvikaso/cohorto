import asyncio

from app.db.redis import connect_redis, close_redis
from app.services.rate_limit_service import get_active_jobs


USER_ID = "user_doc_1"


async def main():
    try:
        await connect_redis()

        count = await get_active_jobs(USER_ID)
        print(f"User ID: {USER_ID}")
        print(f"Active jobs: {count}")

        redis_client = __import__(
            "app.db.redis", fromlist=["get_redis"]
        ).get_redis()

        print("\nRedis active-job keys:")

        async for key in redis_client.scan_iter(
            match="cohort:active:*"
        ):
            value = await redis_client.get(key)
            print(f"{key} = {value}")

    finally:
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())