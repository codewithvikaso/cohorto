import asyncio
import redis.asyncio as redis
import pytest
from app.core.config import settings

@pytest.mark.asyncio
async def test_redis():

    client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True
    )

    try:
        print("Redis URL:", settings.REDIS_URL)

        print("PING:", await client.ping())

        info = await client.info("server")

        print("Redis Version:", info.get("redis_version"))

        result = await client.execute_command(
            "XGROUP",
            "HELP"
        )

        print("XGROUP command is available")
        print(result)

    except Exception as e:
        print("Redis Error:", repr(e))

    finally:
        await client.aclose()


asyncio.run(test_redis())