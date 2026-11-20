import json
import logging

from app.db.redis import get_redis
from app.core.config import settings


logger = logging.getLogger(__name__)

# Cache key includes pipeline version.
CACHE_PREFIX = "cohort:content-cache:v1:"


def get_cache_key(content_hash: str) -> str:
    return f"{CACHE_PREFIX}{content_hash}"


async def get_cached_result(content_hash: str):
    """
    Retrieve completed summary and tags from Redis cache.

    Returns:
        dict: Cached summary and tags.
        None: Cache miss or cache unavailable.
    """

    try:
        redis_client = get_redis()

        cache_key = get_cache_key(content_hash)

        cached_data = await redis_client.get(cache_key)

        if cached_data is None:
            logger.info("Cache miss: %s", content_hash)
            return None

        result = json.loads(cached_data)

        # Validate cached response.
        if not isinstance(result, dict):
            return None

        summary = result.get("summary")
        tags = result.get("tags")

        if not isinstance(summary, str):
            return None

        if not isinstance(tags, list):
            return None

        if not all(isinstance(tag, str) for tag in tags):
            return None

        logger.info("Cache hit: %s", content_hash)

        return {
            "summary": summary,
            "tags": tags,
        }

    except Exception:
        # Cache failure should not stop document creation.
        logger.exception("Redis cache read failed")
        return None


async def set_cached_result(
    content_hash: str,
    summary: str,
    tags: list,
) -> bool:
    """
    Store completed summary and tags in Redis.

    Only successfully completed results should be cached.
    """

    try:
        if not isinstance(summary, str):
            return False

        if not isinstance(tags, list):
            return False

        if not all(isinstance(tag, str) for tag in tags):
            return False

        redis_client = get_redis()

        cache_key = get_cache_key(content_hash)

        cache_data = {
            "summary": summary,
            "tags": tags,
        }

        await redis_client.set(
            cache_key,
            json.dumps(cache_data),
            ex=settings.CACHE_TTL,
        )

        logger.info("Cache stored successfully: %s", content_hash)

        return True

    except Exception:
        logger.exception("Redis cache write failed")
        return False