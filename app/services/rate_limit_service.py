from app.db.redis import get_redis
from app.core.config import settings


# Atomic reservation:
# 0 = limit reached
# 1 = new reservation created
# 2 = reservation already exists

RESERVE_SCRIPT = """
local existing = redis.call('EXISTS', KEYS[2])

if existing == 1 then
    return 2
end

local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local maximum = tonumber(ARGV[1])

if current >= maximum then
    return 0
end

redis.call('INCR', KEYS[1])
redis.call('SET', KEYS[2], '1')

return 1
"""


# Release reservation only once.

RELEASE_SCRIPT = """
local existing = redis.call('EXISTS', KEYS[2])

if existing == 0 then
    return 0
end

redis.call('DEL', KEYS[2])

local current = redis.call('DECR', KEYS[1])

if current < 0 then
    redis.call('SET', KEYS[1], '0')
end

return 1
"""

async def reserve_job_status(user_id: str, document_id: str) -> int:
    redis_client = get_redis()

    active_key = f"cohort:active:{user_id}"
    reservation_key = f"cohort:reservation:{user_id}:{document_id}"

    result = await redis_client.eval(
        RESERVE_SCRIPT,
        2,
        active_key,
        reservation_key,
        settings.MAX_ACTIVE_JOBS,
    )

    # 0 = Limit exceeded
    # 1 = New reservation created
    # 2 = Reservation already exists

    return int(result)


async def reserve_job(user_id: str, document_id: str) -> bool:
    result = await reserve_job_status(user_id, document_id)
    return result in (1, 2)

async def release_job(user_id: str, document_id: str) -> bool:
    redis_client = get_redis()

    active_key = f"cohort:active:{user_id}"
    reservation_key = f"cohort:reservation:{user_id}:{document_id}"

    result = await redis_client.eval(
        RELEASE_SCRIPT,
        2,
        active_key,
        reservation_key,
    )

    return result == 1


async def get_active_jobs(user_id: str) -> int:
    redis_client = get_redis()

    active_key = f"cohort:active:{user_id}"

    count = await redis_client.get(active_key)

    return int(count or 0)