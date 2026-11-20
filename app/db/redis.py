import redis.asyncio as redis

from app.core.config import settings
redis_client = None
async def connect_redis():
    global redis_client
    
    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses = True
    )
    await redis_client.ping()
    print("Redis Connected Successfully")
    
async def close_redis():
    global redis_client
    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None
        
    print("Redis Connection closed")
    
def get_redis():
    if redis_client is None:
        raise RuntimeError("Redis Not Connected")
    
    return redis_client