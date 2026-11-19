from app.db.redis import get_redis
STREAM_NAME = "document_processing"
GROUP_NAME = "document_workers"

async def enqueue_document(
    document_id:str,
    user_id:str,
    version:int,
):
    redis_client = get_redis()
    message_id = await redis_client.xadd(
        STREAM_NAME,
        {"document_id": document_id,
         "user_id": user_id,
         "version": str(version)
         }
    )
    return message_id