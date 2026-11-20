import hashlib
from datetime import datetime, timezone
import logging
from bson import ObjectId
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument
from app.services.rate_limit_service import reserve_job, release_job, reserve_job_status
from fastapi import HTTPException, status

from app.db.mongodb import get_database
from app.schemas.document import (
    DocumentCreate,
    DocumentUpdate,
    DocumentStatus,
)


from app.services.queue_service import enqueue_document
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from app.services.cache_service import get_cached_result
from app.services.crosswalk_service import (
    find_document_by_ref,
    validate_crosswalk_owner,
    is_same_content,
)
logger = logging.getLogger(__name__)
def generate_content_hash(content : str) ->str :
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def get_current_version_data(document:dict) -> dict:
    version_key = str(document["content_version"])
    return document["versions"][version_key]

def build_document_response(document:dict) -> dict:
    version_data = get_current_version_data(document)
    processing = version_data["processing"]
    enriching = version_data["enriching"]
    
    is_current = (
        document.get("published_version") == document["content_version"]
        and processing["status"] == "completed"
        and enriching["status"] == "completed"
    )
    
    current_result = None
    
    if is_current:
        current_result = {
            "version": document["content_version"],
            "summary": processing["summary"],
            "tags": enriching["tags"],
        }
        
    return {
        "document_id" : str(document["_id"]),
        "user_id": document["user_id"],
        "title": document["title"],
        "content_version": document["content_version"],
        "published_version":  document.get("published_version"),
        "status": document["status"],
        "processing":processing,
        "enriching":enriching,
        "current_result":current_result,
        "is_current":is_current,
        "created_at": document["created_at"],
        "updated_at": document["created_at"],
    }


async def create_document(data: DocumentCreate):
    db = get_database()
    documents = db["documents"]

    now = datetime.now(timezone.utc)
    content_hash = generate_content_hash(data.content)

    # Step 1: Check Redis cache before reserving a job slot.
    cached_result = await get_cached_result(content_hash)

    cache_hit = cached_result is not None

    # Step 2: Generate a new document ID.
    document_id = str(ObjectId())

    
    # =================================
    # STEP 1: CROSSWALK CHECK
    # =================================

    if data.client_doc_ref:

        existing = await find_document_by_ref(
            documents=documents,
            client_doc_ref=data.client_doc_ref,
        )

        if existing:

            # Verify document ownership
            validate_crosswalk_owner(
                existing=existing,
                user_id=data.user_id,
            )

            # Same reference + same content
            if is_same_content(
                existing=existing,
                content=data.content,
            ):
                return {
                    "document_id": str(existing["_id"]),
                    "status": existing["status"],
                    "content_version": existing["content_version"],
                }

            # Same reference + different content
            # Update the existing document instead of creating a new one.
            existing_document_id = str(existing["_id"])

            await update_document(
                document_id=existing_document_id,
                user_id=data.user_id,
                data=DocumentUpdate(
                    content=data.content,
                    expected_version=existing["content_version"],
                ),
            )

            return {
                "document_id": existing_document_id,
                "status": DocumentStatus.QUEUED.value,
                "content_version": existing["content_version"] + 1,
            }

    
    # Step 3: Reserve a slot only when cache is unavailable.
    if not cache_hit:
        reserved = await reserve_job(
            user_id=data.user_id,
            document_id=document_id,
        )

        if not reserved:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Maximum 3 active documents allowed per user",
            )

    # Step 4: Prepare processing and enriching stages.
    if cache_hit:
        processing_data = {
            "status": "completed",
            "attempts": 0,
            "summary": cached_result["summary"],
            "error": None,
        }

        enriching_data = {
            "status": "completed",
            "attempts": 0,
            "tags": cached_result["tags"],
            "error": None,
        }

        document_status = DocumentStatus.COMPLETED.value
        published_version = 1

    else:
        processing_data = {
            "status": "queued",
            "attempts": 0,
            "summary": None,
            "error": None,
        }

        enriching_data = {
            "status": "queued",
            "attempts": 0,
            "tags": [],
            "error": None,
        }

        document_status = DocumentStatus.QUEUED.value
        published_version = None

    # Step 5: Prepare MongoDB document.
    document = {
        "_id": ObjectId(document_id),
        "user_id": data.user_id,
        "title": data.title,
        "client_doc_ref": data.client_doc_ref,
        "content": data.content,
        "content_hash": content_hash,
        "content_version": 1,
        "published_version": published_version,
        "status": document_status,
        "versions": {
            "1": {
                "content": data.content,
                "content_hash": content_hash,
                "processing": processing_data,
                "enriching": enriching_data,
            }
        },
        "created_at": now,
        "updated_at": now,
    }

    # Step 6: Insert document and enqueue only on cache miss.
    try:
        await documents.insert_one(document)

        if not cache_hit:
            await enqueue_document(
                document_id=document_id,
                user_id=data.user_id,
                version=1,
            )

    except DuplicateKeyError:
        await documents.delete_one({
            "_id": ObjectId(document_id)
        })

        if not cache_hit:
            try:
                await release_job(
                    user_id=data.user_id,
                    document_id=document_id,
                )
            except Exception:
                logger.exception("Failed to release reservation")

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="client_doc_ref already exists",
        )

    except Exception:
        logger.exception(
            "Document creation failed: %s",
            document_id,
        )

        await documents.delete_one({
            "_id": ObjectId(document_id)
        })

        if not cache_hit:
            try:
                await release_job(
                    user_id=data.user_id,
                    document_id=document_id,
                )
            except Exception:
                logger.exception("Failed to release reservation")

        raise

    # Step 7: Return response.
    return {
        "document_id": document_id,
        "status": document_status,
        "content_version": 1,
    }
async def get_document(document_id: str, user_id: str):
    if not ObjectId.is_valid(document_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document Not Found"
        )
    db = get_database()
    document = await db["documents"].find_one({
        "_id": ObjectId(document_id),
        "user_id":user_id
    })
    
    if document is None:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail="Document nnot found"
        )
        
    return build_document_response(document)
async def update_document(
    document_id: str,
    user_id: str,
    data: DocumentUpdate,
):
    # Step 1: Validate document ID
    if not ObjectId.is_valid(document_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found",
        )

    db = get_database()
    documents = db["documents"]

    # Step 2: Find existing document
    existing = await documents.find_one({
        "_id": ObjectId(document_id),
        "user_id": user_id,
    })

    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found",
        )

    # Step 3: Validate expected version
    expected_version = data.expected_version

    if expected_version != existing["content_version"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Version conflict. Fetch the latest document and try again.",
        )

    # Step 4: If content is unchanged, return existing document
    if data.content == existing["content"]:
        return build_document_response(existing)

    # Step 5: Generate new version
    new_version = expected_version + 1

    content_hash = generate_content_hash(data.content)

    now = datetime.now(timezone.utc)

    # Step 6: Reserve active job slot in Redis
    reservation_result = await reserve_job_status(
        user_id=user_id,
        document_id=document_id,
    )

    if reservation_result == 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Maximum 3 active documents allowed per user",
        )

    # Result 1 means this request created a new reservation.
    # Result 2 means an existing reservation was reused.
    new_reservation_created = reservation_result == 1

    # Step 7: Prepare new version data
    new_version_data = {
        "content": data.content,
        "content_hash": content_hash,
        "processing": {
            "status": "queued",
            "attempts": 0,
            "summary": None,
            "error": None,
        },
        "enriching": {
            "status": "queued",
            "attempts": 0,
            "tags": [],
            "error": None,
        },
    }

    # Step 8: Update MongoDB using optimistic concurrency
    try:
        updated = await documents.find_one_and_update(
            {
                "_id": ObjectId(document_id),
                "user_id": user_id,
                "content_version": expected_version,
            },
            {
                "$set": {
                    "content": data.content,
                    "content_hash": content_hash,
                    "content_version": new_version,
                    "published_version": None,
                    "status": DocumentStatus.QUEUED.value,
                    f"versions.{new_version}": new_version_data,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    except Exception:
        # Release only a reservation created by this request.
        if new_reservation_created:
            try:
                await release_job(
                    user_id=user_id,
                    document_id=document_id,
                )
            except Exception:
                logger.exception(
                    "Failed to rollback Redis reservation: %s",
                    document_id,
                )

        raise

    # Step 9: Handle version conflict
    if updated is None:

        if new_reservation_created:
            try:
                await release_job(
                    user_id=user_id,
                    document_id=document_id,
                )
            except Exception:
                logger.exception(
                    "Failed to rollback Redis reservation: %s",
                    document_id,
                )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Document was updated by another request. Fetch the latest version.",
        )

    # Step 10: Enqueue new version for background processing
    try:
        await enqueue_document(
            document_id=str(updated["_id"]),
            user_id=user_id,
            version=new_version,
        )

    except Exception:
        logger.exception(
            "Failed to enqueue updated document: %s",
            document_id,
        )

        # Keep the reservation because MongoDB is already updated.
        # Reconciliation/recovery is needed for this queue failure.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Document updated, but background job could not be queued.",
        )

    # Step 11: Return updated document
    return build_document_response(updated)

async def list_user_documents(
    user_id:str,
    page:int = 1,
    page_size:int = 10,
    status_filter: str | None = None,
):
    db = get_database()
    documents = db["documents"]
    
    query = {"user_id": user_id}
    
    if status_filter:
        allowed_statuses = {item.value for item in DocumentStatus}
        
        if status_filter not in allowed_statuses:
            raise HTTPException(
                status_code = status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="invalid document status"
            )
        query["status"]= status_filter
        
    skip = (page-1) * page_size
    cursor = (
        documents.find(query).sort("created_at", -1).skip(skip).limit(page_size)
        
    )
    items = []
    
    async for document in cursor:
        items.append(build_document_response(document))
        
    total = await documents.count_documents(query)
    
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total,
    }
    
async def get_document_by_ref(client_doc_ref: str, user_id: str):
    db = get_database
    document = await db["document"].find_one({
        "client_doc_ref": client_doc_ref,
        "user_id": user_id,
    })
    
    if document is None:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = "Document Not Found"
        )
    return build_document_response(document)
