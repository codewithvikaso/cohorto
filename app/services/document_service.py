import hashlib
from datetime import datetime, timezone

from bson import ObjectId
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument

from fastapi import HTTPException, status

from app.db.mongodb import get_database
from app.schemas.document import (
    DocumentCreate,
    DocumentUpdate,
    DocumentStatus,
)



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

async def create_document(data:DocumentCreate):
    db = get_database()
    documents = db["documents"]
    now = datetime.now(timezone.utc)
    content_hash = generate_content_hash(data.content)
    document = {
        "user_id": data.user_id,
        "title": data.title,
        "client_doc_ref":data.client_doc_ref,
        "content": data.content,
        "content_hash":content_hash,
        "content_version": 1,
        "publish_version": None,
        "status": DocumentStatus.QUEUED.value,
        "versions": {
            "1": {
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
        },
        "created_at": now,
        "update_at": now
    }
    try:
        result = await documents.insert_one(document)
    except DuplicateKeyError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="client_doc_ref already exist"
            
        )
    return {
        "document_id":str(result.inserted_id),
        "status": DocumentStatus.QUEUED.value,
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
    if not ObjectId.is_valid(document_id):
        raise HTTPException(
            status_code= status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    db = get_database()
    documents = db["documents"]
    existing = await documents.find_one({
        "_id":ObjectId(document_id),"user_id": user_id
    })
    
    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Documents not found"
        )
    expected_version = data.expected_version
    if expected_version != existing["content_version"]:
        raise HTTPException(
            status_code = status.HTTP_409_CONFLICT,
            detail="version conflict. fetch the latest document and try again"
        )
    if data.content == existing["content"]:
        return build_document_response(existing)
    new_version = expected_version + 1
    content_hash = generate_content_hash(data.content)
    now  = datetime.now(timezone.utc)
    
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
    updated = await documents.find_one_and_update(
        {
            "_id": ObjectId(document_id),
            "user_id": user_id,
            "content_version": expected_version,
        },
        {
            "$set":{
                "content": data.content,
                "content_hash": content_hash,
                "content_version": new_version,
                "status": DocumentStatus.QUEUED.value,
                f"versions.{new_version}": new_version_data,
                "updated_at": now,
            }
        },
        return_document =  ReturnDocument.AFTER,
    )
    
    if updated is None:
        raise HTTPException(
            status_code = status.HTTP_409_CONFLICT,
            detail = "Document was updated by another request. Featch the latest version."
        )
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
