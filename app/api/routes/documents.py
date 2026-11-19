from fastapi import APIRouter, Depends, Query, status, HTTPException
from app.core.auth import get_current_user_id
from app.schemas.document import (
    DocumentCreate,
    DocumentUpdate,
    DocumentCreateResponse,
    DocumentResponse,
    DocumentListResponse,
)
from app.services.document_service import (
    create_document,
    get_document,
    update_document,
    list_user_documents,
    get_document_by_ref,
)
router = APIRouter()

@router.post("/documents",response_model=DocumentCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_document_api(data: DocumentCreate, current_user_id:str = Depends(get_current_user_id)):
    if data.user_id != current_user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You cannot create document for another user")
    return await create_document(data)

@router.get("/document/by-ref/{client_doc_ref}", response_model=DocumentResponse)
async def get_document_by_ref_api(client_doc_ref: str, current_user_id:str = Depends(get_current_user_id)):
    return await get_document_by_ref(client_doc_ref, current_user_id)


@router.get("/documents/{document_id}",response_model=DocumentResponse)
async def get_document_api(document_id:str, current_user_id:str = Depends(get_current_user_id)):
    return await get_document(document_id, current_user_id)

@router.patch("/documents/{document_id}", response_model=DocumentResponse)
async def update_document_api(document_id:str, data: DocumentUpdate, current_user_id:str = Depends(get_current_user_id)):
    return await update_document(document_id, current_user_id, data)

@router.get("/users/{user_id}/documents", response_model= DocumentListResponse)
async def list_user_document_api(
    user_id:str, 
    page:int = Query(default=1, ge=1), 
    page_size:int = Query(default=10, ge=1, le=100), 
    status_filter:str | None = Query(default= None, alias="status"),
    current_user_id:str = Depends(get_current_user_id),
    ):
    if user_id != current_user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail= "You cannot access another user's documents")
    
    return await list_user_documents(user_id=user_id, page=page, page_size=page_size, status_filter=status_filter)