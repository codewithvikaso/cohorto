from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"
    
class StageStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    
    
class DocumentCreate(BaseModel):
    user_id : str = Field(min_length=1, max_length=100)
    title : str = Field(min_length=1, max_length=200)
    content : str = Field(min_length=1, max_length=100_000)
    client_doc_ref : Optional[str] = Field(default=None, min_length=1, max_length=200)
    
    
    @field_validator("user_id", "title", "content", "client_doc_ref")
    @classmethod
    def strip_string_values(cls, value):
        if value is None:
            return value
        value = value.strip()
        
        if not value:
            raise ValueError("Field cannot contain whitespace")
        return value
    

class DocumentUpdate(BaseModel):
    content : str = Field(min_length=1, max_length=100_000)
    
    expected_version: int = Field(ge=1)
    
    @field_validator("content")
    @classmethod
    def strip_content(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("content cannot be empty")
        return value
    
class ProcessingStage(BaseModel):
    status : StageStatus = StageStatus.QUEUED
    attempts : int = 0
    summary : Optional[str] = None
    error : Optional[str] = None
    
    
class EnrichingStage(BaseModel):
    status : StageStatus = StageStatus.QUEUED
    attempts : int = 0
    tags : list[str] = Field(default_factory=list)
    error : Optional[str] = None
    
class VersionData(BaseModel):
    content : str
    content_hash : str
    processing : ProcessingStage = Field(
        default_factory = ProcessingStage
    )
    enriching : EnrichingStage = Field(
        default_factory=EnrichingStage
    )
    
    
class PublishResult(BaseModel):
    version : int
    summary : str
    tags : list[str]
    
    
class DocumentCreateResponse(BaseModel):
    document_id : str
    status : DocumentStatus
    content_version : int
    
class DocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    document_id : str
    user_id : str
    title : str
    content_version : int
    published_version: Optional[int] = None
    status : DocumentStatus
    processing : ProcessingStage
    enriching : EnrichingStage
    
    
    current_result : Optional[PublishResult] = None
    
    is_current : bool
    created_at : datetime
    updated_at : datetime
    
    
class DocumentListResponse(BaseModel):
    items : list[DocumentResponse]
    page : int
    page_size : int
    
    