import pytest
from pydantic import ValidationError
from app.schemas.document import (
    DocumentCreate,
    DocumentUpdate,
    VersionData,
)

def test_valid_document_create():
    document = DocumentCreate(
        user_id = "user123",
        title= "python programming",
        content = "python is a programming language"
    )
    assert document.user_id == "user123"
    assert document.title == "python programming"
    
def test_empty_content_not_allowed():
    with pytest.raises(ValidationError):
        DocumentCreate(
            user_id = "user123",
            title = "python",
            content = ""
        )
def test_whitespace_content_not_allowed():
    with pytest.raises(ValidationError):
        DocumentCreate(
            user_id = "user123",
            title = "python",
            content = "  "
        )
        
def test_document_update():
    update = DocumentUpdate(
        content = "Updated Python Content",
        expected_version = 1    
    )
    
    assert update.content == "Updated Python Content"
    assert update.expected_version == 1
    
    
def test_version_data_default_stage():
    version = VersionData(
        content = "Python Content",
        content_hash = "abc123"
    )
    
    assert version.processing.status == "queued"
    assert version.enriching.status == "queued"
    assert version.enriching.tags == []