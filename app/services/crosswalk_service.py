from fastapi import HTTPException, status


async def find_document_by_ref(documents, client_doc_ref: str):
    """
    Find an existing document using client_doc_ref.
    """

    if not client_doc_ref:
        return None

    existing = await documents.find_one({
        "client_doc_ref": client_doc_ref
    })

    return existing


def validate_crosswalk_owner(existing: dict, user_id: str):
    """
    Ensure that the client_doc_ref belongs to the same user.
    """

    if existing["user_id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="client_doc_ref is already associated with another user",
        )


def is_same_content(existing: dict, content: str) -> bool:
    """
    Check whether the incoming content matches existing content.
    """

    return existing.get("content") == content