from fastapi import Header, HTTPException, status

async def get_current_user_id(
    x_user_id: str = Header(..., alias = "X-User-ID")
) ->str :
    if not x_user_id.strip():
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail = "X-User-ID header cannot be empty"
        )
    return x_user_id.strip()