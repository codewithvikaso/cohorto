from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.config import settings
from app.db.mongodb import (
    connect_mongodb, 
    close_mongodb,
    get_database,
    )
from app.db.redis import (  
    connect_redis,
    close_redis,
    get_redis,
)
from app.api.routes.documents import router as document_router
import logging

from fastapi.responses import JSONResponse
logger = logging.getLogger(__name__)
@asynccontextmanager
async def lifespan(app:FastAPI):
    await connect_mongodb()
    await connect_redis()
    yield
    
    await close_redis()
    await close_mongodb()
    
app = FastAPI(title = settings.APP_NAME,
              lifespan = lifespan)
    
app.include_router(document_router)
@app.get("/")
async def root():
    return {"message": "Cohort Insights API is Runing"}

    
@app.get("/health", tags=["Health"])
async def health_check():

    checks = {}

    # Check MongoDB
    try:
        await get_database().command("ping")
        checks["mongodb"] = "ok"

    except Exception:
        logger.exception("MongoDB health check failed")
        checks["mongodb"] = "error"

    # Check Redis
    try:
        await get_redis().ping()
        checks["redis"] = "ok"

    except Exception:
        logger.exception("Redis health check failed")
        checks["redis"] = "error"

    # Overall application health
    is_healthy = all(
        value == "ok"
        for value in checks.values()
    )

    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content={
            "status": "healthy" if is_healthy else "unhealthy",
            "checks": checks,
        },
    )