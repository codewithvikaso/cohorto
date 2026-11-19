from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.config import settings
from app.db.mongodb import (
    connect_mongodb, 
    close_mongodb
    )
from app.db.redis import (
    connect_redis,
    close_redis
)
from app.api.routes.documents import router as document_router

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

    
