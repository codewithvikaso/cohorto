from pymongo import AsyncMongoClient
from app.core.config import settings

mongo_client = None
database = None
async def connect_mongodb():
    global mongo_client, database
    
    mongo_client = AsyncMongoClient(
        settings.MONGODB_URL
    )
    database = mongo_client[
        settings.MONGODB_DATABASE
    ]
    
    await mongo_client.admin.command("ping")
    await create_indexes()
    print("Mongodb Connected Sucessfully")
    
    
async def close_mongodb():
    global mongo_client
    
    if mongo_client is not None:
        await mongo_client.close()
        mongo_client = None
        
    print("Mongodb connection close")
    
def get_database():
    if database is None:
        raise RuntimeError("MongoDB is not connected")
    return database
    
async def create_indexes():
    documents = database["documents"]
    
    await documents.create_index(
        [
            ("user_id", 1),
            ("created_at", -1)
        ],
        name = "user_created_at_idx"
    )
    
    await documents.create_index(
        [
            ("user_id", 1),
            ("status", 1), 
            ("created_at", -1)
            ],
        name = "user_status_created_at_index"
        )
    
    await documents.create_index(
        [
            ("client_doc_ref", 1)
        ],
        unique = True,
        partialFilterExpression = {
            "client_doc_ref" : {"$type":"string"}
        },
        name = "client_doc_ref_unique_idx"
    )
    
    await documents.create_index([
        ("content_hash", 1)
    ],
    name = "content_hash_idx"                                
    )
    
    print("Mongodb indexes created sucessfully")