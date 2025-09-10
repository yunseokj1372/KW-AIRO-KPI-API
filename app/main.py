from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.security import APIKeyHeader
from app.routers import redo
from app.core.config import settings

# Initialize the app
app = FastAPI()

# Define API key security scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.secret_key:
        raise HTTPException(
            status_code=410,
            detail="Invalid API key"
        )
    return api_key

# Add routers to the app
app.include_router(
    redo.router,
    dependencies=[Depends(verify_api_key)]
) 

# Root endpoint
@app.get("/", dependencies=[Depends(verify_api_key)])
def read_root():
    return {"message": "Welcome to AIRO's KPI app"}
