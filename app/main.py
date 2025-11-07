from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.security import APIKeyHeader
from app.routers import redo
from app.core.config import settings

# Initialize the app
app = FastAPI(
    title="KW AIRO Redo API",
    description= """
    API for generating an Excel spreadsheet with information about redo tickets within a date range for Service Quick.

    ## Authentication
    All endpoints require an API key to be provided in the `X-API-Key` header.
    """,
    version="1.0.1",
    contact={
        "name": "AIRO Support",
        "email": "support@airo.com"
    }
)

# Define API key security scheme
api_key_header = APIKeyHeader(
    name="X-API-Key", 
    auto_error=True,
    description="API key for authentication. Must be provided in the `X-API-Key` header."
)

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
@app.get("/", 
    dependencies=[Depends(verify_api_key)],
    summary="Root endpoint",
    description="Returns a welcome message to confirm the API is running.",
    response_description="Welcome message",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {"message": "Welcome to AIRO's KPI app"}
                }
            }
        },
        410: {
            "description": "Invalid API key",
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid API key"}
                }
            }
        }
    }
)
def read_root():
    return {"message": "Welcome to AIRO's KPI app"}

# Custom OpenAPI schema 
def custom_openapi(): 
    if app.openapi_schema: 
        return app.openapi_schema 
        
    openapi_schema = get_openapi( 
        title=app.title, 
        version=app.version, 
        description=app.description, 
        routes=app.routes, 
    ) 
    
    # Add security scheme 
    openapi_schema["components"]["securitySchemes"] = { 
        "ApiKeyHeader": { 
            "type": "apiKey", 
            "in": "header", 
            "name": "X-API-Key", 
            "description": "API key for authentication" 
        } 
    } 
    
    # Apply security globally 
    openapi_schema["security"] = [{"ApiKeyHeader": []}] 
    
    app.openapi_schema = openapi_schema 
    return app.openapi_schema 
    
    app.openapi = custom_openapi
