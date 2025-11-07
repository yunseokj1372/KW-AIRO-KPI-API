from app.db.connection import getConnection
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import logging
from app.services.redo_service import singleRedoService



# Set up file logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redo.log'),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/redo", 
    tags=["redo"],
    responses={
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

class RedoInput(BaseModel):
    startDate: str = Field(
        ...,
        description="Start date of the date range to retrieve tickets for. Must be in YYYY-MM-DD format.",
        example="2024-01-15"
    )
    endDate: str = Field(
        ...,
        description="End date of the date range to retrieve tickets for. Must be in YYYY-MM-DD format.",
        example="2024-03-31"
    )
    accountNo: list = Field(
        ...,
        description="List of account numbers to retrieve tickets for. Must be in a list of strings.",
        example=["1234567890", "1234567891"]
    )
    class Config:
        schema_extra = {
            "example": {
                "startDate": "2024-01-15",
                "endDate": "2024-03-31"
            }
        }


@router.post("/single",
    summary="Process a single date range of redo tickets",
    description="""
    Processes redo data for a specified date range and returns an Excel file in base64 format.

    The process includes:
    1. Retrieving ticket list from the database
    2. Filtering and processing the data
    3. Merging with ticket information
    4. Generating an Excel file with the results

    The returned base64 encoded Excel file can be used to download the file or displayed in a web browser.
    """,
    response_description="Base64 encoded Excel file with the processed data",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "filename": "redo_output.xlsx",
                        "file": "UEsDBBQAAAgIAMBe..." # Truncated base64 example
                    }
                }
            }
        },
        401: {
            "description": "Error retrieving data from database",
            "content": {
                "application/json": {
                    "example": {"detail": "Error retrieving data from database"}
                }
            }
        },
        402: {
            "description": "Error filtering data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error filtering data"}
                }
            }
        },
        403: {
            "description": "Error processing data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error processing data"}
                }
            }
        },
        404: {
            "description": "Error getting redo data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error getting redo data"}
                }
            }
        },
        405: {
            "description": "Error merging data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error merging data"}
                }
            }
        },
        406: {
            "description": "Error getting excel base64",
            "content": {
                "application/json": {
                    "example": {"detail": "Error getting excel base64"}
                }
            }
        },
        500: {
            "description": "Error connecting to database",
            "content": {
                "application/json": {
                    "example": {"detail": "Error connecting to database"}
                }
            }
        }
    }
)
def singleRedo(request: RedoInput):
    try:
        logger.info("Connecting to database")
        connection = getConnection()
    except:
        logger.error("Error connecting to database")
        raise HTTPException(status_code=500, detail="Error connecting to database")

    result = singleRedoService(request, connection)

    connection.close()

    return result
