from app.db.connection import get_connection
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
from app.db.queries import redo_input, redo_output
from app.utils.process import main_filter, processing, merge_with_redo, get_excel_base64
import logging
import warnings
warnings.filterwarnings("ignore")



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
            "description": "Error compiling data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error compiling data"}
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
def single_redo(request: RedoInput):
    try:
        logger.info("Connecting to database")
        connection = get_connection()
    except:
        logger.error("Error connecting to database")
        raise HTTPException(status_code=500, detail="Error connecting to database")
    
    try:
        logger.info("Retrieving ticket list for date range: %s to %s", request.startDate, request.endDate)
        df = redo_input(request.startDate, request.endDate, connection)
    except:
        logger.error("Error retrieving ticket list")
        raise HTTPException(status_code=401, detail="Error retrieving data from database")
    
    try:
        logger.info("Filtering data")
        df = main_filter(df)
    except:
        logger.error("Error filtering data")
        raise HTTPException(status_code=402, detail="Error filtering data")
    
    try:
        logger.info("Processing data")
        df['REDO_CHECK'] = None
        serial = list(df['SERIALNO'].unique())
        filtered_df = processing(df, serial)
    except:
        logger.error("Error processing data")
        raise HTTPException(status_code=403, detail="Error processing data")
    
    try:
        logger.info("Compiling data")
        separate_df = filtered_df.dropna(subset=['REDO_CHECK'])
        tupe = tuple(separate_df['REDO_CHECK'].astype(int))
        redo_tupe = tuple(f'{x}' for x in tupe)
        output = redo_output(redo_tupe, request.startDate, request.endDate, connection)
        redo_output_df = merge_with_redo(filtered_df, output)
        redo_output_df = redo_output_df.drop('REDO_CHECK', axis=1)
        logger.info(f"Successfully processed redo data. Output shape: {redo_output_df.shape}. Date range: {request.startDate} to {request.endDate}")
    except:
        raise HTTPException(status_code=404, detail="Error compiling data")
    

    return get_excel_base64(redo_output_df)