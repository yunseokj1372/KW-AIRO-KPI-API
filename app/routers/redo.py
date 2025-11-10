from app.db.connection import getConnection
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
from app.db.queries import redoInput, redoOutput
from app.utils.process import mainFilter, processData, mergeWithRedo, getExcelBase64
import logging
import warnings
import asyncio
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
    ),
    accountNo: list = Field(
        ...,
        description="List of account numbers to retrieve tickets for. Must be in a list of strings.",
        example=["1234567890", "0987654321"]
    )

    class Config:
        schema_extra = {
            "example": {
                "startDate": "2024-01-15",
                "endDate": "2024-03-31",
                "accountNo": ["1234567890", "0987654321"]
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
        405: {
            "description": "No redo data found",
            "content": {
                "application/json": {
                    "example": {"detail": "No redo data found"}
                }
            }
        },
        406: {
            "description": "Error compiling redo data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error compiling redo data"}
                }
            }
        },
        407: {
            "description": "Error merging data",
            "content": {
                "application/json": {
                    "example": {"detail": "Error merging data"}
                }
            }
        },
        408: {
            "description": "Error dropping redo check column",
            "content": {
                "application/json": {
                    "example": {"detail": "Error dropping redo check column"}
                }
            }
        },
        409: {
            "description": "Error logging success",
            "content": {
                "application/json": {
                    "example": {"detail": "Error logging success"}
                }
            }
        },
        410: {
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
async def singleRedo(request: RedoInput):
    connection = None
    try:
        logger.info("Connecting to database")
        # Run connection in thread pool since it's blocking I/O
        connection = await asyncio.to_thread(getConnection)
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Error connecting to database")
    
    try:
        logger.info("Retrieving ticket list for date range: %s to %s", request.startDate, request.endDate)

        df = await redoInput(request.startDate, request.endDate, request.accountNo, connection)
    except Exception as e:
        logger.error(f"Error retrieving ticket list: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=401, detail="Error retrieving data from database")
    
    try:
        logger.info("Filtering data")
        df = await mainFilter(df)
    except Exception as e:
        logger.error(f"Error filtering data: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=402, detail="Error filtering data")
    
    try:
        logger.info("Processing data")
        df['REDO_CHECK'] = None
        serial = list(df['SERIALNO'].unique())
        filtered_df = await processData(df, serial)
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=403, detail="Error processing data")
    
    try:
        logger.info("Compiling data")
        separate_df = filtered_df.dropna(subset=['REDO_CHECK'])
    except Exception as e:
        logger.error(f"Error compiling data: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=404, detail="Error compiling separate data")
    
    try:
        logger.info("Compiling redo data")
        tupe = tuple(separate_df['REDO_CHECK'].astype(int))
        redo_tupe = tuple(f'{x}' for x in tupe)
        output = await redoOutput(redo_tupe, request.startDate, request.endDate, connection)

        if output.empty:
            logger.info("No redo data found")
            if connection:
                await asyncio.to_thread(connection.close)
            raise HTTPException(status_code=405, detail="No redo data found")
        else:
            logger.info(f"Redo data found: {output.shape[0]} rows")
            logger.info(f"Redo data columns: {list(output.columns)}")
    except Exception as e:
        logger.error(f"Error compiling redo data: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=406, detail="Error compiling redo data")
    
    try:
        logger.info("Merging data")
        redo_output_df = await mergeWithRedo(filtered_df, output)
    except Exception as e:
        logger.error(f"Error merging data: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=407, detail="Error merging data")
    
    try:
        logger.info("Dropping redo check column")
        redo_output_df = redo_output_df.drop('REDO_CHECK', axis=1)
    except Exception as e:
        logger.error(f"Error dropping redo check column: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=408, detail="Error dropping redo check column")
    
    try:
        logger.info(f"Successfully processed redo data. Output shape: {redo_output_df.shape}. Date range: {request.startDate} to {request.endDate}")
    except Exception as e:
        logger.error(f"Error logging success: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=409, detail="Error logging success")

    try:
        logger.info("Getting excel base64")
        excel_base64 = await getExcelBase64(redo_output_df)
    except Exception as e:
        logger.error(f"Error getting excel base64: {e}")
        if connection:
            await asyncio.to_thread(connection.close)
        raise HTTPException(status_code=410, detail="Error getting excel base64")
    
    # Close the connection
    try:
        if connection:
            await asyncio.to_thread(connection.close)
            logger.info("Database connection closed successfully")
    except Exception as e:
        logger.warning(f"Error closing database connection: {e}")
    
    return excel_base64