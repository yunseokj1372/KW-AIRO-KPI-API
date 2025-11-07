from app.db.connection import get_connection
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from app.db.queries import redo_input, redo_output
from app.utils.process import main_filter, processing, merge_with_redo, get_excel_base64
import logging


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


router = APIRouter(prefix="/redo", tags=["redo"])

class RedoInput(BaseModel):
    startDate: str
    endDate: str
    accountNo: list


@router.post("/single")
def single_redo(request: RedoInput):
    try:
        logger.info("Connecting to database")
        connection = get_connection()
    except:
        logger.error("Error connecting to database")
        raise HTTPException(status_code=500, detail="Error connecting to database")
    
    try:
        df = redo_input(request.startDate, request.endDate, connection)
    except:
        raise HTTPException(status_code=401, detail="Error retrieving data from database")
    
    try:
        df = main_filter(df)
    except:
        raise HTTPException(status_code=402, detail="Error filtering data")
    
    try:
        df['REDO_CHECK'] = None
        serial = list(df['SERIALNO'].unique())
        filtered_df = processing(df, serial)
    except:
        raise HTTPException(status_code=403, detail="Error processing data")
    
    try:
        separate_df = filtered_df.dropna(subset=['REDO_CHECK'])
        tupe = tuple(separate_df['REDO_CHECK'].astype(int))
        redo_tupe = tuple(f'{x}' for x in tupe)
        output = redo_output(redo_tupe, request.startDate, request.endDate, connection)
        redo_output_df = merge_with_redo(filtered_df, output)
        redo_output_df = redo_output_df.drop('REDO_CHECK', axis=1)
    except:
        raise HTTPException(status_code=404, detail="Error retrieving data")
    

    return get_excel_base64(redo_output_df)