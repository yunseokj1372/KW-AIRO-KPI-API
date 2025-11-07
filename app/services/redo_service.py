import pandas as pd
from fastapi import HTTPException
from app.db.queries import redoInput, redoOutput
from app.utils.process import mainFilter, processing, mergeWithRedo, getExcelBase64
import logging
import warnings
import oracledb
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

def singleRedoService(request, connection: oracledb.Connection):

    try:
        logger.info("Retrieving ticket list from database")
        df = redoInput(request.startDate, request.endDate, request.accountNo, connection)
    except:
        logger.error("Error retrieving ticket list from database")
        raise HTTPException(status_code=401, detail="Error retrieving ticket list from database")

    try:
        logger.info("Filtering data")
        df = mainFilter(df)
    except:
        logger.error("Error filtering data")
        raise HTTPException(status_code=402, detail="Error filtering data")
    try:
        logger.info("Processing data")
        serial = df['SERIALNO'].unique()
        df = processing(df, serial)
    except:
        logger.error("Error processing data")
        raise HTTPException(status_code=403, detail="Error processing data")

    try:
        logger.info("Getting redo data")
        redo_tickets = df['REDO_CHECK'].dropna().unique()
        redo_df = redoOutput(redo_tickets, request.startDate, request.endDate, connection)

        if redo_df.empty:
            logger.info("No redo data found")
            redo_df = pd.DataFrame()
            return redo_df
        else:
            logger.info(f"Redo data found: {redo_df}")
            return redo_df
    except:
        logger.error("Error getting redo data")
        raise HTTPException(status_code=404, detail="Error getting redo data")

    try:
        logger.info("Merging data")
        df = mergeWithRedo(df, redo_df)
    except:
        logger.error("Error merging data")
        raise HTTPException(status_code=405, detail="Error merging data")
    try:
        logger.info("Getting excel base64")
        df = getExcelBase64(df)
    except:
        logger.error("Error getting excel base64")
        raise HTTPException(status_code=406, detail="Error getting excel base64")

    
    return df