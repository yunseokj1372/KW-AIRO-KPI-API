import pandas as pd
from app.routers.redo import RedoInput
from app.db.queries import redoInput, redoOutput
from app.utils.process import mainFilter, processing, mergeWithRedo, getExcelBase64
import logging
import warnings
import oracledb
warnings.filterwarnings("ignore")

def singleRedoService(request: RedoInput, connection: oracledb.Connection):

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
        df = processing(df)
    except:
        logger.error("Error processing data")
        raise HTTPException(status_code=403, detail="Error processing data")
    try:
        logger.info("Merging data")
        df = mergeWithRedo(df)
    except:
        logger.error("Error merging data")
        raise HTTPException(status_code=404, detail="Error merging data")
    try:
        logger.info("Getting excel base64")
        df = getExcelBase64(df)
    except:
        logger.error("Error getting excel base64")
        raise HTTPException(status_code=405, detail="Error getting excel base64")

    
    return df