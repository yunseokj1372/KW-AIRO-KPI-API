from app.db.connection import get_connection
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from app.db.queries import redo_input, redo_output
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
    start_date: str
    end_date: str
    tickets: list[int]