import pandas as pd
from datetime import date, timedelta, datetime
import re

def validate_date_format(date_string):
    """Validate that date string matches YYYY-MM-DD format"""
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    return bool(re.match(pattern, date_string))