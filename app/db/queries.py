from app.db.connection import get_connection
from app.utils.process import validate_date_format
import oracledb
import pandas as pd

def redo_input(start_date: str, end_date: str, connection: oracledb.Connection):
    # Validate date formats as additional safety
    if not validate_date_format(start_date) or not validate_date_format(end_date):
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got start: {start_date}, end: {end_date}")
    
    query = """
    SELECT t.ticketno ,wh.nickname,t.ACCOUNTNO , t.buildername, t.PRODUCTTYPE, t.SERVICETYPE , t.ACCOUNTNO ,  t.modelno, t.SERIALNO, b.UPDATEDBY
    , DECODE(t.vendorid, 0, 'Unknown', 1, 'GSPN', 2, 'SQ', 3, 'BF', 4, 'Asurion', 5, 'AIG', 6, 'Assurant', 7, 'ST', 8, 'LW') as VendorID
    , DECODE(t.systemid, 0, 'None', 1, 'IT', 2, 'GSPN', 3, 'SQ', 4, 'BF', 5, 'SVC_BENCH', 6, 'SVC_POWER') as SystemID
    ,t.WARRANTYSTATUS
    ,to_char(t.ASSIGNDTIME , 'mm/dd/yyyy') AS assigndate,to_char(t.APTSTARTDTIME , 'mm/dd/yyyy') AS apptdate
    ,to_char(t.issuedtime, 'mm/dd/yyyy') AS Opendate
    ,to_char(t.COMPLETEDTIME, 'mm/dd/yyyy') AS completedate ,to_char(t.COMPLETEDTIME, 'yyyy_mm')AS completemonth, b.status, t.BRAND, t.TECHID , U.FirstName || ' ' || U.LastName AS TechName
    FROM opticket t INNER JOIN nspwarehouses wh ON wh.warehouseid = t.warehouseid INNER JOIN opbase b ON b.id = t.id
    INNER JOIN nspusers u ON t.techid = u.userid
    WHERE t.COMPLETEDTIME BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
    AND t.vendorid = 1
    AND t.systemid = 2
    AND t.servicetype = 'IH'
    """

    # Use parameterized query
    return pd.read_sql(query, con=connection, params={'start_date': start_date, 'end_date': end_date})

def redo_output(tuple_tickets: tuple, start_date: str, end_date: str, connection: oracledb.Connection):
    # Validate date formats
    if not validate_date_format(start_date) or not validate_date_format(end_date):
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got start: {start_date}, end: {end_date}")
    
    # Validate and sanitize ticket numbers (should be integers)
    sanitized_tickets = []
    for ticket in tuple_tickets:
        try:
            # Ensure ticket is numeric to prevent injection
            ticket_int = int(str(ticket).strip())
            sanitized_tickets.append(ticket_int)
        except ValueError:
            raise ValueError(f"Invalid ticket number: {ticket}")
    
    if not sanitized_tickets:
        # Return empty DataFrame if no valid tickets
        return pd.DataFrame()