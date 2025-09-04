from app.utils.process import validate_date_format
import oracledb
import pandas as pd

def redo_input(start_date: str, end_date: str, connection: oracledb.Connection):
    # Validate date formats as additional safety
    if not validate_date_format(start_date) or not validate_date_format(end_date):
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got start: {start_date}, end: {end_date}")
    
    query = """
    WITH filtered_tickets AS (
        SELECT t.* 
        FROM opticket t
        WHERE t.COMPLETEDTIME BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') 
            AND TO_DATE(:end_date, 'YYYY-MM-DD')
        AND t.vendorid = 1
        AND t.systemid = 2
        AND t.servicetype = 'IH'
    )
    SELECT 
        t.ticketno,
        wh.nickname,
        t.ACCOUNTNO,
        t.buildername,
        t.PRODUCTTYPE,
        t.SERVICETYPE,
        t.modelno,
        t.SERIALNO,
        b.UPDATEDBY,
        CASE t.vendorid 
            WHEN 0 THEN 'Unknown'
            WHEN 1 THEN 'GSPN'
            -- ... other cases ...
        END as VendorID,
        t.WARRANTYSTATUS,
        TO_CHAR(t.ASSIGNDTIME, 'YYYY-MM-DD') AS assigndate,
        TO_CHAR(t.APTSTARTDTIME, 'YYYY-MM-DD') AS apptdate,
        TO_CHAR(t.issuedtime, 'YYYY-MM-DD') AS Opendate,
        TO_CHAR(t.COMPLETEDTIME, 'YYYY-MM-DD') AS completedate,
        TO_CHAR(t.COMPLETEDTIME, 'YYYY_MM') AS completemonth,
        b.status,
        t.BRAND,
        t.TECHID,
        U.FirstName || ' ' || U.LastName AS TechName
    FROM filtered_tickets t
    INNER JOIN nspwarehouses wh ON wh.warehouseid = t.warehouseid
    INNER JOIN opbase b ON b.id = t.id
    INNER JOIN nspusers u ON t.techid = u.userid
    """
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
    
    # Create parameterized query with proper placeholders for IN clause
    placeholders = ','.join([f':ticket_{i}' for i in range(len(sanitized_tickets))])
    
    query = f"""
    SELECT t.ticketno AS redotktno ,wh.nickname AS redoloc,t.ACCOUNTNO AS redoacct
    ,t.ASSIGNDTIME AS redoassigndate
    , t.completedtime AS redocalccomplete
    ,to_char(t.COMPLETEDTIME, 'mm/dd/yyyy') AS redocompletedate ,to_char(t.COMPLETEDTIME, 'yyyy_mm') AS redocompletemonth
    ,t.TECHID , U.FirstName || ' ' || U.LastName AS redotechname
    FROM opticket t
    INNER JOIN nspwarehouses wh ON wh.warehouseid = t.warehouseid
    INNER JOIN opbase b ON b.id = t.id
    INNER JOIN nspusers u ON t.techid = u.userid
    WHERE t.COMPLETEDTIME BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
    AND t.vendorid = 1
    AND t.systemid = 2
    AND t.servicetype = 'IH'
    AND t.TICKETNO IN ({placeholders})
    """
    
    # Build parameters dictionary
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    
    # Add ticket parameters
    for i, ticket in enumerate(sanitized_tickets):
        params[f'ticket_{i}'] = ticket
    
    return pd.read_sql(query, con=connection, params=params)