import pandas as pd
from datetime import date, timedelta, datetime
import re
import io
import base64

def validate_date_format(date_string):
    """Validate that date string matches YYYY-MM-DD format"""
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    return bool(re.match(pattern, date_string))


def main_filter(df):
    # df = df[(df['SYSTEMID'] == 'GSPN') &  (df['SERVICETYPE'] == "IH")]
    df = df[df['SERVICETYPE'] == "IH"]
    df = df[(df['WARRANTYSTATUS'] == 'IW')|(df['WARRANTYSTATUS'] == 'YES')|(df['WARRANTYSTATUS'] == 'POP')]
    df['COMPLETEDATE'] = df['COMPLETEDATE'].astype('datetime64[ns]')
    df['ASSIGNDATE'] = df['ASSIGNDATE'].astype('datetime64[ns]')
    df['WEEK'] = df['COMPLETEDATE'].dt.isocalendar().week
    df = df[df['STATUS']==60]
    return df


def processing(df,serial):
    for k in serial:
        serial_data = df[df['SERIALNO'] == k]
        serial_data = serial_data.sort_values('TICKETNO', ascending = False)
        check_date = serial_data['ASSIGNDATE'].iloc[0]
        redo_check = check_date - timedelta(days=90)
        redo = serial_data[(serial_data['COMPLETEDATE'] >= redo_check) &(serial_data['COMPLETEDATE'] < check_date)]
        final_filter = len(redo)
        if final_filter>0:
            for i in range(len(serial_data)-1):
                tkt = serial_data.iloc[i]['TICKETNO']
                sub_tkt = serial_data.iloc[i+1]['TICKETNO']
                df.loc[ df['TICKETNO'] == tkt,'REDO_CHECK'] = sub_tkt
    

    return df

def merge_with_redo(filtered_df,redo_df):
    result = pd.merge(filtered_df, redo_df, how='left', left_on='REDO_CHECK', right_on='REDOTKTNO')
    return result
    
def test_date_validation():
    assert validate_date_format("2024-01-15") == True
    assert validate_date_format("2024-1-15") == False  # Should fail
    assert validate_date_format("'; DROP TABLE--") == False  # SQL injection attempt
    assert validate_date_format("2024-01-32") == False  # Invalid date



def get_excel_base64(df):

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")

    buffer.seek(0)
    encoded = base64.b64encode(buffer.read()).decode("utf-8")

    return {"filename": "redo_output.xlsx", "file": encoded}