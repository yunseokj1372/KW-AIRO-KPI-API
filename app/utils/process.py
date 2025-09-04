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
    # Combine all filters into one operation
    mask = (
        (df['SERVICETYPE'] == "IH") & 
        (df['WARRANTYSTATUS'].isin(['IW', 'YES', 'POP'])) &
        (df['STATUS'] == 60)
    )
    df = df[mask]
    
    # Convert dates once
    date_columns = ['COMPLETEDATE', 'ASSIGNDATE']
    df[date_columns] = df[date_columns].astype('datetime64[ns]')
    
    df['WEEK'] = df['COMPLETEDATE'].dt.isocalendar().week
    return df


def processing(df, serial):
    # Vectorized approach instead of loops
    df['REDO_CHECK'] = None
    
    # Group by serial number for faster processing
    grouped = df.groupby('SERIALNO')
    
    for k in serial:
        serial_data = grouped.get_group(k) if k in grouped.groups else pd.DataFrame()
        if serial_data.empty:
            continue
            
        serial_data = serial_data.sort_values('TICKETNO', ascending=False)
        
        # Vectorized date comparison
        check_date = serial_data['ASSIGNDATE'].iloc[0]
        redo_check = check_date - timedelta(days=90)
        mask = (serial_data['COMPLETEDATE'] >= redo_check) & (serial_data['COMPLETEDATE'] < check_date)
        
        if mask.any():
            # Use shift to get next ticket number without loop
            df.loc[serial_data.index, 'REDO_CHECK'] = serial_data['TICKETNO'].shift(-1)
    
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