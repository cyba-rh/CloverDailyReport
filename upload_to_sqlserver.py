        # ...existing code...
import os
import pandas as pd
import shutil
from dotenv import load_dotenv
import pyodbc

def get_env():
    load_dotenv()
    return {
        'SQLSERVER_HOST': os.getenv('SQLSERVER_HOST'),
        'SQLSERVER_PORT': os.getenv('SQLSERVER_PORT', '1433'),
        'SQLSERVER_USER': os.getenv('SQLSERVER_USER'),
        'SQLSERVER_PASSWORD': os.getenv('SQLSERVER_PASSWORD'),
        'SQLSERVER_DB': os.getenv('SQLSERVER_DB'),
        'ARCHIVE_DIR': os.getenv('ARCHIVE_DIR', 'archived'),
    }

import glob

def upload_csv_to_sqlserver(env):
    archive_dir = env['ARCHIVE_DIR']
    csv_files = glob.glob('CloverDaily_*.csv')
    if not csv_files:
        print('No CloverDaily CSV files found.')
        return
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={env['SQLSERVER_HOST']},{env['SQLSERVER_PORT']};"
        f"DATABASE={env['SQLSERVER_DB']};"
        f"UID={env['SQLSERVER_USER']};"
        f"PWD={env['SQLSERVER_PASSWORD']};"
        f"Encrypt=no;"
        f"Connection Timeout=30;"
        f"Command Timeout=60"
    )
    
    print("Attempting to connect to SQL Server...")
    try:
        conn = pyodbc.connect(conn_str)
        conn.autocommit = False
        print("Connected successfully")
    except Exception as e:
        print(f"Connection failed: {e}")
        return
    cursor = conn.cursor()
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        # Remove duplicates based on transaction_uuid
        if 'transaction_uuid' in df.columns:
            df = df.drop_duplicates(subset=['transaction_uuid'])
        else:
            print(f"transaction_uuid column not found in {csv_file}, skipping.")
            continue
        columns = df.columns.tolist()
        # Auto-clean numeric columns
        numeric_cols = ['amount', 'refunded_amount']
        integer_cols = ['merchant_account_id', 'mid', 'amount', 'refunded_amount', 'source_first6', 'payment_method_details']
        
        for col in columns:
            if col in integer_cols:
                # Convert to integer, handling decimals and empty values
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
            elif col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
            elif df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                df[col] = df[col].fillna(0)
                df[col] = df[col].replace('', 0)
                if 'float' in str(df[col].dtype):
                    df[col] = df[col].round(2)
            else:
                # Handle various empty values by converting to None
                df[col] = df[col].replace(['', ' ', 'NULL', 'null', 'None'], None)
                
        # Fill all remaining NaN values with None
        df = df.where(pd.notnull(df), None)
        # Debug: Print data types and sample values
        print(f"Processing {csv_file}")
        print("Column data types:")
        print(df.dtypes)
        print("\nSample values from each column:")
        for col in columns:
            print(f"{col}: {df[col].iloc[0:3].tolist()}")
        
        # Debug: Print problematic values in amount/refunded_amount
        for col in numeric_cols:
            # Print values with NaN, non-numeric, or excessive precision
            nan_mask = df[col].isna()
            non_numeric_mask = df[col].apply(lambda x: not isinstance(x, (int, float)))
            excessive_precision_mask = df[col].apply(lambda x: isinstance(x, float) and len(str(x).split('.')[-1]) > 2)
            combined_mask = nan_mask | non_numeric_mask | excessive_precision_mask
            if combined_mask.any():
                print(f"Problematic values found in column '{col}':")
                print(df.loc[combined_mask, col])
        
        # Debug: Check all column data types after cleaning
        print("\nData types after cleaning:")
        for col in columns:
            sample_val = df[col].iloc[0] if len(df) > 0 else None
            print(f"{col}: {type(sample_val)} - sample: {sample_val}")
        print("First row data types:")
        for i, col in enumerate(columns):
            val = df[col].iloc[0] if len(df) > 0 else None
            print(f"  {i+1}. {col}: {val} ({type(val)})")
        col_placeholders = ', '.join(['?'] * len(columns))
        col_names = ', '.join([f'[{col}]' for col in columns])
        update_set = ', '.join([f'target.[{col}] = source.[{col}]' for col in columns if col != 'transaction_uuid'])
        insert_cols = ', '.join([f'[{col}]' for col in columns])
        insert_values = ', '.join([f'source.[{col}]' for col in columns])
        
        # Print column positions for debugging
        print("Column positions:")
        for i, col in enumerate(columns):
            print(f"Parameter {i+1}: {col}")
        
        # Process data in batches for better performance
        batch_size = 100  # Smaller batches to avoid blocking
        total_rows = len(df)
        
        print(f"Starting batch processing of {total_rows} rows...")
        
        try:
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                print(f"Processing batch {start_idx+1}-{end_idx}...")
                
                for row_idx, (_, row) in enumerate(batch_df.iterrows()):
                    try:
                        # Convert values to proper types for SQL
                        sql_values = []
                        integer_cols = ['merchant_account_id', 'mid', 'amount', 'refunded_amount', 'source_first6', 'payment_method_details']
                        
                        for i, col in enumerate(columns):
                            val = row[col]
                            if pd.isna(val) or val is None or val == '' or str(val).lower() in ['nan', 'none', 'null']:
                                converted_val = None
                            elif col in integer_cols:
                                try:
                                    if isinstance(val, str):
                                        converted_val = int(float(val)) if val.strip() != '' else None
                                    else:
                                        converted_val = int(val)
                                except (ValueError, TypeError):
                                    converted_val = None
                            elif col in ['transaction_date', 'created_at', 'updated_at']:
                                converted_val = str(val) if val not in ['', ' ', None] else None
                            else:
                                converted_val = str(val) if val not in ['', None] else None
                            
                            sql_values.append(converted_val)
                        
                        cursor.execute(f"INSERT INTO Landing_Operational.dbo.CLOVERTRANS ({col_names}) VALUES ({col_placeholders})", *sql_values)
                        
                    except Exception as row_error:
                        print(f"Error processing row {start_idx + row_idx + 1}: {row_error}")
                        continue
                
                # Commit each batch
                conn.commit()
                print(f"Committed batch {start_idx+1}-{end_idx} ({end_idx}/{total_rows} rows)")
                
        except Exception as batch_error:
            print(f"Batch processing error: {batch_error}")
            conn.rollback()
            raise
        # Move file to archive
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
        shutil.move(csv_file, os.path.join(archive_dir, os.path.basename(csv_file)))
        print(f"Uploaded and archived {csv_file}")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    env = get_env()
    upload_csv_to_sqlserver(env)
