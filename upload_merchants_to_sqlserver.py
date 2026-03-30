import os
import glob
import shutil
import pandas as pd
import pyodbc
from dotenv import load_dotenv

# Column type mappings
INTEGER_COLS = ['MerchantAccountDatabaseID']
BIGINT_COLS  = ['MerchantGroupID', 'MerchantAccountMID', 'MerchantAccountFiservSalesID']
DATETIME_COLS = ['MerchantAccountCreated']

def get_env():
    load_dotenv()
    return {
        'SQLSERVER_HOST':     os.getenv('SQLSERVER_HOST'),
        'SQLSERVER_PORT':     os.getenv('SQLSERVER_PORT', '1433'),
        'SQLSERVER_USER':     os.getenv('SQLSERVER_USER'),
        'SQLSERVER_PASSWORD': os.getenv('SQLSERVER_PASSWORD'),
        'SQLSERVER_DB':       os.getenv('SQLSERVER_DB'),
        'ARCHIVE_DIR':        os.getenv('ARCHIVE_DIR', 'archived'),
    }

def upload_merchants(env):
    csv_files = glob.glob('MerchantAccounts_*.csv')
    if not csv_files:
        print('No MerchantAccounts CSV files found.')
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

    print("Connecting to SQL Server...")
    try:
        conn = pyodbc.connect(conn_str)
        conn.autocommit = False
        print("Connected successfully.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    cursor = conn.cursor()

    for csv_file in csv_files:
        print(f"\nProcessing {csv_file}...")
        df = pd.read_csv(csv_file, dtype=str)  # read everything as str first for safety

        # Clean and convert columns to proper types
        for col in df.columns:
            if col in INTEGER_COLS or col in BIGINT_COLS:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif col in DATETIME_COLS:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].where(df[col].notna(), None)
                df[col] = df[col].replace(['', ' ', 'NULL', 'null', 'None'], None)

        columns = df.columns.tolist()
        col_names = ', '.join([f'[{c}]' for c in columns])
        col_placeholders = ', '.join(['?'] * len(columns))

        try:
            # Truncate existing data
            print("Truncating CLOVERMERCHANTS table...")
            cursor.execute("TRUNCATE TABLE Landing_Operational.dbo.CLOVERMERCHANTS")
            print("Table truncated.")

            # Insert rows in batches
            batch_size = 200
            total_rows = len(df)
            print(f"Inserting {total_rows} rows...")

            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]

                for _, row in batch.iterrows():
                    sql_values = []
                    for col in columns:
                        val = row[col]
                        if pd.isna(val) if not isinstance(val, str) else val is None:
                            sql_values.append(None)
                        elif col in INTEGER_COLS or col in BIGINT_COLS:
                            try:
                                sql_values.append(int(val))
                            except (ValueError, TypeError):
                                sql_values.append(None)
                        elif col in DATETIME_COLS:
                            sql_values.append(str(val) if val else None)
                        else:
                            sql_values.append(str(val) if val is not None else None)

                    cursor.execute(
                        f"INSERT INTO Landing_Operational.dbo.CLOVERMERCHANTS ({col_names}) VALUES ({col_placeholders})",
                        *sql_values
                    )

                conn.commit()
                print(f"  Committed rows {start_idx + 1}-{end_idx} of {total_rows}")

            print(f"Upload complete: {total_rows} rows inserted into CLOVERMERCHANTS.")

            # Archive the CSV
            archive_dir = env['ARCHIVE_DIR']
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
            shutil.move(csv_file, os.path.join(archive_dir, os.path.basename(csv_file)))
            print(f"Archived {csv_file} to {archive_dir}/")

        except Exception as e:
            print(f"Error during upload: {e}")
            conn.rollback()
            raise

    cursor.close()
    conn.close()

if __name__ == '__main__':
    env = get_env()
    upload_merchants(env)
