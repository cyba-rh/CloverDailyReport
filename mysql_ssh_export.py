import sys
def test_db_connection():
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_private_key=SSH_KEY_PATH,
            remote_bind_address=(DB_HOST, DB_PORT)
        ) as tunnel:
            conn = connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            cur = conn.cursor()
            cur.execute('SELECT 1')
            result = cur.fetchone()
            print('DB connection successful:', result)
            cur.close()
            conn.close()
    except Exception as e:
        print('DB connection failed:', e)
        sys.exit(1)
import os
import json
import pandas as pd
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from mysql.connector import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', 22))
SSH_USER = os.getenv('SSH_USER')
SSH_KEY_PATH = os.getenv('SSH_KEY_PATH')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TRACKING_FILE = os.getenv('TRACKING_FILE', 'tracking.json')
CSV_OUTPUT_TEMPLATE = os.getenv('CSV_OUTPUT', 'CloverDaily_%datetime%.csv')

# Get last run timestamp
def get_last_run():
    if not os.path.exists(TRACKING_FILE):
        return None
    with open(TRACKING_FILE, 'r') as f:
        data = json.load(f)
        return data.get('last_run')

def update_tracking(status, timestamp):
    with open(TRACKING_FILE, 'w') as f:
        json.dump({'last_run': timestamp, 'status': status}, f)

def main():
    now_dt = datetime.now()
    now = now_dt.strftime('%Y-%m-%d %H:%M:%S')
    csv_filename = CSV_OUTPUT_TEMPLATE.replace('%datetime%', now_dt.strftime('%Y%m%d_%H%M%S'))
    last_run = get_last_run()
    merchant_filter = "merchant_account_id > 8"
    if last_run:
        query = f"SELECT * FROM transaction_report WHERE updated_at > '{last_run}' AND updated_at <= '{now}' AND {merchant_filter}"
    else:
        query = f"SELECT * FROM transaction_report WHERE updated_at <= '{now}' AND {merchant_filter}"
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_private_key=SSH_KEY_PATH,
            remote_bind_address=(DB_HOST, DB_PORT)
        ) as tunnel:
            conn = connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            df = pd.read_sql(query, conn)
            df.to_csv(csv_filename, index=False)
            update_tracking('success', now)
            print(f"Data exported to {csv_filename}")
    except Exception as e:
        update_tracking('failure', now)
        print(f"Error: {e}")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'testdb':
        test_db_connection()
    else:
        main()
