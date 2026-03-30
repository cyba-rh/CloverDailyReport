import sys
import os
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
CSV_OUTPUT_TEMPLATE = os.getenv('MERCHANT_CSV_OUTPUT', 'MerchantAccounts_%datetime%.csv')

QUERY = """
SELECT
    mg.merchant_group_id   AS MerchantGroupID,
    mg.name                AS MerchantGroupName,
    mg.address_line1       AS MerchantGroupAddress,
    mg.address_city        AS MerchantGroupCity,
    mg.address_state       AS MerchantGroupState,
    mg.address_postal_code AS MerchantGroupZip,
    ma.id                  AS MerchantAccountDatabaseID,
    ma.name                AS MerchantAccountName,
    ma.gateway_id          AS MerchantAccountGatewayID,
    ma.uuid                AS MerchantAccountUUID,
    ma.mid                 AS MerchantAccountMID,
    ma.gateway_merchant_id AS MerchantAccountGatewayMerchantID,
    ma.fiserv_sales_id     AS MerchantAccountFiservSalesID,
    ma.address_line1       AS MerchantAccountAddress,
    ma.address_city        AS MerchantAccountCity,
    ma.address_state       AS MerchantAccountState,
    ma.address_postal_code AS MerchantAccountZip,
    ma.created_at          AS MerchantAccountCreated,
    u.email                AS ContactEmail,
    u.first_name           AS ContactFirstName,
    u.last_name            AS ContactLastName
FROM practicepay_prod.merchant_account ma
INNER JOIN practicepay_prod.merchants mg ON mg.id = ma.merchant_id
INNER JOIN practicepay_prod.users u ON ma.location_contact_user_id = u.id
WHERE ma.status = 'active'
  AND mg.status = 'ACTIVE'
  AND mg.name NOT LIKE '%test%'
  AND mg.name NOT LIKE '%Team'
ORDER BY MerchantGroupName, MerchantAccountName
"""

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

def main():
    now_dt = datetime.now()
    csv_filename = CSV_OUTPUT_TEMPLATE.replace('%datetime%', now_dt.strftime('%Y%m%d_%H%M%S'))
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
            df = pd.read_sql(QUERY, conn)
            conn.close()
            df.to_csv(csv_filename, index=False)
            print(f"Data exported to {csv_filename} ({len(df)} rows)")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'testdb':
        test_db_connection()
    else:
        main()
