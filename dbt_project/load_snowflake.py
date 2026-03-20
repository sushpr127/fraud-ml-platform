import pandas as pd
import snowflake.connector
import json
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

print("Loading transaction data...")
KEEP_COLS = [
    'TransactionID', 'TransactionDT', 'TransactionAmt',
    'ProductCD', 'card1', 'addr1', 'P_emaildomain',
    'card4', 'isFraud'
]
df = pd.read_csv('data/train_transaction.csv', usecols=KEEP_COLS)
df = df.head(10000)

df['user_id']     = df['card1'].fillna(0).astype(int).apply(lambda x: f"u_{x}")
df['merchant_id'] = (df['ProductCD'] + '_' +
                     df['addr1'].fillna(0).astype(int).astype(str)).apply(lambda x: f"m_{x}")

print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account   = os.getenv('SNOWFLAKE_ACCOUNT'),
    user      = os.getenv('SNOWFLAKE_USER'),
    password  = os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse = 'COMPUTE_WH',
    database  = 'FRAUD_DB',
    schema    = 'RAW',
    role      = 'ACCOUNTADMIN',
)
cur = conn.cursor()

cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
print("Context:", cur.fetchone())

base_time = datetime.datetime(2019, 1, 1)

print("Inserting rows one by one (first 1000 rows)...")
df = df.head(1000)
total = 0

for _, row in df.iterrows():
    event_time = base_time + datetime.timedelta(seconds=int(row['TransactionDT']))
    record = {
        'transaction_id': str(row['TransactionID']),
        'user_id':        row['user_id'],
        'merchant_id':    row['merchant_id'],
        'amount':         float(row['TransactionAmt']),
        'timestamp':      int(row['TransactionDT']),
        'event_time':     event_time.isoformat(),
        'card_type':      str(row['card4']) if pd.notna(row['card4']) else 'unknown',
        'email_domain':   str(row['P_emaildomain']) if pd.notna(row['P_emaildomain']) else 'unknown',
        'is_fraud':       int(row['isFraud']),
    }
    cur.execute(
        "INSERT INTO FRAUD_DB.RAW.RAW_TRANSACTIONS (RECORD_CONTENT, RECORD_METADATA) "
        "SELECT PARSE_JSON(%s), PARSE_JSON(%s)",
        (json.dumps(record), '{}')
    )
    total += 1
    if total % 100 == 0:
        print(f"  Inserted {total} rows...")

conn.commit()
cur.execute("SELECT COUNT(*) FROM FRAUD_DB.RAW.RAW_TRANSACTIONS")
count = cur.fetchone()[0]
print(f"\nTotal rows in RAW_TRANSACTIONS: {count}")
conn.close()
print("Done.")
