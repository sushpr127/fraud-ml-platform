import json
import time
import os
import argparse
import pandas as pd
import boto3
from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

KAFKA_CONF = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'sasl.username':     os.getenv('KAFKA_API_KEY'),
    'sasl.password':     os.getenv('KAFKA_API_SECRET'),
}

S3_BUCKET = os.getenv('S3_BUCKET', 'fraud-raw-events')
s3_client = boto3.client(
    's3',
    region_name=           os.getenv('AWS_REGION', 'us-east-1'),
    aws_access_key_id=     os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'),
)

KEEP_COLS = [
    'TransactionID', 'TransactionDT', 'TransactionAmt',
    'ProductCD', 'card1', 'card2', 'card4', 'card6',
    'addr1', 'addr2', 'P_emaildomain', 'R_emaildomain',
    'isFraud'
]

def build_event(row: pd.Series) -> dict:
    return {
        'transaction_id': str(row['TransactionID']),
        'user_id':        f"u_{int(row['card1']) if pd.notna(row['card1']) else 0}",
        'merchant_id':    f"m_{row['ProductCD']}_{int(row['addr1']) if pd.notna(row['addr1']) else 0}",
        'amount':         float(row['TransactionAmt']),
        'timestamp':      int(row['TransactionDT']),
        'event_time':     datetime.utcnow().isoformat(),
        'card_type':      str(row['card4'])         if pd.notna(row['card4'])         else 'unknown',
        'email_domain':   str(row['P_emaildomain']) if pd.notna(row['P_emaildomain']) else 'unknown',
        'is_fraud':       int(row['isFraud']),
    }

def flush_to_s3(batch: list, batch_num: int):
    date_prefix = datetime.utcnow().strftime('%Y/%m/%d')
    key  = f"raw/{date_prefix}/batch_{batch_num:06d}.json"
    body = '\n'.join(json.dumps(e) for e in batch)
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body)
    print(f"  S3: wrote {len(batch)} events → s3://{S3_BUCKET}/{key}")

def delivery_report(err, msg):
    if err:
        print(f'  Kafka delivery failed: {err}')

def run(speed: float, limit: int, batch_size: int = 1000):
    producer  = Producer(KAFKA_CONF)
    df        = pd.read_csv('data/train_transaction.csv', usecols=KEEP_COLS)
    df        = df.head(limit) if limit else df
    interval  = 1.0 / (10 * speed)

    print(f"Streaming {len(df)} events at {speed}x speed "
          f"({int(10 * speed)} events/sec)...")

    s3_batch  = []
    batch_num = 0

    for i, (_, row) in enumerate(df.iterrows()):
        event = build_event(row)
        producer.produce(
            topic    = 'transactions-raw',
            key      = event['user_id'],
            value    = json.dumps(event).encode('utf-8'),
            callback = delivery_report,
        )
        producer.poll(0)

        s3_batch.append(event)
        if len(s3_batch) >= batch_size:
            flush_to_s3(s3_batch, batch_num)
            s3_batch  = []
            batch_num += 1

        if i % 500 == 0:
            print(f"  Sent {i}/{len(df)} events...")

        time.sleep(interval)

    if s3_batch:
        flush_to_s3(s3_batch, batch_num)

    producer.flush()
    print(f"\nDone. Sent {len(df)} events total.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--speed', type=float, default=1.0,
                        help='Speed multiplier (1=10/sec, 10=100/sec)')
    parser.add_argument('--limit', type=int,   default=5000,
                        help='Number of events to send (0=all)')
    parser.add_argument('--batch', type=int,   default=1000,
                        help='S3 batch size')
    args = parser.parse_args()
    run(args.speed, args.limit, args.batch)
