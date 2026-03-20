import pandas as pd
import os
import datetime
from datetime import timezone

KEEP_COLS = [
    'TransactionID', 'TransactionDT', 'TransactionAmt',
    'ProductCD', 'card1', 'addr1', 'isFraud'
]

print("Loading transaction data...")
df = pd.read_csv('data/train_transaction.csv', usecols=KEEP_COLS)

df['user_id'] = df['card1'].fillna(0).astype(int).apply(lambda x: f"u_{x}")

base_time = datetime.datetime(2019, 1, 1, tzinfo=timezone.utc)
df['event_timestamp'] = df['TransactionDT'].apply(
    lambda x: base_time + datetime.timedelta(seconds=int(x))
)

df = df.sort_values('event_timestamp').reset_index(drop=True)

print("Computing features...")
df['transaction_velocity'] = (
    df.groupby('user_id')['TransactionID']
    .transform('cumcount')
    .clip(upper=100)
    .astype('int64')
)

df['spend_deviation'] = (
    df.groupby('user_id')['TransactionAmt']
    .transform(lambda x: (x - x.expanding().mean()) /
               x.expanding().std().fillna(1))
    .fillna(0.0)
    .astype('float32')
)

df['time_since_last_txn'] = (
    df.groupby('user_id')['TransactionDT']
    .transform(lambda x: x.diff().fillna(0))
    .astype('float32')
)

df['last_amount'] = (
    df.groupby('user_id')['TransactionAmt']
    .transform('last')
    .astype('float32')
)

feast_df = df[[
    'user_id',
    'event_timestamp',
    'transaction_velocity',
    'spend_deviation',
    'time_since_last_txn',
    'last_amount',
]].copy()

os.makedirs('fraud_feast/feature_repo/data', exist_ok=True)
feast_df.to_parquet(
    'fraud_feast/feature_repo/data/fraud_features.parquet',
    index=False
)
print(f"Saved {len(feast_df)} rows")
print(feast_df.head(3))
