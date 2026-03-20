import pandas as pd

KEEP_COLS = [
    'TransactionID', 'TransactionDT', 'TransactionAmt',
    'ProductCD', 'card1', 'card2', 'card4', 'card6',
    'addr1', 'addr2', 'P_emaildomain', 'R_emaildomain',
    'isFraud'
]

df = pd.read_csv('data/train_transaction.csv', usecols=KEEP_COLS)

print("Shape:", df.shape)
print("\nFraud distribution:")
print(df['isFraud'].value_counts())
print("Fraud rate:", df['isFraud'].mean())
print("\nNull counts in kept columns:")
print(df.isnull().sum())
print("\nSample row:")
print(df.iloc[0].to_dict())
