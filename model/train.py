import pandas as pd
import numpy as np
import xgboost as xgb
import pickle
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix
from dotenv import load_dotenv

load_dotenv()

KEEP_COLS = [
    'TransactionID', 'TransactionDT', 'TransactionAmt',
    'ProductCD', 'card1', 'addr1', 'isFraud'
]

print("Loading transaction data...")
df = pd.read_csv('data/train_transaction.csv', usecols=KEEP_COLS)

print("Loading Feast offline features...")
feast_df = pd.read_parquet('fraud_feast/feature_repo/data/fraud_features.parquet')

feast_df = feast_df[[
    'user_id',
    'transaction_velocity',
    'spend_deviation',
    'time_since_last_txn',
    'last_amount',
]].drop_duplicates(subset='user_id')

df['user_id'] = df['card1'].fillna(0).astype(int).apply(lambda x: f"u_{x}")

print("Merging transaction data with Feast features...")
merged = df.merge(feast_df, on='user_id', how='left')

merged['transaction_velocity'] = merged['transaction_velocity'].fillna(0).astype(int)
merged['spend_deviation']      = merged['spend_deviation'].fillna(0.0).astype(float)
merged['time_since_last_txn']  = merged['time_since_last_txn'].fillna(0.0).astype(float)
merged['last_amount']          = merged['last_amount'].fillna(0.0).astype(float)
merged['ProductCD']            = merged['ProductCD'].astype('category').cat.codes
merged['TransactionAmt_log']   = np.log1p(merged['TransactionAmt'])

FEATURE_COLS = [
    'TransactionAmt_log',
    'ProductCD',
    'transaction_velocity',
    'spend_deviation',
    'time_since_last_txn',
    'last_amount',
]

X = merged[FEATURE_COLS]
y = merged['isFraud']

print(f"Dataset shape: {X.shape}")
print(f"Fraud rate: {y.mean():.4f}")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {X_train.shape}, Test: {X_test.shape}")

fraud_count     = (y_train == 1).sum()
non_fraud_count = (y_train == 0).sum()
scale_pos_weight = non_fraud_count / fraud_count
print(f"scale_pos_weight: {scale_pos_weight:.2f}")

print("\nTraining XGBoost model...")
model = xgb.XGBClassifier(
    max_depth        = 6,
    n_estimators     = 300,
    learning_rate    = 0.05,
    scale_pos_weight = scale_pos_weight,
    eval_metric      = 'auc',
    early_stopping_rounds = 20,
    random_state     = 42,
    n_jobs           = -1,
)

model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    verbose=50,
)

y_pred_proba = model.predict_proba(X_test)[:, 1]
y_pred       = (y_pred_proba >= 0.5).astype(int)

auc  = roc_auc_score(y_test, y_pred_proba)
print(f"\n{'='*50}")
print(f"AUC-ROC:  {auc:.4f}")
print(f"\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['legit', 'fraud']))

os.makedirs('models', exist_ok=True)
model_path = 'models/fraud_xgb.pkl'
with open(model_path, 'wb') as f:
    pickle.dump(model, f)

feature_meta = {
    'feature_cols':      FEATURE_COLS,
    'auc':               auc,
    'scale_pos_weight':  scale_pos_weight,
    'n_estimators':      model.best_iteration + 1,
}
with open('models/feature_meta.pkl', 'wb') as f:
    pickle.dump(feature_meta, f)

print(f"\nModel saved to {model_path}")
print(f"Best iteration: {model.best_iteration + 1}")
print(f"Feature importances:")
for feat, imp in sorted(
    zip(FEATURE_COLS, model.feature_importances_),
    key=lambda x: x[1], reverse=True
):
    print(f"  {feat}: {imp:.4f}")
