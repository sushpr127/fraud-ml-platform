import pickle
import os
import time
import numpy as np
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from feast import FeatureStore
from dotenv import load_dotenv
from datetime import datetime, timezone
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram, Gauge

load_dotenv()

app = FastAPI(title="Fraud Detection API")

Instrumentator().instrument(app).expose(app)

FRAUD_COUNTER = Counter(
    'fraud_predictions_total',
    'Total predictions by verdict',
    ['verdict']
)
FEATURE_FETCH_LATENCY = Histogram(
    'feast_feature_fetch_seconds',
    'Time to fetch features from Feast',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5]
)
FRAUD_PROBABILITY = Gauge(
    'last_fraud_probability',
    'Most recent fraud probability score'
)

print("Loading model...")
with open("models/fraud_xgb.pkl", "rb") as f:
    model = pickle.load(f)

with open("models/feature_meta.pkl", "rb") as f:
    meta = pickle.load(f)

FEATURE_COLS = meta["feature_cols"]

print("Loading Feast feature store...")
store = FeatureStore(repo_path="fraud_feast/feature_repo")

PRODUCT_CD_MAP = {"W": 0, "H": 1, "C": 2, "S": 3, "R": 4}

class TransactionRequest(BaseModel):
    user_id:     str
    amount:      float
    product_cd:  str = "W"

class PredictionResponse(BaseModel):
    user_id:           str
    fraud_probability: float
    verdict:           str
    feature_vector:    dict
    latency_ms:        float
    timestamp:         str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict", response_model=PredictionResponse)
def predict(req: TransactionRequest):
    start = time.time()

    t0 = time.time()
    feast_features = store.get_online_features(
        features=[
            "user_fraud_features:transaction_velocity",
            "user_fraud_features:spend_deviation",
            "user_fraud_features:time_since_last_txn",
            "user_fraud_features:last_amount",
        ],
        entity_rows=[{"user_id": req.user_id}],
    ).to_dict()
    feast_latency = time.time() - t0
    FEATURE_FETCH_LATENCY.observe(feast_latency)

    velocity   = feast_features["transaction_velocity"][0] or 0
    deviation  = feast_features["spend_deviation"][0]      or 0.0
    time_since = feast_features["time_since_last_txn"][0]  or 0.0
    last_amt   = feast_features["last_amount"][0]          or req.amount

    product_code = PRODUCT_CD_MAP.get(req.product_cd.upper(), 0)
    amount_log   = np.log1p(req.amount)

    feature_vector = {
        "TransactionAmt_log":   float(amount_log),
        "ProductCD":            int(product_code),
        "transaction_velocity": int(velocity),
        "spend_deviation":      float(deviation),
        "time_since_last_txn":  float(time_since),
        "last_amount":          float(last_amt),
    }

    X = pd.DataFrame([feature_vector])[FEATURE_COLS]
    fraud_prob = float(model.predict_proba(X)[0][1])
    verdict    = "FRAUD" if fraud_prob >= 0.5 else "LEGIT"

    FRAUD_COUNTER.labels(verdict=verdict).inc()
    FRAUD_PROBABILITY.set(fraud_prob)

    total_ms = (time.time() - start) * 1000

    print(f"  predict | user={req.user_id} "
          f"prob={fraud_prob:.4f} verdict={verdict} "
          f"latency={total_ms:.1f}ms feast={feast_latency*1000:.1f}ms")

    return PredictionResponse(
        user_id           = req.user_id,
        fraud_probability = round(fraud_prob, 4),
        verdict           = verdict,
        feature_vector    = feature_vector,
        latency_ms        = round(total_ms, 2),
        timestamp         = datetime.now(timezone.utc).isoformat(),
    )
