from feast import FeatureStore

store = FeatureStore(repo_path="fraud_feast/feature_repo")

entity_rows = [
    {"user_id": "u_13926"},
    {"user_id": "u_4521"},
    {"user_id": "u_0"},
]

print("Fetching features from online store...")
features = store.get_online_features(
    features=[
        "user_fraud_features:transaction_velocity",
        "user_fraud_features:spend_deviation",
        "user_fraud_features:time_since_last_txn",
        "user_fraud_features:last_amount",
    ],
    entity_rows=entity_rows,
).to_dict()

for i, uid in enumerate(["u_13926", "u_4521", "u_0"]):
    print(f"\nuser_id: {uid}")
    for k, v in features.items():
        if k != "user_id":
            print(f"  {k}: {v[i]}")
