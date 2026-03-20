from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64

user = Entity(
    name="user_id",
    join_keys=["user_id"],
)

fraud_source = FileSource(
    path="data/fraud_features.parquet",
    timestamp_field="event_timestamp",
)

user_fraud_features = FeatureView(
    name="user_fraud_features",
    entities=[user],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="transaction_velocity", dtype=Int64),
        Field(name="spend_deviation",      dtype=Float32),
        Field(name="time_since_last_txn",  dtype=Float32),
        Field(name="last_amount",          dtype=Float32),
    ],
    source=fraud_source,
)
