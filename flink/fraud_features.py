import os
import json
import redis
import time
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from datetime import datetime, timezone
import statistics

load_dotenv()

KAFKA_CONF = {
    'bootstrap.servers':  os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol':  'SASL_SSL',
    'sasl.mechanism':     'PLAIN',
    'sasl.username':      os.getenv('KAFKA_API_KEY'),
    'sasl.password':      os.getenv('KAFKA_API_SECRET'),
    'group.id':           'flink-fraud-consumer',
    'auto.offset.reset':  'earliest',
}

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# In-memory windows per user
velocity_window   = defaultdict(list)   # user_id → [timestamps]
spend_window      = defaultdict(list)   # user_id → [amounts]
last_txn_time     = defaultdict(float)  # user_id → last timestamp

VELOCITY_WINDOW_SEC  = 300    # 5 minutes
SPEND_WINDOW_SEC     = 3600   # 1 hour

def now_sec():
    return datetime.now(timezone.utc).timestamp()

def compute_and_store(user_id: str, amount: float, ts: float):
    now = now_sec()

    # --- Feature 1: transaction velocity (last 5 min) ---
    velocity_window[user_id].append(ts)
    velocity_window[user_id] = [
        t for t in velocity_window[user_id] if now - t <= VELOCITY_WINDOW_SEC
    ]
    txn_velocity = len(velocity_window[user_id])

    # --- Feature 2: spend deviation (rolling z-score, last 1hr) ---
    spend_window[user_id].append(amount)
    spend_window[user_id] = [
        a for a in spend_window[user_id] if len(spend_window[user_id]) <= 100
    ]
    amounts = spend_window[user_id]
    if len(amounts) >= 2:
        mean    = statistics.mean(amounts)
        stddev  = statistics.stdev(amounts)
        z_score = (amount - mean) / stddev if stddev > 0 else 0.0
    else:
        z_score = 0.0

    # --- Feature 3: time since last transaction ---
    prev_ts = last_txn_time.get(user_id, ts)
    time_since_last = ts - prev_ts
    last_txn_time[user_id] = ts

    # --- Write all features to Redis ---
    r.hset(f"features:{user_id}", mapping={
        'transaction_velocity': txn_velocity,
        'spend_deviation':      round(z_score, 4),
        'time_since_last_txn':  round(time_since_last, 2),
        'last_amount':          amount,
        'updated_at':           datetime.now(timezone.utc).isoformat(),
    })

    return txn_velocity, round(z_score, 4), round(time_since_last, 2)

def run():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(['transactions-raw'])

    print("Flink feature processor started — consuming from Kafka...")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("-" * 60)

    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}")
                break

            try:
                event    = json.loads(msg.value().decode('utf-8'))
                user_id  = event['user_id']
                amount   = float(event['amount'])
                ts       = float(event['timestamp'])

                velocity, z_score, time_since = compute_and_store(
                    user_id, amount, ts
                )

                processed += 1
                if processed % 100 == 0:
                    print(f"  [{processed}] user={user_id} "
                          f"velocity={velocity} "
                          f"z_score={z_score} "
                          f"time_since={time_since}s")

            except Exception as e:
                print(f"  Error processing message: {e}")

    except KeyboardInterrupt:
        print(f"\nStopped. Processed {processed} events.")
    finally:
        consumer.close()

if __name__ == '__main__':
    run()
