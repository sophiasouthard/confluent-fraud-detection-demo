#!/usr/bin/env python3
"""
Banking Fraud Detection - Continuous Transaction Producer
========================================================
Produces banking transactions continuously to the `banking_transactions` Kafka
topic using Schema Registry JSON serialization (json-registry format).

Traffic profile:
- 5 fraud-heavy accounts receive 85% of transactions
- 5 normal accounts receive 15% of transactions
- Transactions are emitted every 0.5 to 1.5 seconds
- Timestamps use the current event time in milliseconds
- Runs until Ctrl+C and displays live stats

Usage:
  python produce_messages.py
"""

import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from itertools import count
import os

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

# ─── Load configuration ───────────────────────────────────────────────────────

load_dotenv()

REQUIRED_ENV_VARS = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
]

missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing:
    print(f"❌ Missing required environment variables: {', '.join(missing)}")
    print("   Run `terraform apply` first, or copy python/.env.example → python/.env")
    sys.exit(1)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

TOPIC = "banking_transactions"
FRAUD_ACCOUNTS = [f"ACCOUNT-FRAUD-0{i}" for i in range(1, 6)]
NORMAL_ACCOUNTS = [f"ACCOUNT-1000{i}" for i in range(1, 6)]
TRANSACTION_TYPES = [
    "POS_PURCHASE",
    "ATM_WITHDRAWAL",
    "ONLINE_TRANSFER",
    "WIRE_TRANSFER",
    "MOBILE_PAYMENT",
    "BILL_PAYMENT",
]
CHANNELS = ["ATM", "MOBILE", "ONLINE", "BRANCH", "POS"]
MERCHANT_PREFIXES = ["AMZ", "TGT", "WMT", "STR", "TRV", "DIN", "GAS", "TEL"]

# ─── Schema Registry client ───────────────────────────────────────────────────

sr_client = SchemaRegistryClient(
    {
        "url": SR_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    }
)

print("🔍 Fetching schemas from Schema Registry …")
try:
    key_schema = sr_client.get_latest_version(f"{TOPIC}-key").schema
    value_schema = sr_client.get_latest_version(f"{TOPIC}-value").schema
    print("✅ Schemas retrieved successfully")
except Exception as exc:
    print(f"❌ Could not retrieve schemas: {exc}")
    print(
        "   Ensure `terraform apply` has completed and Flink has registered the schemas."
    )
    sys.exit(1)

# ─── Serializers ──────────────────────────────────────────────────────────────

key_serializer = JSONSerializer(key_schema.schema_str, sr_client)
value_serializer = JSONSerializer(value_schema.schema_str, sr_client)

# ─── Kafka producer ───────────────────────────────────────────────────────────

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
    }
)

# ─── Runtime stats ────────────────────────────────────────────────────────────

success_count = 0
failure_count = 0
account_counts = Counter()
transaction_type_counts = Counter()
amount_by_segment = Counter()
start_time = time.time()


def choose_account():
    if random.random() < 0.85:
        return random.choice(FRAUD_ACCOUNTS), "fraud"
    return random.choice(NORMAL_ACCOUNTS), "normal"



def random_amount(segment, transaction_type):
    if segment == "fraud":
        ranges = {
            "POS_PURCHASE": (150.00, 1800.00),
            "ATM_WITHDRAWAL": (200.00, 1000.00),
            "ONLINE_TRANSFER": (500.00, 4500.00),
            "WIRE_TRANSFER": (1000.00, 9000.00),
            "MOBILE_PAYMENT": (100.00, 950.00),
            "BILL_PAYMENT": (120.00, 1500.00),
        }
    else:
        ranges = {
            "POS_PURCHASE": (5.00, 220.00),
            "ATM_WITHDRAWAL": (20.00, 300.00),
            "ONLINE_TRANSFER": (40.00, 900.00),
            "WIRE_TRANSFER": (100.00, 2500.00),
            "MOBILE_PAYMENT": (5.00, 180.00),
            "BILL_PAYMENT": (25.00, 650.00),
        }

    low, high = ranges[transaction_type]
    amount = Decimal(str(random.uniform(low, high))).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    return float(amount)



def build_transaction(sequence_number):
    account_id, segment = choose_account()
    transaction_type = random.choice(TRANSACTION_TYPES)
    now = datetime.now(timezone.utc)

    transaction = {
        "account_id": account_id,
        "transaction_id": f"TXN-LIVE-{sequence_number:08d}",
        "amount": random_amount(segment, transaction_type),
        "transaction_type": transaction_type,
        "merchant_id": f"{random.choice(MERCHANT_PREFIXES)}-{random.randint(1000, 9999)}",
        "channel": random.choice(CHANNELS),
        "transaction_time": int(now.timestamp() * 1000),
    }
    return transaction, segment



def print_stats(last_transaction):
    elapsed = max(time.time() - start_time, 1e-9)
    rate = success_count / elapsed
    fraud_total = sum(account_counts[account] for account in FRAUD_ACCOUNTS)
    normal_total = sum(account_counts[account] for account in NORMAL_ACCOUNTS)

    print("\n" + "─" * 72)
    print(
        f"📊 Live Stats | delivered={success_count} failed={failure_count} "
        f"rate={rate:.2f}/sec elapsed={elapsed:.1f}s"
    )
    print(f"🚨 Fraud accounts:  {fraud_total} ({(fraud_total / max(success_count, 1)) * 100:.1f}%)")
    print(f"✅ Normal accounts: {normal_total} ({(normal_total / max(success_count, 1)) * 100:.1f}%)")
    print(
        "💳 Transaction types: "
        + ", ".join(
            f"{txn_type}={transaction_type_counts[txn_type]}"
            for txn_type in TRANSACTION_TYPES
        )
    )
    print(
        f"💰 Amount totals | fraud=${amount_by_segment['fraud']:.2f} "
        f"normal=${amount_by_segment['normal']:.2f}"
    )
    print(
        "🧾 Last txn: "
        f"{last_transaction['transaction_id']} "
        f"account={last_transaction['account_id']} "
        f"type={last_transaction['transaction_type']} "
        f"amount=${last_transaction['amount']:.2f}"
    )



def delivery_callback(err, msg):
    global success_count, failure_count
    if err:
        print(f"❌ Delivery failed: {err}")
        failure_count += 1
    else:
        success_count += 1


print("\n▶️  Starting continuous producer. Press Ctrl+C to stop.")

last_stats_time = 0.0
last_transaction = None

try:
    for sequence_number in count(1):
        transaction, segment = build_transaction(sequence_number)
        key = {"account_id": transaction["account_id"]}
        value = {k: v for k, v in transaction.items() if k != "account_id"}

        try:
            producer.produce(
                topic=TOPIC,
                key=key_serializer(key, SerializationContext(TOPIC, MessageField.KEY)),
                value=value_serializer(value, SerializationContext(TOPIC, MessageField.VALUE)),
                callback=delivery_callback,
            )
            producer.poll(0)
        except Exception as exc:
            print(f"❌ Error producing message for {transaction['transaction_id']}: {exc}")
            failure_count += 1
        else:
            account_counts[transaction["account_id"]] += 1
            transaction_type_counts[transaction["transaction_type"]] += 1
            amount_by_segment[segment] += transaction["amount"]
            last_transaction = transaction

        now = time.time()
        if last_transaction and now - last_stats_time >= 2:
            print_stats(last_transaction)
            last_stats_time = now

        time.sleep(random.uniform(0.5, 1.5))
except KeyboardInterrupt:
    print("\n\n🛑 Stopping producer …")
finally:
    producer.flush()
    if last_transaction:
        print_stats(last_transaction)
    print("✅ Producer stopped cleanly")
