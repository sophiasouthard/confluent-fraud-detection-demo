#!/usr/bin/env python3
"""
produce_player_events.py
Streams casino player bet events to the player_events Kafka topic
using Schema Registry JSON serialization.

80% of volume: PLAYER-RISK-01..05  (rapid high-value bets — triggers Flink flag)
20% of volume: PLAYER-NORMAL-01..05 (casual play — stays below thresholds)

Schema note:
  The registered player_events-value schema (written by Flink/Connect) stores
  player_id as the Kafka message KEY, not in the value payload.
  All value fields use nullable oneOf:[null, type] patterns.
  This producer aligns with that registered schema.

Usage:
    python produce_player_events.py                              # continuous ~2/sec
    python produce_player_events.py --sample-file <file.json>   # one-shot batch
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

load_dotenv()

# ── Config from .env ──────────────────────────────────────────────────────────
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_SEC = os.environ["KAFKA_API_SECRET"]
SR_URL    = os.environ["SCHEMA_REGISTRY_URL"]
SR_KEY    = os.environ["SCHEMA_REGISTRY_API_KEY"]
SR_SEC    = os.environ["SCHEMA_REGISTRY_API_SECRET"]
TOPIC     = "player_events"

# ── JSON Schema — matches the Flink/Connect-registered schema for player_events-value
# player_id is the Kafka message key; it is NOT in the value payload.
# All value fields are nullable (oneOf: [null, type]) — Confluent Flink/Connect convention.
SCHEMA_STR = json.dumps({
    "additionalProperties": False,
    "title": "Record",
    "type": "object",
    "properties": {
        "session_id": {"connect.index": 0, "oneOf": [{"type": "null"}, {"type": "string"}]},
        "bet_id":     {"connect.index": 1, "oneOf": [{"type": "null"}, {"type": "string"}]},
        "amount":     {"connect.index": 2, "oneOf": [{"type": "null"}, {"type": "number"}]},
        "game_type":  {"connect.index": 3, "oneOf": [{"type": "null"}, {"type": "string"}]},
        "channel":    {"connect.index": 4, "oneOf": [{"type": "null"}, {"type": "string"}]},
        "device_id":  {"connect.index": 5, "oneOf": [{"type": "null"}, {"type": "string"}]},
        "event_time": {"connect.index": 6, "oneOf": [{"type": "null"}, {"type": "string"}]},
    },
})

# ── Player & event data ───────────────────────────────────────────────────────
RISK_PLAYERS   = [f"PLAYER-RISK-{i:02d}"   for i in range(1, 6)]
NORMAL_PLAYERS = [f"PLAYER-NORMAL-{i:02d}" for i in range(1, 6)]
GAME_TYPES     = ["BLACKJACK", "SLOTS", "ROULETTE", "POKER", "SPORTS_BOOK"]
CHANNELS       = ["FLOOR", "ONLINE", "MOBILE", "KIOSK"]


def make_event(player_id: str) -> dict:
    is_risk = player_id.startswith("PLAYER-RISK")
    amount  = round(random.uniform(500, 5000) if is_risk else random.uniform(10, 200), 2)
    return {
        "player_id":  player_id,
        "session_id": str(uuid.uuid4()),
        "bet_id":     str(uuid.uuid4()),
        "amount":     amount,
        "game_type":  random.choice(GAME_TYPES),
        "channel":    random.choice(CHANNELS),
        "device_id":  f"DEV-{random.randint(1000, 9999)}",
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def event_to_dict(event, ctx):
    # player_id is the message key — exclude it from the value payload
    return {k: v for k, v in event.items() if k != "player_id"}


def delivery_report(err, msg):
    if err:
        print(f"  ❌ Delivery failed: {err}")


def build_producer():
    sr_client = SchemaRegistryClient({
        "url":                  SR_URL,
        "basic.auth.user.info": f"{SR_KEY}:{SR_SEC}",
    })
    json_serializer = JSONSerializer(SCHEMA_STR, sr_client, event_to_dict)
    return SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     KAFKA_KEY,
        "sasl.password":     KAFKA_SEC,
        "key.serializer":    StringSerializer("utf_8"),
        "value.serializer":  json_serializer,
    })


def run_continuous():
    producer  = build_producer()
    delivered = 0
    failed    = 0
    start     = time.time()
    last_stat = start
    counts    = {}

    print(f"\n🎰 Producing to topic '{TOPIC}' — Ctrl+C to stop\n")

    while True:
        # 80% risk players, 20% normal
        player_id = (
            random.choice(RISK_PLAYERS)
            if random.random() < 0.8
            else random.choice(NORMAL_PLAYERS)
        )
        event = make_event(player_id)
        counts[player_id] = counts.get(player_id, 0) + 1

        producer.produce(
            topic=TOPIC,
            key=player_id,
            value=event,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        delivered += 1

        now = time.time()
        if now - last_stat >= 5:
            elapsed = now - start
            rate    = delivered / elapsed
            print(f"📊 delivered={delivered}  failed={failed}  rate={rate:.2f}/sec  elapsed={elapsed:.0f}s")
            flagged = [p for p, c in counts.items() if c > 20 and p.startswith("PLAYER-RISK")]
            if flagged:
                print(f"   🚨 Flag threshold reached (>20 bets): {', '.join(flagged)}")
            last_stat = now

        time.sleep(random.uniform(0.4, 0.6))   # ~2 events/sec


def run_sample_file(path: str):
    with open(path) as f:
        events = json.load(f)

    producer = build_producer()

    print(f"\n📤 Producing {len(events)} events from {path}\n")
    for event in events:
        producer.produce(
            topic=TOPIC,
            key=event["player_id"],
            value=event,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    producer.flush()
    print(f"\n✅ Done — {len(events)} events produced to '{TOPIC}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Player events Kafka producer")
    parser.add_argument(
        "--sample-file",
        help="Path to JSON sample file for guaranteed one-shot window trigger",
    )
    args = parser.parse_args()

    if args.sample_file:
        run_sample_file(args.sample_file)
    else:
        run_continuous()
