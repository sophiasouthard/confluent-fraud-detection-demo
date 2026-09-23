#!/usr/bin/env python3
"""
generate_sample_data.py
Generates a JSON file of player events timed to land within a single Flink
tumbling window — guarantees PLAYER-RISK-01 crosses both thresholds
(>20 bets AND >$10k wagered) within the next complete 1-minute window.

Usage:
    python generate_sample_data.py
    python produce_player_events.py --sample-file player_events_sample.json
"""

import json
import uuid
from datetime import datetime, timezone, timedelta

OUTPUT_FILE = "player_events_sample.json"


def next_full_minute() -> datetime:
    """Return the start of the next full UTC minute."""
    now = datetime.now(timezone.utc)
    return (now + timedelta(minutes=1)).replace(second=0, microsecond=0)


def make_event(player_id: str, t: datetime, amount: float) -> dict:
    return {
        "player_id":  player_id,
        "session_id": str(uuid.uuid4()),
        "bet_id":     str(uuid.uuid4()),
        "amount":     amount,
        "game_type":  "SLOTS",
        "channel":    "ONLINE",
        "device_id":  f"DEV-{uuid.uuid4().hex[:4].upper()}",
        "event_time": t.isoformat(),
    }


def generate():
    window_start = next_full_minute()
    window_end   = window_start + timedelta(minutes=1)

    print(f"\n📅 Target window : {window_start.strftime('%H:%M:%S')} → {window_end.strftime('%H:%M:%S')} UTC")
    print(f"⏱  Produce these events before {window_end.strftime('%H:%M:%S')} UTC\n")

    events = []

    # 25 bets for PLAYER-RISK-01 (crosses >20 bet threshold)
    # Spread across the window, total wagered $15,000 (crosses >$10k)
    n_bets   = 25
    step_ms  = 50_000 // n_bets   # spread across ~50 seconds of the window
    for i in range(n_bets):
        t = window_start + timedelta(milliseconds=i * step_ms)
        events.append(make_event("PLAYER-RISK-01", t, 600.0))

    # 5 normal player events — one per player, low bets
    for j in range(5):
        t = window_start + timedelta(seconds=j * 8)
        events.append(make_event(f"PLAYER-NORMAL-0{j+1}", t, 50.0))

    with open(OUTPUT_FILE, "w") as f:
        json.dump(events, f, indent=2)

    print(f"✅ Written {len(events)} events to {OUTPUT_FILE}")
    print(f"   PLAYER-RISK-01: {n_bets} bets × $600 = ${n_bets * 600:,} total wagered")
    print(f"\n   Next: python produce_player_events.py --sample-file {OUTPUT_FILE}")


if __name__ == "__main__":
    generate()
