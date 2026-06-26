#!/usr/bin/env python3
"""
generate_sample_data.py
========================
Generates window-aligned sample transaction data for the fraud detection demo.

Run this BEFORE produce_messages.py to create python/sample-transactions.json
with timestamps that align to the next 5-minute window boundary.

Why window alignment matters:
  Flink TUMBLE windows align to clock boundaries (00:00, 00:05, 00:10, …).
  If events span two window boundaries, counts are split and the threshold
  check (COUNT(*) > 10) produces wrong results.

Usage:
  python generate_sample_data.py
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

# ─── Window alignment ─────────────────────────────────────────────────────────

now = datetime.now()
# Round UP to the next 5-minute boundary (+1 minute safety buffer)
next_boundary_minute = ((now.minute // 5) + 1) * 5
overflow = next_boundary_minute >= 60

base_time = now.replace(
    minute=next_boundary_minute if not overflow else 0,
    second=10,
    microsecond=0,
)
if overflow:
    base_time += timedelta(hours=1)

base_ms = int(base_time.timestamp() * 1000)

# All events must land before the window closes (5 min = 300 s; keep 20 s buffer)
window_duration_ms = (300 - 20) * 1000  # 280 000 ms

# Watermark-advance event: 6 minutes after window start (forces window close)
watermark_ms = base_ms + 6 * 60 * 1000

print(f"📅 Window start : {base_time.strftime('%H:%M:%S')}")
print(f"📅 Window end   : {(base_time + timedelta(minutes=5)).strftime('%H:%M:%S')}")
print(f"📅 Watermark    : {(base_time + timedelta(minutes=6)).strftime('%H:%M:%S')}")
print()

# ─── Template-based data generation ──────────────────────────────────────────

# Read the template (uses placeholder strings)
template_file = Path(__file__).parent / "sample-transactions-template.json"
template_text = template_file.read_text()

# Offsets for each __WINDOW_T_PLUS_Xms__ placeholder
# 12 suspect events spread evenly across 280 000 ms window
# 3 normal events, 10 borderline events, 1 watermark event
spacing = window_duration_ms // 12  # ~23 333 ms between suspect events

replacements = {}
for i in range(13):  # TXN-001 through TXN-001-012 + watermark
    offset = i * spacing
    replacements[f"__WINDOW_T_PLUS_{(i + 0) * 20000}__"] = base_ms + offset

# Pre-compute all offsets used in the template
offset_map = {
    10000:  base_ms + int(spacing * 0),
    30000:  base_ms + int(spacing * 1),
    50000:  base_ms + int(spacing * 2),
    70000:  base_ms + int(spacing * 3),
    90000:  base_ms + int(spacing * 4),
    110000: base_ms + int(spacing * 5),
    130000: base_ms + int(spacing * 6),
    150000: base_ms + int(spacing * 7),
    170000: base_ms + int(spacing * 8),
    190000: base_ms + int(spacing * 9),
    210000: base_ms + int(spacing * 10),
    230000: base_ms + int(spacing * 11),
    # ACC-002-NORMAL: 3 events
    20000:  base_ms + int(spacing * 0.5),
    160000: base_ms + int(spacing * 6.5),
    # ACC-003-BORDERLINE: 10 events
    15000:  base_ms + int(spacing * 0.3),
    45000:  base_ms + int(spacing * 1.7),
    75000:  base_ms + int(spacing * 3.0),
    105000: base_ms + int(spacing * 4.3),
    135000: base_ms + int(spacing * 5.6),
    165000: base_ms + int(spacing * 6.9),
    195000: base_ms + int(spacing * 8.2),
    225000: base_ms + int(spacing * 9.5),
    255000: base_ms + int(spacing * 10.8),
    265000: base_ms + int(spacing * 11.2),
}

for placeholder, ms_value in offset_map.items():
    template_text = template_text.replace(
        f'"__WINDOW_T_PLUS_{placeholder}__"', str(ms_value)
    )

template_text = template_text.replace('"__WATERMARK_ADVANCE__"', str(watermark_ms))

# ─── Write output ─────────────────────────────────────────────────────────────

transactions = json.loads(template_text)

output_file = Path(__file__).parent / "sample-transactions.json"
output_file.write_text(json.dumps(transactions, indent=2))

print(f"✅ Generated {len(transactions)} transactions → {output_file}")
print()
for acct in ["ACC-001-SUSPECT", "ACC-002-NORMAL", "ACC-003-BORDERLINE"]:
    count = sum(1 for t in transactions if t["account_id"] == acct and "WATERMARK" not in t["transaction_id"])
    flag = "🚨 WILL BE FLAGGED (>10)" if count > 10 else ("⚠️  AT THRESHOLD (=10)" if count == 10 else "✅ Safe")
    print(f"  {acct}: {count} transactions  {flag}")
print()
print("Next: python produce_messages.py")
