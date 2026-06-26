# Testing Approach — Banking Fraud Detection

Run all queries in the **Confluent Cloud Flink SQL console**:
1. Select your environment → **Flink** → **SQL workspace**
2. Set the catalog to `banking-fraud-env` and database to `fraud-cluster`
3. Paste each query and run it

> **Timing note:** Allow ~30 seconds after the last event for the 5-minute
> tumbling window to close before running queries 2–4.

---

## Query 1 — Raw Stream (source table)

**Purpose:** Verify transactions are landing in `banking_transactions`.

```sql
SELECT *
FROM banking_transactions
LIMIT 10;
```

**Expected output (sample rows):**

| account_id | transaction_id | amount | transaction_type | merchant_id | channel | transaction_time |
|------------|---------------|--------|-----------------|-------------|---------|-----------------|
| ACC-001-SUSPECT | TXN-001-001 | 49.99 | DEBIT | MERCH-AMAZON | ONLINE | 2025-01-01 14:00:10.000 |
| ACC-002-NORMAL | TXN-002-001 | 3200.00 | CREDIT | MERCH-PAYROLL | BRANCH | 2025-01-01 14:00:10.xxx |
| … | … | … | … | … | … | … |

**Pass criteria:**
- ✅ Rows returned (not empty)
- ✅ `transaction_time` is a TIMESTAMP (not a number)
- ✅ `amount` displays as a decimal (not an integer)
- ✅ All three account IDs appear in the results

---

## Query 2 — Aggregated Fraud Alerts (destination table)

**Purpose:** Verify the Flink job is writing results to `fraud_alerts`.

```sql
SELECT
  account_id,
  window_start,
  window_end,
  transaction_count,
  total_amount,
  is_flagged
FROM fraud_alerts
ORDER BY account_id;
```

**Expected output:**

| account_id | window_start | window_end | transaction_count | total_amount | is_flagged |
|------------|-------------|------------|-------------------|--------------|------------|
| ACC-001-SUSPECT | 2025-01-01 14:00:00 | 2025-01-01 14:05:00 | 12 | 1480.33 | **true** |
| ACC-002-NORMAL | 2025-01-01 14:00:00 | 2025-01-01 14:05:00 | 3 | 4485.40 | **false** |
| ACC-003-BORDERLINE | 2025-01-01 14:00:00 | 2025-01-01 14:05:00 | 10 | 598.94 | **false** |

**Pass criteria:**
- ✅ Exactly 3 rows (one per account per window)
- ✅ `ACC-001-SUSPECT` has `is_flagged = true`
- ✅ `ACC-002-NORMAL` has `is_flagged = false`
- ✅ `ACC-003-BORDERLINE` has `is_flagged = false` (10 is NOT > 10)
- ✅ All rows share the same `window_start` / `window_end` boundaries

---

## Query 3 — Windowed Aggregation Verification (TVF syntax)

**Purpose:** Confirm the TUMBLE window alignment and time boundaries directly
against the source table.

```sql
SELECT
  account_id,
  window_start,
  window_end,
  COUNT(*)    AS transaction_count,
  SUM(amount) AS total_amount
FROM TABLE(
  TUMBLE(
    TABLE banking_transactions,
    DESCRIPTOR(transaction_time),
    INTERVAL '5' MINUTES
  )
)
GROUP BY account_id, window_start, window_end
ORDER BY transaction_count DESC;
```

**Expected output:**

| account_id | window_start | window_end | transaction_count | total_amount |
|------------|-------------|------------|-------------------|--------------|
| ACC-001-SUSPECT | HH:MM:00 | HH:(MM+5):00 | 12 | 1480.33 |
| ACC-003-BORDERLINE | HH:MM:00 | HH:(MM+5):00 | 10 | 598.94 |
| ACC-002-NORMAL | HH:MM:00 | HH:(MM+5):00 | 3 | 4485.40 |

**Pass criteria:**
- ✅ `window_start` aligns to a 5-minute clock boundary (seconds = 00)
- ✅ `window_end = window_start + 5 minutes`
- ✅ All events for a single account fall within **one** window (no split rows)
- ✅ `ACC-001-SUSPECT` count = 12 (highest)

---

## Query 4 — Fraud Flag Filter (business logic validation)

**Purpose:** Extract only flagged accounts — simulates what a downstream alert
consumer would query.

```sql
SELECT
  account_id,
  window_start,
  window_end,
  transaction_count,
  total_amount
FROM fraud_alerts
WHERE is_flagged = true
ORDER BY transaction_count DESC;
```

**Expected output:**

| account_id | window_start | window_end | transaction_count | total_amount |
|------------|-------------|------------|-------------------|--------------|
| ACC-001-SUSPECT | 2025-01-01 14:00:00 | 2025-01-01 14:05:00 | 12 | 1480.33 |

**Pass criteria:**
- ✅ Exactly **1 row** returned
- ✅ `account_id = ACC-001-SUSPECT`
- ✅ `transaction_count = 12` (> 10 threshold)
- ✅ `ACC-002-NORMAL` and `ACC-003-BORDERLINE` do **not** appear

---

## Troubleshooting Test Failures

| Failure | Likely Cause | Fix |
|---------|-------------|-----|
| Query 1 returns 0 rows | Producer hasn't run yet | Run `python produce_messages.py` |
| Query 2 returns 0 rows | Window hasn't closed | Wait 30 s after last event |
| Events split across 2 windows | Timestamps span a boundary | Re-run `generate_sample_data.py` and produce again |
| `is_flagged` is `false` for SUSPECT | Count < 10 (split window) | As above |
| `ACC-003-BORDERLINE` flagged | Count > 10 (duplicate produces) | Check for double-produce; flush Kafka topic |
| Schema Registry error | Flink statements not yet Running | Verify all 3 Flink statements are in Running state |

---

## Flink SQL Console Tips

- Use **LIMIT** on streaming queries to avoid continuous result scans
- The `fraud_detection_job` INSERT statement runs continuously — do not stop it
- After `terraform destroy`, all topics and schemas are deleted automatically
