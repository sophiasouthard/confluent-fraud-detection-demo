-- ─────────────────────────────────────────────────────────────────────────────
-- Statement 1: Create player_events source table
-- ─────────────────────────────────────────────────────────────────────────────
-- Run in: Confluent Cloud → your cluster → Stream Processing → Statements → New statement
--
-- KEY DECISIONS:
--   DISTRIBUTED BY HASH(player_id)
--     ✅ Value-only partitioning — player_id stays a queryable column in SELECT
--     ❌ NOT: DISTRIBUTED BY (player_id) — that creates a Kafka key column,
--        requires key.format, and makes player_id invisible in TUMBLE TVF output
--
--   PRIMARY KEY (player_id) NOT ENFORCED
--     ✅ Required so Flink's TUMBLE TVF validator resolves player_id in GROUP BY
--     Without this, Confluent reports: "Column 'player_id' not found in any table"
--
--   'value.format' = 'json-registry' ONLY — no key.format declaration
--
--   $rowtime for windowing (see Statement 3)
--     ✅ Confluent's built-in row-time attribute — always the right type
--     ❌ NOT: PROCTIME() — not supported in Confluent Cloud's Flink SQL dialect
--     ❌ NOT: event_time with WATERMARK — event_time is a STRING column here,
--        converting to TIMESTAMP_LTZ causes type mismatch on CAST
--
-- Expected status after run: COMPLETED
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS player_events;

CREATE TABLE IF NOT EXISTS player_events (
  player_id   STRING,
  session_id  STRING,
  bet_id      STRING,
  amount      DECIMAL(18, 2),
  game_type   STRING,
  channel     STRING,
  device_id   STRING,
  event_time  STRING,
  PRIMARY KEY (player_id) NOT ENFORCED
) DISTRIBUTED BY HASH(player_id) INTO 4 BUCKETS
WITH (
  'value.format' = 'json-registry'
);
