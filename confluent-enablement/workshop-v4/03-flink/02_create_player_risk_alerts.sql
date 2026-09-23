-- ─────────────────────────────────────────────────────────────────────────────
-- Statement 2: Create player_risk_alerts sink table
-- ─────────────────────────────────────────────────────────────────────────────
-- Run AFTER Statement 1 is COMPLETED.
--
-- KEY DECISIONS:
--   NO PRIMARY KEY → append-only mode
--     ✅ Every 1-minute window emits a new row per player
--     ❌ NOT: PRIMARY KEY (player_id) NOT ENFORCED
--        Adding a PRIMARY KEY activates the upsert connector,
--        which returns MT_UPSERT_NOT_SUPPORTED in Confluent Cloud Flink
--
--   window_start/end as TIMESTAMP(3)
--     ✅ Matches the CAST(window_start AS TIMESTAMP(3)) in Statement 3 INSERT
--
--   DISTRIBUTED BY HASH(player_id) → value-only partitioning (no key.format)
--
-- Expected status after run: COMPLETED
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS player_risk_alerts;

CREATE TABLE IF NOT EXISTS player_risk_alerts (
  player_id      STRING,
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  bet_count      BIGINT,
  total_wagered  DECIMAL(18, 2),
  avg_bet        DECIMAL(18, 2),
  is_flagged     BOOLEAN
) DISTRIBUTED BY HASH(player_id) INTO 4 BUCKETS
WITH (
  'value.format' = 'json-registry'
);
