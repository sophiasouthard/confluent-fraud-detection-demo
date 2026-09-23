-- ─────────────────────────────────────────────────────────────────────────────
-- Statement 3: Player risk detection — 1-minute tumbling window aggregation
-- ─────────────────────────────────────────────────────────────────────────────
-- Run AFTER Statement 2 is COMPLETED. This job runs forever (status: RUNNING).
-- First alerts appear ~1 minute after the producer starts sending events.
--
-- WHAT THIS DOES:
--   Every 60 seconds, Flink seals the current window and computes per-player:
--     • bet_count   — how many bets this player placed in this minute
--     • total_wagered — total dollars wagered
--     • avg_bet      — average bet size
--     • is_flagged   — true if bet_count > 20 OR total_wagered > $10,000
--   One row is written to player_risk_alerts per player per window.
--
-- KEY DECISIONS:
--   DESCRIPTOR($rowtime)
--     ✅ Confluent's built-in row-time attribute — always the correct type
--     ❌ NOT: DESCRIPTOR(proc_time) — PROCTIME() not supported in Confluent Cloud
--     ❌ NOT: DESCRIPTOR(event_time) — event_time is a STRING column here
--
--   GROUP BY window_start, window_end, player_id
--     ✅ Window boundary columns MUST come first — Confluent Flink requirement
--     ❌ NOT: GROUP BY player_id, window_start, window_end — will fail
--
--   CAST(window_start AS TIMESTAMP(3))
--     ✅ Aligns with the TIMESTAMP(3) column type in player_risk_alerts sink table
--
-- Expected status: RUNNING — this job never stops.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO player_risk_alerts
SELECT
  player_id,
  CAST(window_start AS TIMESTAMP(3))      AS window_start,
  CAST(window_end   AS TIMESTAMP(3))      AS window_end,
  COUNT(*)                                AS bet_count,
  SUM(amount)                             AS total_wagered,
  AVG(amount)                             AS avg_bet,
  (COUNT(*) > 20 OR SUM(amount) > 10000)  AS is_flagged
FROM TUMBLE(
  TABLE player_events,
  DESCRIPTOR($rowtime),
  INTERVAL '1' MINUTE
)
GROUP BY window_start, window_end, player_id;
