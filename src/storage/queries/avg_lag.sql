-- avg_lag.sql
-- Measure average pipeline lag in the last N minutes.
--
-- "Lag" = how long after an event was created on GitHub did our consumer
-- actually write it to Parquet. Formula: ingested_at - created_at (seconds).
--
-- A healthy pipeline should have lag < 30s (our flush interval).
-- Rising lag could mean the consumer is falling behind, Kafka is backing up,
-- or GitHub's API is slow.
--
-- Parameters:
--   {data_glob}    : glob path to Parquet files
--   {since_minutes}: look-back window (e.g. 60)

SELECT
    ROUND(AVG(
        EPOCH(ingested_at) - EPOCH(created_at)
    ), 1)                          AS avg_lag_seconds,
    ROUND(MIN(
        EPOCH(ingested_at) - EPOCH(created_at)
    ), 1)                          AS min_lag_seconds,
    ROUND(MAX(
        EPOCH(ingested_at) - EPOCH(created_at)
    ), 1)                          AS max_lag_seconds,
    COUNT(*)                       AS sample_size
FROM read_parquet('{data_glob}')
WHERE ingested_at >= NOW() - INTERVAL '{since_minutes}' MINUTE
  AND ingested_at >= created_at    -- guard against clock skew producing negative lag
