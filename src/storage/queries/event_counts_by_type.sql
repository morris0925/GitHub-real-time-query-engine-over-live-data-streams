-- event_counts_by_type.sql
-- Count how many events of each type arrived in the last N minutes.
--
-- Parameters (substituted by Python before executing):
--   {data_glob}    : glob path to Parquet files, e.g. data/events/**/*.parquet
--   {since_minutes}: integer, how far back to look (e.g. 60 for last hour)

SELECT
    event_type,
    COUNT(*) AS event_count
FROM read_parquet('{data_glob}')
WHERE created_at >= NOW() - INTERVAL '{since_minutes}' MINUTE
GROUP BY event_type
ORDER BY event_count DESC
