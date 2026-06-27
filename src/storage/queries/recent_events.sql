-- recent_events.sql
-- Fetch the N most recent events for the live event feed panel.
--
-- Parameters (substituted by Python before executing):
--   {data_glob}: glob path to Parquet files, e.g. data/events/**/*.parquet
--   {limit}    : integer, how many rows to return (e.g. 20)

SELECT
    event_type,
    actor_login,
    repo_name,
    created_at
FROM read_parquet('{data_glob}')
ORDER BY created_at DESC
LIMIT {limit}
