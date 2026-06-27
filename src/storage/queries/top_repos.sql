-- top_repos.sql
-- Find the most active repositories in the last N minutes.
--
-- Parameters (substituted by Python before executing):
--   {data_glob}    : glob path to Parquet files, e.g. data/events/**/*.parquet
--   {since_minutes}: integer, how far back to look
--   {limit}        : integer, how many repos to return (e.g. 10)

SELECT
    repo_name,
    COUNT(*)                    AS event_count,
    COUNT(DISTINCT actor_login) AS unique_actors,
    MAX(created_at)             AS last_seen_at
FROM read_parquet('{data_glob}')
WHERE created_at >= NOW() - INTERVAL '{since_minutes}' MINUTE
GROUP BY repo_name
ORDER BY event_count DESC
LIMIT {limit}
