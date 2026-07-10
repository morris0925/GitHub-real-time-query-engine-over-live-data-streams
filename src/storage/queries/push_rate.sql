-- PushEvent rate (events per hour) in a time window
-- [now - since_hours, now - until_hours).
-- Placeholders: {data_glob}, {since_hours}, {until_hours}, {window_hours}.
SELECT
    count(*) / {window_hours}.0 AS pushes_per_hour,
    count(*)                    AS push_count
FROM read_parquet('{data_glob}')
WHERE event_type = 'PushEvent'
  {repo_filter}
  AND created_at >= now() - INTERVAL '{since_hours} hours'
  AND created_at <  now() - INTERVAL '{until_hours} hours'
