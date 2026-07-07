-- Average PR merge duration (hours) from PullRequestEvent payloads in a
-- time window [now - since_hours, now - until_hours).
-- Placeholders: {data_glob}, {since_hours}, {until_hours}.
-- merged_at/created_at live inside the raw payload JSON blob.
SELECT
    avg(epoch(
        CAST(json_extract_string(payload_json, '$.pull_request.merged_at')  AS TIMESTAMPTZ)
      - CAST(json_extract_string(payload_json, '$.pull_request.created_at') AS TIMESTAMPTZ)
    ) / 3600.0)  AS avg_merge_hours,
    count(*)     AS merged_count
FROM read_parquet('{data_glob}')
WHERE event_type = 'PullRequestEvent'
  AND json_extract_string(payload_json, '$.action') = 'closed'
  AND json_extract_string(payload_json, '$.pull_request.merged') = 'true'
  AND created_at >= now() - INTERVAL '{since_hours} hours'
  AND created_at <  now() - INTERVAL '{until_hours} hours'
