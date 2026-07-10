-- PRs merged within a recent window, newest first — the "what changed just
-- before things broke" suspect list for evidence-grounded diagnosis.
-- Placeholders: {data_glob}, {repo_filter}, {since_hours}, {limit}.
SELECT
    json_extract_string(payload_json, '$.number')             AS pr_number,
    json_extract_string(payload_json, '$.pull_request.title') AS title,
    actor_login,
    created_at                                                AS merged_at
FROM read_parquet('{data_glob}')
WHERE event_type = 'PullRequestEvent'
  {repo_filter}
  AND json_extract_string(payload_json, '$.action') = 'closed'
  AND json_extract_string(payload_json, '$.pull_request.merged') = 'true'
  AND created_at >= now() - INTERVAL '{since_hours} hours'
ORDER BY created_at DESC
LIMIT {limit}
