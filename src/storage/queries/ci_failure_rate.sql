-- CI failure rate over completed workflow runs in a time window.
-- Window: [now - since_hours, now - until_hours). Placeholders: {ci_glob},
-- {since_hours}, {until_hours}. Only decisive conclusions count.
SELECT
    avg(CASE WHEN conclusion = 'failure' THEN 1.0 ELSE 0.0 END) AS failure_rate,
    count(*)                                                    AS run_count
FROM read_parquet('{ci_glob}')
WHERE conclusion IN ('success', 'failure')
  AND created_at >= now() - INTERVAL '{since_hours} hours'
  AND created_at <  now() - INTERVAL '{until_hours} hours'
