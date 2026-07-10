-- Workflows ranked by failure rate within a recent window — "where is CI
-- actually breaking". Placeholders: {ci_glob}, {since_hours}, {limit}.
-- HAVING >= 2 runs keeps one-off flakes from topping the list.
SELECT
    workflow_name,
    count(*) FILTER (conclusion = 'failure')                    AS failures,
    count(*)                                                    AS runs,
    avg(CASE WHEN conclusion = 'failure' THEN 1.0 ELSE 0.0 END) AS failure_rate
FROM read_parquet('{ci_glob}')
WHERE conclusion IN ('success', 'failure')
  AND created_at >= now() - INTERVAL '{since_hours} hours'
GROUP BY workflow_name
HAVING count(*) >= 2
ORDER BY failure_rate DESC, failures DESC
LIMIT {limit}
