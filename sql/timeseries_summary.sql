-- Aggregates per meetreeks_id into summary metrics
-- Replace {source_table} with the actual source table identifier.
SELECT
  meetreeks_id,
  COUNT(*) AS cnt,
  MIN(timestamp) AS first_timestamp,
  MAX(timestamp) AS last_timestamp,
  DATEDIFF(MAX(timestamp), MIN(timestamp)) AS span_days
FROM {source_table}
GROUP BY meetreeks_id;
