-- KPI: total number of distinct timeseries
-- Replace {source_table} with the actual source table identifier.
SELECT COUNT(DISTINCT meetreeks_id) AS total_timeseries
FROM {source_table};
