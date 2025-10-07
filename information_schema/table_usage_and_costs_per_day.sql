-- This query shows costs and billed-usage per-day per table in your project.
-- It may not be up-to-date as the data is typically 2-3 days behind inside of information schema

-- Change this value to change how far in the past the query will search
-- Note currently (as of 10/7/2025) this view only supports up to 90-days in the past
DECLARE interval_in_days INT64 DEFAULT 7;

-- Change these for different regions, this value is for the US multi-region
-- Note these values are in 1 gigibyte (gib) hours
-- These values can be found here: https://cloud.google.com/bigquery/pricing?hl=en#storage-pricing
DECLARE active_logical_storage_per_hour NUMERIC DEFAULT 0.000027397;
DECLARE long_term_logical_storage_per_hour NUMERIC DEFAULT 0.000013699;

DECLARE active_physical_storage_per_hour NUMERIC DEFAULT 0.000054795;
DECLARE long_term_physical_storage_per_hour NUMERIC DEFAULT 0.000027397;

SELECT
  usage_date,
  project_id,
  table_schema,
  table_name,
  SUM(billable_total_logical_usage)/POW(1024, 3) AS billable_total_logical_usage_in_gibytes,
  SUM(billable_total_logical_usage)/POW(1024, 4) AS billable_total_logical_usage_in_tibytes,

  SUM(billable_active_logical_usage)/POW(1024, 3) AS billable_active_logical_usage_in_gibytes,
  SUM(billable_active_logical_usage)/POW(1024, 4) AS billable_active_logical_usage_in_tibytes,
  SUM(billable_active_logical_usage)/POW(1024, 3) * active_logical_storage_per_hour AS active_logical_cost,
  CONCAT('$ ',FORMAT("%'.2f", SUM(billable_active_logical_usage)/POW(1024, 3) * active_logical_storage_per_hour)) AS active_logical_pretty_cost,

  SUM(billable_long_term_logical_usage)/POW(1024, 3) AS billable_long_term_logical_usage_in_gibytes,
  SUM(billable_long_term_logical_usage)/POW(1024, 4) AS billable_long_term_logical_usage_in_tibytes,
  SUM(billable_long_term_logical_usage)/POW(1024, 3) * long_term_logical_storage_per_hour AS long_term_logical_cost,
  CONCAT('$ ',FORMAT("%'.2f", SUM(billable_long_term_logical_usage)/POW(1024, 3) * long_term_logical_storage_per_hour)) AS long_term_logical_pretty_cost,

  SUM(billable_total_physical_usage)/POW(1024, 3) AS billable_total_physical_usage_in_gibytes,
  SUM(billable_total_physical_usage)/POW(1024, 4) AS billable_total_physical_usage_in_tibytes,

  SUM(billable_active_physical_usage)/POW(1024, 3) AS billable_active_physical_usage_in_gibytes,
  SUM(billable_active_physical_usage)/POW(1024, 4) AS billable_active_physical_usage_in_tibytes,
  SUM(billable_active_physical_usage)/POW(1024, 3) * active_physical_storage_per_hour AS active_physical_cost,
  CONCAT('$ ',FORMAT("%'.2f", SUM(billable_active_physical_usage)/POW(1024, 3) * active_physical_storage_per_hour)) AS long_term_physical_pretty_cost,

  SUM(billable_long_term_physical_usage)/POW(1024, 3) AS billable_long_term_physical_usage_in_gibytes,
  SUM(billable_long_term_physical_usage)/POW(1024, 4) AS billable_long_term_physical_usage_in_tibytes,
  SUM(billable_long_term_physical_usage)/POW(1024, 3) * long_term_physical_storage_per_hour AS long_term_logical_cost,
  CONCAT('$ ',FORMAT("%'.2f", SUM(billable_long_term_physical_usage)/POW(1024, 3) * long_term_physical_storage_per_hour)) AS long_term_physical_pretty_cost
FROM
  `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLE_STORAGE_USAGE_TIMELINE
WHERE
  usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
GROUP BY
  1,
  2,
  3,
  4
ORDER BY
  usage_date;