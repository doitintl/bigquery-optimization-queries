-- This query aggregates costs across all queries in the specified timeframe and compares
-- on-demand pricing versus different BigQuery Editions pricing models

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 30;

BEGIN
WITH
  src AS (
  SELECT
    ROUND(SAFE_DIVIDE(total_slot_ms,
        TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS approximateSlotCount,
    job_type,
    query,
    project_id AS projectId,
    start_time AS startTime,
    end_time AS endTime,
    ROUND(COALESCE(total_bytes_billed,
        0), 2) AS totalBytesBilled,
    ROUND(COALESCE(total_bytes_billed,
        0) / POW(1024, 2), 2) AS totalMegabytesBilled,
    ROUND(COALESCE(total_bytes_billed,
        0) / POW(1024, 3), 2) AS totalGigabytesBilled,
    ROUND(COALESCE(total_bytes_billed,
        0) / POW(1024, 4), 2) AS totalTerabytesBilled,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS executionTimeMs
  FROM
     `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
    AND CURRENT_TIMESTAMP()
  ),
  rounded AS (
    SELECT
      *,
      -- Rounds up to the nearest 50 slots (autoscaler increments)
      floor((CEIL(approximateSlotCount) + 49) / 50) * 50 AS roundedUpSlots,
      
      -- If query ran in under 1 minute (60 seconds * 1000 ms) then round up to 1 minute
      IF(executionTimeMs < 1000*60, 1000*60, executionTimeMs) AS billedDurationMs,
      -- Calculates the duration in hours for calculating slot/hours used
      -- Formula: (Execution Time in ms)/(1000 ms * 60 seconds * 60 minutes)
      IF(executionTimeMs < 1000*60, 1000*60, executionTimeMs)/(1000*60*60) AS billedDurationHour,
      
      -- Apply minimum 10 MiB billing per query
      GREATEST(totalBytesBilled, 10 * POW(1024, 2)) AS billedBytes,
      GREATEST(totalMegabytesBilled, 10) AS billedMegabytes,
      GREATEST(totalGigabytesBilled, 10/1024) AS billedGigabytes,
      GREATEST(totalTerabytesBilled, 10/POW(1024, 2)) AS billedTerabytes
    FROM src
  ),
  costs AS (
    SELECT
      *,
      SAFE_DIVIDE(billedBytes,
        POW(1024, 4)) * 6.25 AS onDemandCost,
      -- Multiply by roundedUpSlots to correctly calculate slot-hours cost
      roundedUpSlots * billedDurationHour * 0.04 AS standardEditionCost,
      roundedUpSlots * billedDurationHour * 0.06 AS enterpriseEditionCost,
      roundedUpSlots * billedDurationHour * 0.048 AS enterpriseEdition1YearCost,
      roundedUpSlots * billedDurationHour * 0.036 AS enterpriseEdition3YearCost,
      roundedUpSlots * billedDurationHour * 0.1 AS enterprisePlusEditionCost,
      roundedUpSlots * billedDurationHour * 0.08 AS enterprisePlusEdition1YearCost,
      roundedUpSlots * billedDurationHour * 0.06 AS enterprisePlusEdition3YearCost
    FROM
      rounded
  )

-- Aggregate costs across all queries
SELECT
  -- Time period information
  CONCAT('Last ', interval_in_days, ' days (', 
         FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)),
         ' to ',
         FORMAT_TIMESTAMP('%Y-%m-%d', CURRENT_TIMESTAMP()),
         ')') AS time_period,
  
  -- Query count
  COUNT(DISTINCT query) AS total_unique_queries,
  COUNT(*) AS total_query_executions,
  
  -- Total bytes processed
  SUM(billedBytes) AS total_bytes_billed,
  ROUND(SUM(billedBytes) / POW(1024, 4), 2) AS total_terabytes_billed,
  
  -- Total slot hours
  SUM(billedDurationHour) AS total_billed_slot_hours,
  
  -- Aggregated costs
  ROUND(SUM(onDemandCost), 2) AS total_on_demand_cost,
  ROUND(SUM(standardEditionCost), 2) AS total_standard_edition_cost,
  ROUND(SUM(enterpriseEditionCost), 2) AS total_enterprise_edition_cost,
  ROUND(SUM(enterpriseEdition1YearCost), 2) AS total_enterprise_1year_cost,
  ROUND(SUM(enterpriseEdition3YearCost), 2) AS total_enterprise_3year_cost,
  ROUND(SUM(enterprisePlusEditionCost), 2) AS total_enterprise_plus_cost,
  ROUND(SUM(enterprisePlusEdition1YearCost), 2) AS total_enterprise_plus_1year_cost,
  ROUND(SUM(enterprisePlusEdition3YearCost), 2) AS total_enterprise_plus_3year_cost,
  
  -- Cost comparisons (positive means on-demand is more expensive)
  ROUND(SUM(onDemandCost) - SUM(standardEditionCost), 2) AS on_demand_vs_standard_diff,
  ROUND(SUM(onDemandCost) - SUM(enterpriseEditionCost), 2) AS on_demand_vs_enterprise_diff,
  ROUND(SUM(onDemandCost) - SUM(enterpriseEdition1YearCost), 2) AS on_demand_vs_enterprise_1year_diff,
  ROUND(SUM(onDemandCost) - SUM(enterpriseEdition3YearCost), 2) AS on_demand_vs_enterprise_3year_diff,
  
  -- Cost savings percentages
  ROUND(100 * (SUM(onDemandCost) - SUM(standardEditionCost)) / NULLIF(SUM(onDemandCost), 0), 2) AS standard_edition_savings_pct,
  ROUND(100 * (SUM(onDemandCost) - SUM(enterpriseEditionCost)) / NULLIF(SUM(onDemandCost), 0), 2) AS enterprise_edition_savings_pct,
  ROUND(100 * (SUM(onDemandCost) - SUM(enterpriseEdition1YearCost)) / NULLIF(SUM(onDemandCost), 0), 2) AS enterprise_1year_savings_pct,
  ROUND(100 * (SUM(onDemandCost) - SUM(enterpriseEdition3YearCost)) / NULLIF(SUM(onDemandCost), 0), 2) AS enterprise_3year_savings_pct,
  
  -- Overall recommendation
  CASE
    WHEN SUM(onDemandCost) < SUM(standardEditionCost) THEN 'On-demand pricing recommended'
    WHEN SUM(enterpriseEdition3YearCost) < SUM(standardEditionCost) AND 
         SUM(enterpriseEdition3YearCost) < SUM(enterpriseEdition1YearCost) AND
         SUM(enterpriseEdition3YearCost) < SUM(enterpriseEditionCost) THEN 'Enterprise Edition with 3-year commitment recommended'
    WHEN SUM(enterpriseEdition1YearCost) < SUM(standardEditionCost) AND
         SUM(enterpriseEdition1YearCost) < SUM(enterpriseEditionCost) THEN 'Enterprise Edition with 1-year commitment recommended'
    WHEN SUM(enterpriseEditionCost) < SUM(standardEditionCost) THEN 'Enterprise Edition recommended'
    ELSE 'Standard Edition recommended'
  END AS overall_recommendation
FROM
  costs;
END
