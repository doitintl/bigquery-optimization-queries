-- This query analyzes BigQuery usage over a specified timeframe
-- focusing on slot usage and data processed without edition recommendations

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

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
    AND total_slot_ms IS NOT NULL
    AND state = "DONE"
  ORDER BY
    approximateSlotCount DESC 
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
  queryCounts AS (
  SELECT
    query,
    COUNT(query) AS queryCount
  FROM
    src
  GROUP BY
    query 
  ),
  queryMetrics AS (
  SELECT
    rounded.query,
    projectId,
    startTime,
    endTime,
    billedDurationHour,
    billedBytes,
    billedMegabytes,
    billedGigabytes,
    billedTerabytes,
    approximateSlotCount,
    roundedUpSlots,
    -- Calculate slot-hours for this query
    roundedUpSlots * billedDurationHour AS slotHours,
    -- Calculate on-demand cost
    SAFE_DIVIDE(billedBytes, POW(1024, 4)) * 6.25 AS onDemandCost,
    queryCount
  FROM
    rounded
  JOIN
    queryCounts
  ON
    rounded.query = queryCounts.query 
  )

-- Final output with query metrics
SELECT
  projectId,
  query,
  startTime,
  endTime,
  billedDurationHour AS queryDurationHours,
  approximateSlotCount,
  roundedUpSlots AS billedSlotCount,
  slotHours,
  onDemandCost,
  queryCount AS executionCount,
  billedBytes,
  billedMegabytes,
  billedGigabytes,
  billedTerabytes
FROM
  queryMetrics
ORDER BY
  onDemandCost DESC,
  slotHours DESC;
END
