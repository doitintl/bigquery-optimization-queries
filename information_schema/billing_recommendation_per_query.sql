-- This query will look at every query ran over the specified timeframe and determine if it is better to be run
-- under an on-demand or flat-rate pricing billing model.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
WITH
  src AS (
  SELECT
    ROUND(SAFE_DIVIDE(total_slot_ms,
        TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS approximateSlotCount,
    ROUND(SAFE_DIVIDE(total_bytes_billed,
        POW(1024, 4)) * 5, 2) AS onDemandCost,
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
    approximateSlotCount DESC ),
  queryCounts AS (
  SELECT
    query,
    COUNT(query) AS queryCount
  FROM
    src
  GROUP BY
    query ),
  recommendations AS (
  SELECT
    src.query,
    projectId,
    startTime,
    endTime,
    totalBytesBilled,
    totalMegabytesBilled,
    totalGigabytesBilled,
    totalTerabytesBilled,
    approximateSlotCount,
    onDemandCost,
    -- On-Demand recommendation score
    (onDemandCost * 100) + IF(job_type = 'QUERY',100,200) + IF(executionTimeMs > 60000, 100, 200) +
        IF(approximateSlotCount > 500,500,100) + (queryCount * onDemandCost * -0.1) +
        IF(job_type = 'LOAD', 100, 0) + IF(job_type = 'COPY', 100, 0) + IF(job_type = 'EXPORT', 100, 0) AS onDemandScore,
    -- Flat rate recommendation score
    (onDemandCost * 125) + IF(job_type = 'QUERY', 200, 100) + IF(executionTimeMs > 60000, 200, 100) +
        IF(approximateSlotCount > 200, 100, 500) + (queryCount * 0.1 * onDemandCost) AS flatRateScore
  FROM
    src
  JOIN
    queryCounts
  ON
    src.query = queryCounts.query )

SELECT
  query,
  IF (onDemandScore > flatRateScore, 'On Demand', 'Flat Rate') AS recommendedBilling,
  ROUND(IF(onDemandScore > flatRateScore,
      onDemandScore/flatRateScore, flatRateScore/onDemandScore), 2) AS recommendedBillingRatio,
  projectId,
  startTime,
  endTime,
  approximateSlotCount,
  onDemandCost,
  totalBytesBilled,
  totalMegabytesBilled,
  totalGigabytesBilled,
  totalTerabytesBilled
FROM
  recommendations
ORDER BY
  onDemandCost DESC,
  approximateSlotCount DESC;
END