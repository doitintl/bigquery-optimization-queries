-- This query will look at every query ran over the specified timeframe and determine if it is better to be run
-- under an on-demand or an Editions pricing billing model. In the final resultset, it will recommend on-demand 
-- or an Edition (or an Edition with a commit period) for each Edition type and commit period

/*
 *  This query does some real-world estimations of actual costs of Editions when using the autoscaler.
 *  It gets to this by utilizing a few nuances of the autoscaler and its behavior,
 *  namely that each query is billed for a minimum of 1 minute and
 *  slots are billed in increments of 100 (rounds up to nearest 100).
*/

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

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
    approximateSlotCount DESC ),
  rounded AS (
    SELECT
      *,
      -- Rounds up to the nearest 100 slots
      floor((CEIL(approximateSlotCount) + 99) / 100) * 100 AS roundedUpSlots,
      
      -- If query ran in under 1 minute (60 seconds * 1000 ms) then round up to 1 minute
      IF(executionTimeMs < 1000*60, 1000*60, executionTimeMs) AS billedDurationMs,
      -- Calculates the duration in hours for calculating slot/hours used
      -- Formula: (Execution Time in ms)/(1000 ms * 60 seconds * 60 minutes)
      IF(executionTimeMs < 1000*60, 1000*60, executionTimeMs)/(1000*60*60) AS billedDurationHour
    FROM src
  ),
  costs AS (
    SELECT
      *,
      ROUND(SAFE_DIVIDE(totalBytesBilled,
        POW(1024, 4)) * 5, 2) AS legacyOnDemandCost,
      ROUND(SAFE_DIVIDE(totalBytesBilled,
        POW(1024, 4)) * 6.25, 2) AS onDemandCost,

      billedDurationHour * 0.04 AS standardEditionCost,
      billedDurationHour * 0.06 AS enterpriseEditionCost,
      billedDurationHour * 0.048 AS enterpriseEdition1YearCost,
      billedDurationHour * 0.036 AS enterpriseEdition3YearCost,
      billedDurationHour * 0.1 AS enterprisePlusEditionCost,
      billedDurationHour * 0.08 AS enterprisePlusEdition1YearCost,
      billedDurationHour * 0.06 AS enterprisePlusEdition3YearCost
    FROM
      rounded
  ),
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
    costs.query,
    projectId,
    startTime,
    endTime,
    billedDurationHour AS editionsBilledDurationInHours,

    totalBytesBilled,
    totalMegabytesBilled,
    totalGigabytesBilled,
    totalTerabytesBilled,
    approximateSlotCount,
    roundedUpSlots AS billedSlotCount,

    legacyOnDemandCost,
    onDemandCost,

    standardEditionCost,
    enterpriseEditionCost,
    enterpriseEdition1YearCost,
    enterpriseEdition3YearCost,
    enterprisePlusEditionCost,
    enterprisePlusEdition1YearCost,
    enterprisePlusEdition3YearCost,

    onDemandCost - standardEditionCost AS standardEditionComparison,
    onDemandCost - enterpriseEditionCost AS enterpriseEditionComparison,
    onDemandCost - enterpriseEdition1YearCost AS enterpriseEdition1YearComparison,
    onDemandCost - enterpriseEdition3YearCost AS enterpriseEdition3YearComparison,
    onDemandCost - enterprisePlusEditionCost AS enterprisePlusEditionComparison,
    onDemandCost - enterprisePlusEdition1YearCost AS enterprisePlusEdition1YearComparison,
    onDemandCost - enterprisePlusEdition3YearCost AS enterprisePlusEdition3YearComparison
  FROM
    costs
  JOIN
    queryCounts
  ON
    costs.query = queryCounts.query )

SELECT
  query,
  IF(standardEditionComparison < 0, 'On-demand', 'Standard Edition') AS standardEditionRecommendation,
  IF(enterpriseEditionComparison < 0, 'On-demand', 'Enterprise Edition') AS enterpriseEditionRecommendation,
  IF(enterpriseEdition1YearComparison < 0, 'On-demand', 'Enterprise Edition 1 Year Commit') AS 
  enterpriseEdition1YearRecommendation,
  IF(enterpriseEdition3YearComparison < 0, 'On-demand', 'Enterprise Edition 3 Year Commit') AS enterpriseEdition3YearRecommendation,

  IF(enterpriseEditionComparison < 0, 'On-demand', 'Enterprise Plus Edition') AS enterprisePlusEditionRecommendation,
  IF(enterpriseEdition1YearComparison < 0, 'On-demand', 'Enterprise Edition 1 Year Commit') AS 
  enterpriseEditionPlus1YearRecommendation,
  IF(enterprisePlusEdition3YearComparison < 0, 'On-demand', 'Enterprise Plus Edition 3 Year Commit') AS enterprisePlusEdition3YearRecommendation,

  startTime,
  endTime,
  editionsBilledDurationInHours,
  approximateSlotCount,
  billedSlotCount,

  legacyOnDemandCost,
  onDemandCost,
  standardEditionCost,
  enterpriseEditionCost,
  enterpriseEdition1YearCost,
  enterpriseEdition3YearCost,
  enterprisePlusEditionCost,
  enterprisePlusEdition1YearCost,
  enterprisePlusEdition3YearCost,

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