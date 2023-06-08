-- This query will look at every query ran over the specified timeframe and determine if it is better to be run
-- under an on-demand or an Editions pricing billing model. In the final resultset, it will recommend on-demand 
-- or an Edition (or an Edition with a commit period)

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
  costs AS (
    SELECT
      *,
      ROUND(SAFE_DIVIDE(totalBytesBilled,
        POW(1024, 4)) * 5, 2) AS legacyOnDemandCost,
      ROUND(SAFE_DIVIDE(totalBytesBilled,
        POW(1024, 4)) * 6.25, 2) AS onDemandCost,
      (approximateSlotCount/(60*60)) * 0.04 AS standardEditionCost,
      (approximateSlotCount/(60*60)) * 0.06 AS enterpriseEditionCost,
      (approximateSlotCount/(60*60)) * 0.048 AS enterpriseEdition1YearCost,
      (approximateSlotCount/(60*60)) * 0.036 AS enterpriseEdition3YearCost,
      (approximateSlotCount/(60*60)) * 0.1 AS enterprisePlusEditionCost,
      (approximateSlotCount/(60*60)) * 0.08 AS enterprisePlusEdition1YearCost,
      (approximateSlotCount/(60*60)) * 0.06 AS enterprisePlusEdition3YearCost,
    FROM
      src
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
    totalBytesBilled,
    totalMegabytesBilled,
    totalGigabytesBilled,
    totalTerabytesBilled,
    approximateSlotCount,

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
  IF(standardEditionComparison > 0, 'On-demand', 'Standard Edition') AS standardEditionRecommendation,
  IF(enterpriseEditionComparison > 0, 'On-demand', 'Enterprise Edition') AS enterpriseEditionRecommendation,
  IF(enterpriseEdition1YearComparison > 0, 'On-demand', 'Enterprise Edition 1 Year Commit') AS 
  enterpriseEdition1YearRecommendation,
  IF(enterpriseEdition3YearComparison > 0, 'On-demand', 'Enterprise Edition 3 Year Commit') AS enterpriseEdition3YearRecommendation,

  IF(enterpriseEditionComparison > 0, 'On-demand', 'Enterprise Plus Edition') AS enterprisePlusEditionRecommendation,
  IF(enterpriseEdition1YearComparison > 0, 'On-demand', 'Enterprise Edition 1 Year Commit') AS 
  enterpriseEditionPlus1YearRecommendation,
  IF(enterprisePlusEdition3YearComparison > 0, 'On-demand', 'Enterprise Plus Edition 3 Year Commit') AS enterprisePlusEdition3YearRecommendation,

  startTime,
  endTime,
  approximateSlotCount,

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