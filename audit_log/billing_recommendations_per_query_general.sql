-- This query returns general information about jobs run over the interval

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

-- This query lists out every job over a period of time along with their execution details (including slot cout)
WITH src AS (
    SELECT
        SAFE_DIVIDE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
          TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
            protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
            MILLISECOND)) AS approximateSlotCount,
        ROUND(SAFE_DIVIDE(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes, 0),
            102400000000) * 5, 2) AS onDemandCost,
        CASE protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName
            WHEN 'query_job_completed' THEN 'QUERY'
            WHEN 'load_job_completed' THEN 'LOAD'
            WHEN 'extract_job_completed' THEN 'EXTRACT'
            WHEN 'table_copy_job_completed' THEN 'TABLE COPY'
        END AS eventType,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS query,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS projectId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0), 2) AS totalBytesBilled,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0) / 1000000, 2) AS totalMegabytesBilled,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0) / 1000000000, 2) AS totalGigabytesBilled,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0) / 1000000000000, 2) AS totalTerabytesBilled,
        TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
                        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
                        MILLISECOND) AS executionTimeMs,
    FROM `<project>.<dataset>cloudaudit_googleapis_com_data_access`
    WHERE
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE 'query_job_completed'
        AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
        AND protopayload_auditlog.authenticationInfo.principalEmail != ""
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun IS NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
        -- Change to look at your proper dates
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
),
queryCounts AS (
  SELECT
    query,
    COUNT(query) AS queryCount
  FROM
    src
  GROUP BY
    query),
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