-- This query will return queries that cost the most based upon their billed byte count and the number of runs over
-- the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 700;

WITH
  src AS (
  SELECT
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,
    SHA256(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query) AS hashed,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes, 0) AS totalBilledBytes,
    ROW_NUMBER() OVER(PARTITION BY protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId ORDER BY timestamp DESC) AS _rnk
  FROM
    `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
  WHERE
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration IS NOT NULL
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE 'query_job_completed'
    AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
    AND protopayload_auditlog.authenticationInfo.principalEmail != ""
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun IS NULL
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY) ),
  jobsDeduplicated AS (
  SELECT
    * EXCEPT(_rnk)
  FROM
    src
  WHERE
    _rnk = 1),
  hashedQueries AS (
    SELECT
      query,
      hashed,
      SUM(totalBilledBytes) AS totalBytesBilled,
      COUNT(*) AS queryCount
    FROM
      jobsDeduplicated
    GROUP BY
      hashed,
      query
  )

SELECT
  query,
  queryCount,
  ROUND(COALESCE(totalBytesBilled, 0), 2) AS totalBytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / POW(1024, 2), 2) AS totalMegabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / POW(1024, 3), 2) AS totalGigabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / POW(1024, 4), 2) AS totalTerabytesBilled,
  ROUND(SAFE_DIVIDE(totalBytesBilled, POW(1024, 4)) * 5, 2) AS onDemandCost
FROM
  hashedQueries
ORDER BY
  onDemandCost DESC;