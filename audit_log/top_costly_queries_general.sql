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
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE '%_job_completed'
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
    src1.query AS query,
    src1.hashed AS hashed,
    SUM(src1.totalBilledBytes) AS totalBytesBilled,
    COUNT(src1.hashed) AS queryCount
  FROM
    jobsDeduplicated AS src1,
    jobsDeduplicated AS src2
  WHERE
    src1.hashed = src2.hashed
    AND src1.jobId <> src2.jobId
  GROUP BY
    hashed,
    query)

SELECT
  query,
  queryCount,
  ROUND(COALESCE(totalBytesBilled, 0), 2) AS totalBytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / 1000000, 2) AS totalMegabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / 1000000000, 2) AS totalGigabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / 1000000000000, 2) AS totalTerabytesBilled,
  ROUND(SAFE_DIVIDE(totalBytesBilled, 1000000000000) * 5, 2) AS onDemandCost
FROM
  hashedQueries
ORDER BY
  onDemandCost DESC;