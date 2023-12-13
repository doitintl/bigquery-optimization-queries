 -- This query will count the amount of times a specific query is run over the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 700;

WITH
  src AS (
  SELECT
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,
    SHA256(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query) AS hashed,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes, 0) AS totalBytesBilled,
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
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)),
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
      src1.hashed AS hashed
    FROM
      src AS src1,
      src AS src2
    WHERE
      src1.hashed = src2.hashed ),
  pricedQueries AS (
    SELECT
      hashed,
      SUM(totalBytesBilled) AS totalBytesBilled
    FROM
      jobsDeduplicated
    GROUP BY
      hashed),
  countQueries AS (
    SELECT
      query,
      hashed,
      COUNT(hashed) AS queryCount
    FROM
      hashedQueries
    GROUP BY
      hashed,
      query),
  countedAndPricedQueries AS (
    SELECT
      query,
      totalBytesBilled,
      queryCount
    FROM
      countQueries
    JOIN
      pricedQueries
    ON
      countQueries.hashed = pricedQueries.hashed)

SELECT
  query,
  queryCount,
  ROUND(SAFE_DIVIDE(totalBytesBilled, POW(1024, 4)) * 6.25, 2) AS onDemandCost,
  ROUND(COALESCE(totalBytesBilled, 0), 2) AS totalBytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / POW(1024, 2), 2) AS totalMegabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / POW(1024, 3), 2) AS totalGigabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / POW(1024, 4), 2) AS totalTerabytesBilled
FROM
  countedAndPricedQueries
ORDER BY
  queryCount DESC;