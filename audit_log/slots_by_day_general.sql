-- This query returns how many slots were used on a per day basis

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

WITH src AS (
SELECT
      protopayload_auditlog.authenticationInfo.principalEmail AS user_email,
        CASE protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName
            WHEN 'query_job_completed' THEN 'QUERY'
            WHEN 'load_job_completed' THEN 'LOAD'
            WHEN 'extract_job_completed' THEN 'EXTRACT'
            WHEN 'table_copy_job_completed' THEN 'TABLE COPY'
        END AS eventType,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.extract,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.tableCopy,
        timestamp,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS billingProjectId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
        SAFE_DIVIDE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
          TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
            protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
            MILLISECOND)) AS slotCount,
        ROW_NUMBER() OVER(PARTITION BY protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId ORDER BY timestamp DESC) AS _rnk
    FROM
      `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
    WHERE
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE '%_job_completed'
      AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
      AND protopayload_auditlog.authenticationInfo.principalEmail != ""
      AND DATE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun IS NULL
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
      AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
),
jobsDeduplicated AS (
    SELECT
      * EXCEPT(_rnk)
    FROM
      src
    WHERE
      _rnk = 1),
differences AS (
  SELECT
    *,
    GENERATE_TIMESTAMP_ARRAY(startTime, endTime, INTERVAL 1 DAY) AS int
  FROM
    jobsDeduplicated),
byDays AS (
  SELECT
    * EXCEPT(int)
  FROM
    differences,
    UNNEST(int) AS day)

SELECT
  day,
  eventType,
  SUM(slotCount) AS slotCount
FROM byDays
WHERE slotCount IS NOT NULL
GROUP BY day, eventType
ORDER BY day ASC