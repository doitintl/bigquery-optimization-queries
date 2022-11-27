-- This query lists out every load job over a period of time along with their execution details (including slot cout)

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 5;

WITH src AS (
SELECT
  protopayload_auditlog.authenticationInfo.principalEmail AS user_email,
  timestamp,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS billingProjectId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load.sourceUris,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load.destinationTable.projectId AS destinationProjectId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load.destinationTable.datasetId AS destinationDatasetId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load.destinationTable.tableId AS destinationTabletId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
  TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
                 protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
                 MILLISECOND) as executionTimeMs,
  ROW_NUMBER() OVER(PARTITION BY protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId ORDER BY timestamp DESC) AS _rnk
FROM `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
WHERE
protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load IS NOT NULL
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName = 'load_job_completed'
      AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
      AND protopayload_auditlog.authenticationInfo.principalEmail != ""
      AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
      -- Change to look at your proper dates
      AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
),
jobsDeduplicated AS (
    SELECT
      * EXCEPT(_rnk),
      SAFE_DIVIDE(executionTimeMs, totalSlotMs) AS approximateSlotCount
    FROM
      src
    WHERE
      _rnk = 1 )


SELECT *
FROM jobsDeduplicated
ORDER BY startTime