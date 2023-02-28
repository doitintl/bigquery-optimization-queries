-- This query lists out query job in descending order by complexity (slot count)

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 5;

WITH src AS (
    SELECT
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS billingProjectId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
        ROUND(SAFE_DIVIDE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes, 1000000000000) * 5, 2) AS onDemandCost,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
        TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
                       protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
                       MILLISECOND) AS executionTimeMs,
        ROW_NUMBER() OVER(PARTITION BY protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId ORDER BY timestamp DESC) AS _rnk
    FROM `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
    WHERE
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE 'query_job_completed'
        AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
        AND protopayload_auditlog.authenticationInfo.principalEmail != ""
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun IS NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId = '<project-name>'
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
),
jobsDeduplicated AS (
    SELECT
        ROUND(SAFE_DIVIDE(executionTimeMs, totalSlotMs), 2) AS approximateSlotCount,
        * EXCEPT(_rnk)
    FROM
        src
    WHERE
        _rnk = 1 AND approximateSlotCount IS NOT NULL
)


SELECT *
FROM jobsDeduplicated
ORDER BY approximateSlotCount DESC