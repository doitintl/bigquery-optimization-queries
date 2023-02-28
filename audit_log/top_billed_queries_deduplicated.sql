-- This query returns the top billed queries from the past day

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

WITH src AS (
    SELECT
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
        timestamp,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS billingProjectId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
        ROUND(SAFE_DIVIDE(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
            0), 1000000000000) * 5, 2) AS onDemandCost,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0), 2) AS totalBytesBilled,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0) / 1000000, 2) AS totalMegabytesBilled,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0) / 1000000000, 2) AS totalGigabytesBilled,
        ROUND(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
          0) / 1000000000000, 2) AS totalTerabytesBilled,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query
    FROM
        `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
    WHERE
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query IS NOT NULL
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName = 'query_job_completed'
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes IS NOT NULL
            AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
            AND protopayload_auditlog.authenticationInfo.principalEmail != ""
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId = '<project-name>'
            AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
    ORDER BY
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes DESC),
jobsDeduplicated AS (
    SELECT
        * EXCEPT(_rnk),
        ROUND(SAFE_DIVIDE(executionTimeMs, totalSlotMs), 2) AS approximateSlotCount
    FROM
        src
    WHERE
        _rnk = 1 AND approximateSlotCount IS NOT NULL
)


SELECT *
FROM jobsDeduplicated
ORDER BY totalBilledBytes DESC