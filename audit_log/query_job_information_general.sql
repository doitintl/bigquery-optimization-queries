-- This query returns information about query jobs run over the interval

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

-- This query lists out every job over a period of time along with their execution details (including slot cout)
WITH src AS (
    SELECT
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS projectId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables as referencedTables,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.labels as labels,
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
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs AS totalSlotMs,
        SAFE_DIVIDE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
          TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
            protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
            MILLISECOND)) AS approximateSlotCount,
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
        -- Change to look at your proper dates
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
),
jobsDeduplicated AS (
    SELECT
        * EXCEPT(_rnk)
    FROM
        src
    WHERE
        _rnk = 1
)


SELECT *
FROM jobsDeduplicated
ORDER BY startTime;