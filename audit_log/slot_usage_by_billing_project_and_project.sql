-- This query returns slot usage over an interval per billing project, project, dataset, and table

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

WITH
src AS (
    SELECT
        timestamp,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS billingProjectId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs
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
            -- To change the interval change these two lines
            AND DATE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun IS NULL
            AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)),
jobs AS (
    SELECT
        *,
        TIMESTAMP_DIFF(endTime, startTime, MILLISECOND) as executionTimeMs,
        ROW_NUMBER() OVER(PARTITION BY jobId ORDER BY timestamp DESC) AS _rnk
    FROM
        src ),
jobsDeduplicated AS (
    SELECT
        * EXCEPT(_rnk),
        executionTimeMs / totalSlotMs AS approximateSlotCount
    FROM
        jobs
    WHERE
        _rnk = 1 )

SELECT
    billingProjectId,
    tables.projectId,
    tables.datasetId,
    tables.tableId,
    ROUND(SUM(approximateSlotCount), 2) AS slotUsage
FROM
    jobsDeduplicated,
    UNNEST(referencedTables) AS tables
GROUP BY
    billingProjectId,
    tables.projectId,
    tables.datasetId,
    tables.tableId
ORDER BY
    slotUSage DESC