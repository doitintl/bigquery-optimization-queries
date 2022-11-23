-- This query lists out every Looker job over a period of time along with their execution details (including slot cout)
-- This will look for a Looker service account being the

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 5;

WITH src AS (
    SELECT
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
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
        TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
                       protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
                       MILLISECOND) AS executionTimeMs,
        ROW_NUMBER() OVER(PARTITION BY protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId ORDER BY timestamp DESC) AS _rnk
    FROM `<project>.<dataset>cloudaudit_googleapis_com_data_access`
    WHERE
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE '%_job_completed'
        AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
        AND protopayload_auditlog.authenticationInfo.principalEmail != ""
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun IS NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId IS NOT NULL
        AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId = '<project-name>'
        -- Filter by a regex looking for a Looker service account
        AND REGEXP_CONTAINS(protopayload_auditlog.authenticationInfo.principalEmail, '[a-zA-Z0-9._%+-]*looker[a-zA-Z0-9._%+-]*@[a-zA-Z0-9-]+.iam.gserviceaccount.com')
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
        _rnk = 1
)


SELECT *
FROM jobsDeduplicated
ORDER BY startTime