-- This query returns the counts and other data of jobs that query tables (project, dataset, and project included)
-- per user over the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 700;

-- This query lists out every job over a period of time along with their execution details (including slot cout)
WITH src AS (
    SELECT
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables,
        ROW_NUMBER() OVER(PARTITION BY protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId ORDER BY timestamp DESC) AS _rnk
    FROM `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
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
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL interval_in_days DAY)
),
jobsDeduplicated AS (
    SELECT
        * EXCEPT(_rnk)
    FROM
        src
    WHERE
        _rnk = 1
),
tables AS (
    SELECT
        jobId,
        user,
        tables.projectId AS projectId,
        tables.datasetId AS datasetId,
        tables.tableId AS tableId
    FROM jobsDeduplicated
    CROSS JOIN
      UNNEST(jobsDeduplicated.referencedTables) AS tables
)

SELECT
  user,
  projectId,
  datasetId,
  tableId,
  COUNT(tableId) AS jobCount
FROM
  tables
GROUP BY
  user,
  projectId,
  datasetId,
  tableId;
