-- This query will pull all jobs with labels and then sort them by cost descending

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

-- This query lists out every job over a period of time along with their execution details (including slot cout)
WITH src AS (
    SELECT
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.labels,
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
labels AS (
    SELECT
        SUM(COALESCE(totalBilledBytes, 0)) AS totalBilledBytes,
        ROUND(SUM(COALESCE(totalBilledBytes,
              0)) / 1000000, 2) AS totalMegabytesBilled,
        ROUND(SUM(COALESCE(totalBilledBytes,
              0)) / 1000000000, 2) AS totalGigabytesBilled,
        ROUND(SUM(COALESCE(totalBilledBytes,
              0)) / 1000000000000, 2) AS totalTerabytesBilled,
        ROUND(SAFE_DIVIDE(SUM(COALESCE(totalBilledBytes, 0)),
          1000000000000) * 5, 2) AS onDemandCost,
        labels.key AS labelKey,
        labels.value AS labelValue
    FROM jobsDeduplicated
    CROSS JOIN
      UNNEST(jobsDeduplicated.labels) AS labels
    WHERE
      labels.key IS NOT NULL
      AND labels.value IS NOT NULL
    GROUP BY
      labelKey,
      labelValue
)

SELECT
      labelKey,
      labelValue,
      totalBilledBytes,
      totalMegabytesBilled,
      totalTerabytesBilled,
      onDemandCost
    FROM
      labels
    ORDER BY
      onDemandCost DESC;