/*
 *  This query will return back the project, location, job_type, and a counter of the number of jobs running in the combination
 *  of them for all projects included in the BQ audit log.
 *
 *  This will assist in tracking down jobs running outside of expected regions or show an overview of where job types are
 *  are running throughout your organization.
 */

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

SELECT
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId AS project_id,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location AS location,
  UPPER(REPLACE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName, '_job_completed', '')) AS job_type,
  COUNT(3) AS job_type_counter
FROM
  `<project>.<dataset>.cloudaudit_googleapis_com_data_access`
WHERE
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId IS NOT NULL
  AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId NOT LIKE 'script_job_%' -- filter BQ script child jobs
  AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName LIKE '%_job_completed'
  AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
  AND protopayload_auditlog.authenticationInfo.principalEmail != ""
  AND (timestamp) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
GROUP BY
  1,
  2,
  3