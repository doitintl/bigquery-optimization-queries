-- This query will show the count of concurrent jobs broken down by seconds

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

BEGIN
WITH
  src AS (
  SELECT
    TIMESTAMP_TRUNC(period_start, MILLISECOND) AS period_start,
    job_id,
    ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_creation_time DESC) AS _rnk
  FROM
     `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT
  WHERE
    job_creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
    AND CURRENT_TIMESTAMP()
    AND state = 'DONE'
    AND job_type = 'QUERY'
    AND parent_job_id IS NULL),
  jobsDeduplicatedAndCounted AS (
  SELECT
    period_start,
    COUNT(job_id) AS job_count
  FROM
    src
  WHERE
    _rnk = 1
  GROUP BY
    src.period_start )

SELECT
  *
FROM
  jobsDeduplicatedAndCounted
  ORDER BY period_start ASC;
END