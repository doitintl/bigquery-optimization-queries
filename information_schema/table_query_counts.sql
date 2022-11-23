-- This query returns the counts and other data of jobs that query tables (project, dataset, and project included)
-- per user over the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
    WITH src AS (
      SELECT
        user_email AS user,
        project_id AS projectId,
        job_id AS jobId,
        referenced_tables AS referencedTables,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND total_slot_ms IS NOT NULL),
  jobsDeduplicated AS (
      SELECT
        * EXCEPT(_rnk)
      FROM
        src
      WHERE
        _rnk = 1),
  tables AS (
    SELECT
        jobId,
        user,
        tables.project_id AS projectId,
        tables.dataset_id AS datasetId,
        tables.table_id AS tableId
    FROM jobsDeduplicated
    CROSS JOIN
      UNNEST(jobsDeduplicated.referencedTables) AS tables)

SELECT
  tables.user,
  tables.projectId,
  tables.datasetId,
  tables.tableId,
  COUNT(tables.tableId) AS tableIdCount
FROM
  tables
GROUP BY
  tables.user,
  tables.projectId,
  tables.datasetId,
  tables.tableId;
END