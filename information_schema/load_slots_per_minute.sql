-- This query will show the slot usage of all load jobs for each second in the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

BEGIN
    WITH src AS (
      SELECT
        user_email AS user,
        job_id AS jobId,
        query,
        DATETIME_TRUNC(start_time,
          MINUTE) AS startTime,
        DATETIME_TRUNC(end_time,
          MINUTE) AS endTime,
        SAFE_DIVIDE(total_slot_ms,
          TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS slotCount,
        TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS diff,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND total_slot_ms IS NOT NULL
        AND job_type = 'LOAD'),
  jobsDeduplicated AS (
      SELECT
        * EXCEPT(_rnk)
      FROM
        src
      WHERE
        _rnk = 1 ),
  differences AS (
      SELECT
        *,
        generate_timestamp_array(startTime,
          endTime,
          INTERVAL 1 MINUTE) AS int
      FROM
        jobsDeduplicated ),
  byMinutes AS (
      SELECT
        * EXCEPT(int)
      FROM
        differences,
        UNNEST(int) AS minute )

SELECT
  slotCount,
  user,
  jobId,
  query,
  minute
FROM
  byMinutes
ORDER BY
  minute ASC;
END