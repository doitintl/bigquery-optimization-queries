-- This query returns the most complex query in descending order from most complex to least complex.
-- Complexity is defined by average slot usage during the job.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
    WITH src AS (
        SELECT
          ROUND(SAFE_DIVIDE(total_slot_ms,
            TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS approximateSlotCount,
          user_email AS user,
          query,
          job_id AS jobId,
          project_id AS projectId,
          start_time AS startTime,
          end_time AS endTime,
          total_bytes_billed AS totalBytesBilled,
          ROUND(SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) * 5, 2) AS onDemandCost,
          TIMESTAMP_DIFF(end_time, start_time, SECOND) AS executionTime,
          TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS executionTimeMs,
          ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
        FROM
          `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
          creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
          AND CURRENT_TIMESTAMP()
          AND job_type = "QUERY"
          AND total_slot_ms IS NOT NULL
          AND state = "DONE"),
    jobsDeduplicated AS (
      SELECT
        * EXCEPT(_rnk)
      FROM
        src
      WHERE
        _rnk = 1 )

SELECT
    *
FROM
    jobsDeduplicated
ORDER BY
    approximateSlotCount DESC;
END