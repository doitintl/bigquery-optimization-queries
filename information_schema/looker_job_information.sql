-- This query will return general information from jobs run over the specified timeframe by any Looker service accounts.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

BEGIN
    WITH src AS (
      SELECT
        user_email AS user,
        job_type AS jobType,
        query,
        job_id AS jobId,
        project_id AS projectId,
        start_time AS startTime,
        end_time AS endTime,
        total_bytes_billed AS totalBytesBilled,
        total_slot_ms AS totalSlotMs,
        ROUND(SAFE_DIVIDE(total_bytes_billed,
            POW(1024, 4)) * 6.25, 2) AS cost,
        ROUND(SAFE_DIVIDE(total_slot_ms,
            TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS approximateSlotCount,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND total_slot_ms IS NOT NULL
        AND state = "DONE"
        AND REGEXP_CONTAINS(user_email, '[a-zA-Z0-9._%+-]*looker[a-zA-Z0-9._%+-]*@[a-zA-Z0-9-]+.iam.gserviceaccount.com')
      ORDER BY
        startTime DESC),
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
  jobsDeduplicated;
END