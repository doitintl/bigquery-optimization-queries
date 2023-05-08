-- This query returns the users that have spent the most amount of money in the project over the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
    WITH src AS (
        SELECT
          user_email AS user,
          job_id AS jobId,
          end_time AS endTime,
          ROUND(COALESCE(total_bytes_billed, 0), 2) AS totalBytesBilled,
          ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
        FROM
          `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
          creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
          AND CURRENT_TIMESTAMP()
          AND job_type = "QUERY"
          AND total_slot_ms IS NOT NULL
          AND state = "DONE"
        ),
    jobsDeduplicated AS (
        SELECT
            user,
            ROUND(SAFE_DIVIDE(SUM(src.totalBytesBilled), POW(1024, 4)) * 5, 2) AS onDemandCost
        FROM
            src
        WHERE
            _rnk = 1
        GROUP BY user
    )

SELECT
    *
FROM
    jobsDeduplicated
ORDER BY
    user DESC,
    onDemandCost DESC;
END