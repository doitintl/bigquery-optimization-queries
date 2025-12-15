-- This query will show the aggregate average slot usage of all jobs for each minute in the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

DECLARE time_period INT64 DEFAULT (1000 * 60);  -- Number of milliseconds in a minute

BEGIN
    WITH src AS (
      SELECT
        SAFE_DIVIDE(SUM(period_slot_ms), time_period) AS slot_usage,  -- Divide by 1 minute (1000 ms * 60 seconds) to convert to slots/minute
        TIMESTAMP_TRUNC(period_start, MINUTE) as period_start
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_TIMELINE
      WHERE
        period_start BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY) AND CURRENT_TIMESTAMP()
        AND statement_type <> 'SCRIPT'   -- Exclude scripts since they pull in child process slots and skew results
      GROUP BY
        period_start
      ORDER BY
        period_start DESC
    ),
    time_series AS(
    SELECT
      *
    FROM
      UNNEST(generate_timestamp_array(DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY), MINUTE),
          DATETIME_TRUNC(CURRENT_TIMESTAMP(), MINUTE),
          INTERVAL 1 MINUTE)) AS timeInterval
  ),
  joined AS (
      SELECT
        COALESCE(src.slot_usage, 0) as slot_usage,
        timeInterval
      FROM
        src RIGHT OUTER JOIN time_series
          ON period_start = timeInterval
  )

SELECT
  *
FROM
  joined
ORDER BY
  timeInterval ASC;
END