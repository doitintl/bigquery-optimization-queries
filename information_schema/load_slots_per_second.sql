-- This query will show the slot usage of all load jobs for each second in the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

DECLARE time_period INT64 DEFAULT (1000);  -- Number of milliseconds in a second

BEGIN
    WITH src AS (
      SELECT
        SAFE_DIVIDE(SUM(period_slot_ms), time_period) AS slotUsage,  -- Divide by 1 second (1000 ms) to convert to slots/second
        period_start
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_TIMELINE
      WHERE
        period_start BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY) AND CURRENT_TIMESTAMP()
        AND job_type = 'LOAD'
      GROUP BY
        period_start
      ORDER BY
        period_start DESC
    ),
    timeSeries AS(
    SELECT
      *
    FROM
      UNNEST(generate_timestamp_array(DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY), SECOND),
          DATETIME_TRUNC(CURRENT_TIMESTAMP(), SECOND),
          INTERVAL 1 SECOND)) AS timeInterval
  ),
  joined AS (
      SELECT
        COALESCE(src.slotUsage, 0) as slotUsage,
        timeInterval
      FROM
        src RIGHT OUTER JOIN timeSeries
          ON period_start = timeInterval
  )

SELECT
  *
FROM
  joined
ORDER BY
  timeInterval ASC;
END