-- This query will show the aggregate average slot usage of all jobs for each second in the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

DECLARE time_period INT64;
SET time_period = (1000);  -- Number of milliseconds in a second

BEGIN
    WITH src AS (
      SELECT
        SAFE_DIVIDE(SUM(total_slot_ms), time_period) AS slotUsage,
        DATETIME_TRUNC(creation_time,
          SECOND) AS creationTime
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND total_slot_ms IS NOT NULL
      GROUP BY
        creationTime),
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
          ON creationTime = timeInterval)

SELECT
  *
FROM
  joined
ORDER BY
  timeInterval ASC;
END