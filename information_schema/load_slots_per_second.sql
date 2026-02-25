-- This query will show the slot usage of all load jobs for each second in the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

DECLARE time_period INT64 DEFAULT (1000); -- Number of milliseconds in a second

BEGIN
    WITH src AS (
      SELECT
        SAFE_DIVIDE(SUM(period_slot_ms), time_period) AS slotUsage,
        -- Truncate the period_start to the second in PST
        TIMESTAMP_TRUNC(period_start, SECOND, 'America/Los_Angeles') as period_start_pst
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_TIMELINE
      WHERE
        -- Filter using PST boundaries
        period_start BETWEEN 
            TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('America/Los_Angeles'), INTERVAL interval_in_days DAY), 'America/Los_Angeles') 
            AND CURRENT_TIMESTAMP()
        AND job_type = 'LOAD'
      GROUP BY
        period_start_pst
    ),
    time_series AS (
      SELECT
        timeInterval
      FROM
        UNNEST(GENERATE_TIMESTAMP_ARRAY(
          TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY), SECOND, 'America/Los_Angeles'),
          TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND, 'America/Los_Angeles'),
          INTERVAL 1 SECOND)) AS timeInterval
    ),
  joined AS (
      SELECT
        COALESCE(src.slotUsage, 0) as slotUsage,
        -- Convert the final output to a DATETIME string for easier PST reading
        DATETIME(time_series.timeInterval, 'America/Los_Angeles') AS timeInterval_PST
      FROM
        time_series
      LEFT OUTER JOIN src 
        ON time_series.timeInterval = src.period_start_pst
    )

SELECT
  *
FROM
  joined
ORDER BY
  timeInterval_PST ASC;
END