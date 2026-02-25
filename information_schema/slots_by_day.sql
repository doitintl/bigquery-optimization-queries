-- This query will show the aggregate average slot usage of all jobs for each day in the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

DECLARE time_period INT64 DEFAULT (1000*60*60*24); -- Number of milliseconds in a day

BEGIN
    WITH src AS (
      SELECT
        SAFE_DIVIDE(SUM(period_slot_ms), time_period) AS slot_usage,
        -- Truncate the period_start to the day in PST
        TIMESTAMP_TRUNC(period_start, DAY, 'America/Los_Angeles') as period_start_pst
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_TIMELINE
      WHERE
        -- Filter using PST boundaries
        period_start BETWEEN 
            TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('America/Los_Angeles'), INTERVAL interval_in_days DAY), 'America/Los_Angeles') 
            AND CURRENT_TIMESTAMP()
        AND statement_type <> 'SCRIPT'   -- Exclude scripts since they pull in child process slots and skew results
      GROUP BY
        period_start_pst
    ),
    time_series AS (
      SELECT
        timeInterval
      FROM
        UNNEST(GENERATE_TIMESTAMP_ARRAY(
          TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY), DAY, 'America/Los_Angeles'),
          TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'),
          INTERVAL 1 DAY)) AS timeInterval
    ),
    joined AS (
      SELECT
        COALESCE(src.slot_usage, 0) as slot_usage,
        -- Convert the final output to a DATETIME string for easier PST reading
        DATETIME(timeInterval, 'America/Los_Angeles') AS timeInterval_PST
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
