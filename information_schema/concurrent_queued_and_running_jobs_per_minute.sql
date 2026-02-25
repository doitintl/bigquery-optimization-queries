/**
 * This query will show the concurrent number of queued and running jobs for each minute in the specified timeframe.
 */

-- Update this to go back however many hours you want to look at
-- Do not recommend going back more than 24 hours, as it will can get expensive
DECLARE interval_in_hours INT64 DEFAULT 24;

WITH time_series AS (
  -- Generate a list of every minute for the last 24 hours
  SELECT 
    minute 
  FROM 
    UNNEST(
      GENERATE_TIMESTAMP_ARRAY(
        TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_hours HOUR), MINUTE), 
        TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), 
        INTERVAL 1 MINUTE
      )
    ) AS minute
),
job_data AS (
  -- Get all query jobs from the last day
  SELECT
    job_id,
    creation_time,
    start_time,
    end_time,
    -- A job is 'Running' if it has started but not finished
    -- A job is 'Queued' if it was created but hasn't started yet
    -- We use creation_time, start_time, and end_time to define these windows
  FROM 
    `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE 
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (interval_in_hours + 1)  HOUR) -- Buffer for long jobs
    AND job_type = 'QUERY'
    AND statement_type != 'SCRIPT' -- Optional: excludes child jobs of scripts
)

SELECT
  t.minute,
  COUNTIF(j.start_time <= t.minute AND (j.end_time > t.minute OR j.end_time IS NULL)) AS concurrent_running,
  COUNTIF(j.creation_time <= t.minute AND (j.start_time > t.minute OR j.start_time IS NULL)) AS concurrent_queued
FROM 
  time_series t
LEFT JOIN 
  job_data j
  ON (
    -- Match if the job was alive (either queued or running) during this minute
    j.creation_time <= t.minute 
    AND (j.end_time > t.minute OR j.end_time IS NULL)
  )
GROUP BY 1
ORDER BY 1 DESC