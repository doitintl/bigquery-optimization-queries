/*
This query will show the number of queued jobs broken down by seconds, regardless 
of job type.

Nested query can be used separately to identify job_ids when a hotspot timeframe has been 
identified.
*/

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 1;

with base_data as 
(
  select
    period_start,
    job_id,
    state,
    error_result.reason,
    error_result.location,
    error_result.message
  from
    `wpro-kraken-314320`.`region-us-west2`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT
  where
  job_creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
      AND CURRENT_TIMESTAMP()
  AND state = 'PENDING'
)
select
  period_start,
  count(distinct job_id) as queued_jobs_cnt
from
  base_data
group by 1
order by 1;