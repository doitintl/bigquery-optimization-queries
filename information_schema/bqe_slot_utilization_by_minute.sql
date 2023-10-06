/*
This query pulls job data for a project and calculates job metrics 
to the minute grain (i.e. for each minute X number of jobs running 
and Y total approximate slots utilized).  This rowset is then joined
to reservations timeline data to calculate estimated utilization 
of available slots per minute on a designated BigQuery Editions 
reservation (not on demand).  
*/

declare interval_in_days int64;
declare res_name string;
set interval_in_days = 7;
set res_name = '<my_reservation_name>';

with reservation_data as 
(
  select
    period_start,
    reservation_name,
    autoscale.current_slots,
    autoscale.max_slots,
    reservation_id
  from
  --Fill in project_id and region where reservation is set up.
    <project_id>.`<region-REGION>`.INFORMATION_SCHEMA.RESERVATIONS_TIMELINE_BY_PROJECT
  where
  period_start > timestamp_sub(current_timestamp(), INTERVAL interval_in_days DAY)
  and reservation_name = res_name
),
base_jobs_data as 
(
  SELECT
    job_id,
    start_time,
    end_time,
    --Calculate the job run time in minutes (rounded up)
    CEIL(TIMESTAMP_DIFF(end_time,start_time,MILLISECOND)/1000/60) AS run_duration_minutes,
    total_slot_ms,
    total_bytes_processed,
    total_bytes_billed,
    --Calculate total approximate slot count for the job
    ROUND(SAFE_DIVIDE(total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS total_slot_count,
    --Calculate slot milliseconds per minute of job run time
    total_slot_ms/CEIL(TIMESTAMP_DIFF(end_time,start_time,MILLISECOND)/1000/60) AS slot_ms_per_minute,
    --Calculate approximate slot count per minute of job run time
    ROUND(SAFE_DIVIDE((total_slot_ms/CEIL(TIMESTAMP_DIFF(end_time,start_time,MILLISECOND)/1000/60)), TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS total_slot_count_per_minute,
  FROM
    --Fill in project_id and region where jobs are being executed from
    <project_id>.`<region-REGION>`.INFORMATION_SCHEMA.JOBS
  WHERE
    total_slot_ms IS NOT NULL
    and creation_time > timestamp_sub(current_timestamp(), INTERVAL interval_in_days DAY)
    and (statement_type != "SCRIPT" OR statement_type IS NULL)
    --Limit jobs data only to ones that ran in the designated reservation
    and reservation_id in (select distinct reservation_id from reservation_data)
),
--Isolate multiple minute jobs for the next step
multiple_min_data as 
(
  select * from base_jobs_data where run_duration_minutes > 1
),
--For jobs that ran more than 1 minute, generate a duplicate row for each minute
generate_single_minute_data as
(
  select
    job_id,
    start_time,
    end_time,
    slot_ms_per_minute,
    total_slot_count_per_minute,
    --Generate a timestamp for each minute and truncate the seconds
    timestamp_trunc(minute_start, minute) as minute_timestamp
  from multiple_min_data
  cross join unnest(generate_timestamp_array(start_time, end_time, interval 1 minute)) as minute_start
),
--Combine all single minute rows
combined_detail_data as 
(
  select
    job_id,
    start_time,
    end_time,
    slot_ms_per_minute,
    total_slot_count_per_minute,
    minute_timestamp
  from generate_single_minute_data
  union all
  select
    job_id,
    start_time,
    end_time,
    slot_ms_per_minute,
    total_slot_count_per_minute,
    --Generate a timestamp for each minute and truncate the seconds
    timestamp_trunc(start_time, minute) as minute_timestamp
  from base_jobs_data
  where run_duration_minutes < 2  
),
agg_metrics_per_minute as 
(
  select
    minute_timestamp as job_run_minute,
    count(distinct job_id) as job_count_per_minute,
    sum(total_slot_count_per_minute) as sum_slots_per_minute
  from
  combined_detail_data
  group by 1
)
select 
  a.*,
  r.period_start,
  r.reservation_name,
  r.autoscale.current_slots,
  r.autoscale.max_slots,
  --Calculate approximate utilization of slots available by the jobs running during that minute
  case 
    when r.autoscale.current_slots = 0 then 0 
    else (a.sum_slots_per_minute/r.autoscale.current_slots)*100 
  end as utilization_pct
from agg_metrics_per_minute a 
left join
reservation_data r
on a.job_run_minute = r.period_start
order by job_run_minute desc;