/*
This query pulls job data for a project and calculates job metrics 
to the minute grain (i.e. for each minute X number of jobs running 
and Y total approximate slots utilized).  This rowset is then joined
to reservations timeline data to calculate estimated utilization
of available slots per minute on a designated BigQuery Editions 
reservation (not on demand) by a project.  
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
    <project_id>.`<region-REGION>`.INFORMATION_SCHEMA.RESERVATIONS_TIMELINE
  where
  period_start > timestamp_sub(current_timestamp(), INTERVAL interval_in_days DAY)
  and reservation_name = res_name
),
base_jobs_data as 
(
  --Pull all jobs data in the org sliced by the second
  SELECT
    --Truncate period to the minute
    timestamp_trunc(a.period_start,minute) as period_start,
    a.period_slot_ms,
    a.job_id,
    a.job_start_time,
    a.job_end_time,
    a.reservation_id
  FROM
    --Fill in project_id and region where jobs are being submitted to
    <project_id>.`<region-REGION>`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION a 
    join
    --Limit jobs data only to ones that ran in the designated reservation
    (select distinct reservation_id from reservation_data) b
    on a.reservation_id = b.reservation_id
  WHERE
    period_slot_ms IS NOT NULL
    and job_creation_time > timestamp_sub(current_timestamp(), INTERVAL interval_in_days DAY)
    --Avoid duplication caused by script jobs due to parent/child thread scenarios
    and (statement_type != "SCRIPT" OR statement_type IS NULL)
),
jobs_transform1 as 
(
  --Aggregate each job's periods to the minute
  select 
    period_start,
    job_id,
    reservation_id,
    sum(period_slot_ms) as period_slot_ms_per_minute
  from base_jobs_data
  group by 1,2,3
),
jobs_transform2 as 
(
  --Convert slot_ms to slots in each job's period
  select
    period_start,
    reservation_id,
    count(distinct job_id) as active_jobs_per_period,
    round(safe_divide(cast(sum(period_slot_ms_per_minute) as float64),60000),2) as slots_per_period
  from
  jobs_transform1
  group by 1,2
)
--Join reservation periods with job periods for final recordset.
select
  r.*,
  coalesce(j.active_jobs_per_period,0) as active_jobs_per_period,
  coalesce(j.slots_per_period,0) as slots_per_period,
  case 
    when r.current_slots = 0 then 0
    else (j.slots_per_period/r.current_slots)*100
  end as utilization_pct
from
reservation_data r
left join 
jobs_transform2 j
on r.period_start = j.period_start
and r.reservation_id = j.reservation_id
order by r.period_start desc
;
