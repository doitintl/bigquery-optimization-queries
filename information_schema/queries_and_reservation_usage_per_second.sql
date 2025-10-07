/*
 *  This query retrieves a detailed, per-second timeline of how slots were utilized and assigned, helping you monitor and understand your BigQuery reservation performance.
 *  It is useful for observing the autoscaler behavior on a per-reservation basis.

 *  Instructions for use:
 *  1. Modify the <project-name> and <dataset-region> placeholders below to match your required values. 
 *  2. Change the interval_in_days value to travel back further in time. By default this is 7 days.
 *
 *  Important Notes:
 *  This uses the INFORMATION_SCHEMA.TIMELINE_BY_ORGANIZATION vieW, which might need additional permissions or you might need to add specific reservations to the WHERE clause to filter out.
 *  The INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION view only retains data for 7 days, so you will only be able to go back that far in time.
 *  The jobs column is a repeated field (an array of structs) showing the jobs that were running during that second for that reservation.
 *
 *  Note: If not using the US or EU multi-regions, then the costs may be different.
 *    Change the standard_edition_cost, enterprise_edition_cost, and enterprise_plus_edition_cost values below to match the actual cost listed here:
 *    https://cloud.google.com/bigquery/pricing?hl=en#:~:text=size%20calculation.-,Capacity%20compute%20pricing,-BigQuery%20offers%20a
 */

-- Modify this to go further back in time
DECLARE interval_in_days INT64 DEFAULT 7;

-- Modify these values if not using the US or EU multi-regions
-- Values can be found here: https://cloud.google.com/bigquery/pricing?hl=en#:~:text=size%20calculation.-,Capacity%20compute%20pricing,-BigQuery%20offers%20a
DECLARE standard_edition_cost NUMERIC DEFAULT 0.04;
DECLARE enterprise_edition_cost NUMERIC DEFAULT 0.06;
DECLARE enterprise_plus_edition_cost NUMERIC DEFAULT 0.10;

WITH slot_data AS
(
  SELECT
    -- Start time (aggregated per-second)
    details.start_time,
    reservation_name,
    edition,
    -- Number of slots added to the reservation by autoscaling at this second.
    details.autoscale_current_slots,
    -- Maximum number of slots that could be added to the reservation by autoscaling at this second.
    details.autoscale_max_slots,
    -- Number of slots assigned to this reservation at this second. It equals the baseline slot capacity of a reservation.
    details.slots_assigned,
    -- Maximum slot capacity for this reservation, including slot sharing at this second.
    -- If ignore_idle_slots is true, this field is same as slots_assigned.
    -- Otherwise, the slots_max_assigned field is the total number of slots in all capacity commitments in the administration project.
    details.slots_max_assigned
  FROM
    -- Schema reference is here: https://cloud.google.com/bigquery/docs/information-schema-reservation-timeline#schema
    -- Change the <project-name> and <dataset-region> placeholders below to match your required values
    `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.RESERVATIONS_TIMELINE,
    -- The per_second_details is an array of structs, so needs to be unnested and joined versus rest of the data
    -- The full row is per minute, and the details are per second within that minute thus the need for an array in this column
    UNNEST(per_second_details) AS details
  WHERE
    period_start BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
),
job_data AS
(
    SELECT
      period_start,
      -- Extract the reservation name from the fully-qualified reservation ID returned by this view
      SPLIT(reservation_id, '.')[OFFSET(ARRAY_LENGTH(SPLIT(reservation_id, '.')) - 1)] AS reservation_name,
      -- Create an array of structs showing the jobs that were running during this period
      ARRAY_AGG(
        STRUCT(
          period_slot_ms,
          period_slot_ms/1000 AS period_slot_s,
          ((CASE edition
            WHEN 'STANDARD' THEN standard_edition_cost
            WHEN 'ENTERPRISE' THEN enterprise_edition_cost
            WHEN 'ENTERPRISE PLUS' THEN enterprise_plus_edition_cost
            END
            )/3600) * (period_slot_ms/1000) AS editions_cost,
          project_id,
          user_email,
          job_id,
          statement_type,
          job_start_time,
          state,
          -- Note that the bytes billed reported are for the entire job, not just the portion that ran during this second
          -- This is because the jobs can run across multiple seconds, and the view does not provide this for on-demand jobs
          total_bytes_billed AS total_bytes_billed_for_full_job
        )
      ) AS jobs
    FROM
      `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE
      period_start BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
      -- Use the partition column to grab jobs as well to reduce bytes processed
      -- This might cause some jobs to be lost if they started before the boundary, but otherwise
      -- this query might process too much data for many users.
      -- Note this is job_creation_time, not creation_time as the docs say
      AND job_creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
      -- Note that this only pulls queries and excludes SCRIPT job types
      -- Change this if needed for other job types
      AND job_type = 'QUERY'
      AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
    GROUP BY
      period_start, reservation_name
),
combined_data AS
(
  SELECT
    start_time,
    slot_data.reservation_name,
    edition,
    autoscale_current_slots,
    autoscale_max_slots,
    slots_assigned,
    slots_max_assigned,
    jobs
  FROM
    slot_data JOIN job_data
      ON slot_data.start_time = job_data.period_start
      AND slot_data.reservation_name = job_data.reservation_name
)

SELECT
  *
FROM
  combined_data
ORDER BY
  combined_data.start_time DESC
