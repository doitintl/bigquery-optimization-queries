/*
 *  This query retrieves a detailed, per-second timeline of how slots were utilized and assigned, helping you monitor and understand your BigQuery reservation performance.
 *  It is useful for observing the autoscaler behavior on a per-reservation basis.

 *  Instructions for use:
 *  1. Modify the <project-name> and <dataset-region> placeholders below to match your required values. 
 *  2. Change the interval_in_days value to travel back further in time. By default this is 7 days.

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

WITH
  slot_data AS (
  SELECT
    -- Start time (aggregated per-second)
    details.start_time,
    reservation_name,
    edition,
    -- Number of slots added to the reservation by autoscaling at this second.
    details.autoscale_current_slots,
    ((CASE edition
          WHEN 'STANDARD' THEN standard_edition_cost
          WHEN 'ENTERPRISE' THEN enterprise_edition_cost
          WHEN 'ENTERPRISE PLUS' THEN enterprise_plus_edition_cost
      END
        )/3600) * details.autoscale_current_slots AS autoscaled_cost,
    -- Maximum number of slots that could be added to the reservation by autoscaling at this second.
    details.autoscale_max_slots,
    -- Number of slots assigned to this reservation at this second. It equals the baseline slot capacity of a reservation.
    details.slots_assigned,
    ((CASE edition
          WHEN 'STANDARD' THEN standard_edition_cost
          WHEN 'ENTERPRISE' THEN enterprise_edition_cost
          WHEN 'ENTERPRISE PLUS' THEN enterprise_plus_edition_cost
      END
        )/3600) * details.slots_assigned AS slots_assigned_cost,
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
)

SELECT
  start_time,
  reservation_name,
  autoscale_current_slots,
  autoscaled_cost,
  slots_assigned slots_assigned_cost,
  slots_max_assigned,
  autoscaled_cost + slots_assigned_cost AS total_slots_cost,
  CONCAT('$ ',FORMAT("%'.2f", autoscaled_cost + slots_assigned_cost)) AS total_slots_cost_formatted
FROM
  slot_data
ORDER BY
  start_time DESC