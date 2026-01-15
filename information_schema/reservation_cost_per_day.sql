/*
 *  This query does cost reconciliation for Editions reservations, particularly for the autoscaler.
 *  It uses the RESERVATIONS_TIMELINE view and the period_autoscale_slot_seconds column
 *  that was added to it on 1/12/2026.
 *  
 *  This column contains the billed slot seconds for each reservation on a per-minute basis.
 *  Due to this it makes it possible to calculate the exact cost for each reservation.
 *  
 *  This also takes into account the commitments for each edition and factors those into the calculation.
*/

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

/*
 *  These values are pricing values for each edition and commitment type.
 *  Update these values to match your specific pricing.
 *  These are defaulted for US multi-region, but may need to be updated for your region.
 *
 *  Values can be found here: https://cloud.google.com/bigquery/pricing
*/
DECLARE standard_payg FLOAT64 DEFAULT 0.04;

DECLARE enterprise_payg FLOAT64 DEFAULT 0.06;
DECLARE enterprise_1y_commit   FLOAT64 DEFAULT 0.048; 
DECLARE enterprise_3y_commit   FLOAT64 DEFAULT 0.036; 

DECLARE enterprise_plus_payg FLOAT64 DEFAULT 0.10;
DECLARE enterprise_plus_1y_commit   FLOAT64 DEFAULT 0.08;  
DECLARE enterprise_plus_3y_commit   FLOAT64 DEFAULT 0.06;  

-- 3. MAIN QUERY
WITH current_reservations AS (
  -- Get the current edition for each reservation
  SELECT reservation_name, edition
  FROM `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.RESERVATIONS
),

commitment_totals AS (
  -- Calculate commitment totals grouped by Edition
  SELECT 
    edition,
    SUM(slot_count) AS total_slots_in_edition,
    SUM(CASE 
      WHEN edition = 'ENTERPRISE' AND commitment_plan = 'ANNUAL' THEN slot_count * enterprise_1y_commit
      WHEN edition = 'ENTERPRISE' AND commitment_plan = 'THREE_YEAR' THEN slot_count * enterprise_3y_commit
      WHEN edition = 'ENTERPRISE_PLUS' AND commitment_plan = 'ANNUAL' THEN slot_count * enterprise_plus_1y_commit
      WHEN edition = 'ENTERPRISE_PLUS' AND commitment_plan = 'THREE_YEAR' THEN slot_count * enterprise_plus_3y_commit
      ELSE 0 
    END) AS hourly_cost_of_commitments
  FROM `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.CAPACITY_COMMITMENTS
  WHERE state = 'ACTIVE'
  GROUP BY edition
),

pricing_map AS (
  -- Generate the specific rates for each edition
  SELECT 
    e.edition,
    COALESCE(SAFE_DIVIDE(ct.hourly_cost_of_commitments, ct.total_slots_in_edition), 
             CASE WHEN e.edition = 'ENTERPRISE' THEN enterprise_payg 
                  WHEN e.edition = 'ENTERPRISE_PLUS' THEN enterprise_plus_payg 
                  ELSE standard_payg END) AS blended_baseline_rate,
    CASE WHEN e.edition = 'ENTERPRISE' THEN enterprise_payg 
         WHEN e.edition = 'ENTERPRISE_PLUS' THEN enterprise_plus_payg 
         ELSE standard_payg END AS autoscale_list_rate
  FROM (SELECT 'STANDARD' as edition UNION ALL SELECT 'ENTERPRISE' UNION ALL SELECT 'ENTERPRISE_PLUS') e
  LEFT JOIN commitment_totals ct ON e.edition = ct.edition
),

daily_usage AS (
  -- Aggregate usage using the 'period_autoscale_slot_seconds' column at the day level
  -- This column is the billed price in slot seconds aggregated at the minute level
  SELECT
    EXTRACT(DATE FROM period_start) AS usage_date,
    reservation_name,
    SUM(slot_capacity) / 60 AS baseline_slot_hours,
    -- Divide by 3600 (60 minutes * 60 seconds) to convert from slot-seconds to slot-hours
    SUM(period_autoscale_slot_seconds) / 3600 AS autoscale_slot_hours
  FROM `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.RESERVATIONS_TIMELINE
  WHERE period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
  GROUP BY usage_date, reservation_name
)

SELECT
  u.usage_date,
  u.reservation_name,
  r.edition,
  ROUND(u.baseline_slot_hours, 2) AS total_baseline_slot_hours,
  ROUND(u.autoscale_slot_hours, 2) AS total_autoscale_slot_hours,
  
  -- Apply the edition-specific rates
  ROUND(u.baseline_slot_hours * p.blended_baseline_rate, 2) AS daily_baseline_cost_usd,
  ROUND(u.autoscale_slot_hours * p.autoscale_list_rate, 2) AS daily_autoscale_cost_usd,
  ROUND(
    (u.baseline_slot_hours * p.blended_baseline_rate) + 
    (u.autoscale_slot_hours * p.autoscale_list_rate), 2
  ) AS total_daily_reservation_cost_usd

FROM daily_usage u
JOIN current_reservations r ON u.reservation_name = r.reservation_name
LEFT JOIN pricing_map p ON r.edition = p.edition
ORDER BY usage_date DESC, total_daily_reservation_cost_usd DESC;