-- This query aggregates costs across all queries in the specified timeframe and compares
-- on-demand pricing versus different BigQuery Editions pricing models

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 30;
DECLARE baselines_to_test ARRAY<INT64> DEFAULT [0, 50, 100, 200, 500, 1000];

BEGIN
WITH
  -- Per-minute concurrent slot demand across all queries
  per_minute AS (
    SELECT
      period_start,
      SUM(period_slot_ms) / 60000.0 AS concurrent_slots
    FROM `<project>`.`region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT
    WHERE period_start BETWEEN
        TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('America/Los_Angeles'),
                               INTERVAL interval_in_days DAY),
                  'America/Los_Angeles')
        AND CURRENT_TIMESTAMP()
      AND job_type = 'QUERY'
    GROUP BY period_start
  ),

  -- Apply autoscale 50-slot rounding at the reservation level (per minute)
  provisioned AS (
    SELECT
      period_start,
      concurrent_slots,
      CEIL(concurrent_slots / 50.0) * 50 AS provisioned_slots_no_baseline
    FROM per_minute
  ),

  -- On-demand cost (TB scanned × $6.25) — unchanged from original
  on_demand AS (
    SELECT
      COUNT(*) AS total_query_executions,
      COUNT(DISTINCT query) AS total_unique_queries,
      ROUND(SUM(GREATEST(total_bytes_billed, 10*POW(1024,2)))/POW(1024,4), 2)
        AS total_terabytes_billed,
      ROUND(SUM(GREATEST(total_bytes_billed, 10*POW(1024,2)))/POW(1024,4) * 6.25, 2)
        AS total_on_demand_cost
    FROM `<project>`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time BETWEEN
        TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('America/Los_Angeles'),
                               INTERVAL interval_in_days DAY),
                  'America/Los_Angeles')
        AND CURRENT_TIMESTAMP()
  ),

  calendar AS (
    SELECT interval_in_days * 24 * 60 AS total_calendar_minutes
  ),

  -- For each candidate baseline, model: baseline_cost + overflow_cost
  scenarios AS (
    SELECT
      bs AS baseline_slots,
      -- Slot-minutes provisioned BEYOND the committed baseline
      (SELECT SUM(GREATEST(
                    0,
                    GREATEST(provisioned_slots_no_baseline, bs) - bs))
       FROM provisioned) AS autoscale_slot_minutes,
      cal.total_calendar_minutes AS calendar_minutes
    FROM UNNEST(baselines_to_test) bs, calendar cal
  ),

  costed AS (
    SELECT
      s.baseline_slots,
      -- Standard: $0.04 PAYG, no committed-rate tiers exist on Standard
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.04
        + s.autoscale_slot_minutes / 60 * 0.04, 2)
        AS standard_total_cost,
      -- Enterprise: PAYG ($0.06), 1-yr ($0.048), 3-yr ($0.036) on baseline only.
      -- Autoscale always at $0.06 PAYG.
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.06
        + s.autoscale_slot_minutes / 60 * 0.06, 2)
        AS enterprise_payg_cost,
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.048
        + s.autoscale_slot_minutes / 60 * 0.06, 2)
        AS enterprise_1yr_cost,
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.036
        + s.autoscale_slot_minutes / 60 * 0.06, 2)
        AS enterprise_3yr_cost,
      -- Enterprise Plus: PAYG ($0.10), 1-yr ($0.08), 3-yr ($0.06) on baseline only.
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.10
        + s.autoscale_slot_minutes / 60 * 0.10, 2)
        AS enterprise_plus_payg_cost,
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.08
        + s.autoscale_slot_minutes / 60 * 0.10, 2)
        AS enterprise_plus_1yr_cost,
      ROUND(
        s.baseline_slots * s.calendar_minutes / 60 * 0.06
        + s.autoscale_slot_minutes / 60 * 0.10, 2)
        AS enterprise_plus_3yr_cost
    FROM scenarios s
  )

SELECT
  CONCAT('Last ', interval_in_days, ' days') AS time_period,
  od.total_unique_queries,
  od.total_query_executions,
  od.total_terabytes_billed,
  od.total_on_demand_cost,
  c.baseline_slots,
  c.standard_total_cost,
  c.enterprise_payg_cost,
  c.enterprise_1yr_cost,
  c.enterprise_3yr_cost,
  c.enterprise_plus_payg_cost,
  c.enterprise_plus_1yr_cost,
  c.enterprise_plus_3yr_cost,
  -- Savings vs on-demand on the cheapest Enterprise commitment
  ROUND(od.total_on_demand_cost - c.enterprise_3yr_cost, 2)
    AS savings_vs_ondemand_at_3yr,
  ROUND(100 *
    (od.total_on_demand_cost - c.enterprise_3yr_cost)
    / NULLIF(od.total_on_demand_cost, 0), 2)
    AS savings_pct_at_3yr
FROM costed c, on_demand od
ORDER BY c.baseline_slots;
END
