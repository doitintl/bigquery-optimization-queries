-- This query returns general information about all jobs run over the specified timeframe

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 7;

BEGIN
    WITH src AS (
      SELECT
        user_email AS user,
        job_type AS jobType,
        reservation_id AS reservationId,
        IF(reservation_id IS NULL, 'On-Demand', 'Editions') AS queryBillingType,
        query,
        job_id AS jobId,
        project_id AS projectId,
        start_time AS startTime,
        end_time AS endTime,
        ROUND(COALESCE(total_bytes_billed, 0), 2) AS totalBytesBilled,
        ROUND(COALESCE(total_bytes_billed, 0) / POW(1024, 2), 2) AS totalMegabytesBilled,
        ROUND(COALESCE(total_bytes_billed, 0) / POW(1024, 3), 2) AS totalGigabytesBilled,
        ROUND(COALESCE(total_bytes_billed, 0) / POW(1024, 4), 2) AS totalTerabytesBilled,
        total_slot_ms AS totalSlotMs,
        ROUND(SAFE_DIVIDE(total_bytes_billed,
          POW(1024, 4)) * 6.25, 2) AS onDemandCost,
        ROUND(SAFE_DIVIDE(total_slot_ms,
            TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)), 2) AS approximateSlotCount,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND total_slot_ms IS NOT NULL
        AND state = "DONE"
      ORDER BY
        startTime DESC ),
  jobsDeduplicated AS (
      SELECT
        * EXCEPT(_rnk)
      FROM
        src
      WHERE
        _rnk = 1 )

SELECT
  *
FROM
  jobsDeduplicated;
END
