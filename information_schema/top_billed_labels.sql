-- This query will pull all jobs with labels and then sort them by cost descending

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
    WITH src AS (
      SELECT
        job_id AS jobId,
        labels.key AS labelKey,
        labels.value AS labelValue,
        total_bytes_billed AS totalBilledBytes,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        -- Cross join the labels to the table
        CROSS JOIN UNNEST(labels) AS labels
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND total_slot_ms IS NOT NULL
        AND state = "DONE"
        -- Filter out jobs without labels
        AND labels.key IS NOT NULL
        AND labels.value IS NOT NULL 
        ),
  jobsDeduplicated AS (
    SELECT
      * EXCEPT(_rnk)
    FROM
      src
    WHERE
      _rnk = 1)

    SELECT
        SUM(COALESCE(totalBilledBytes, 0)) AS totalBilledBytes,
        ROUND(SUM(COALESCE(totalBilledBytes,
              0)) / POW(1024, 2), 2) AS totalMegabytesBilled,
        ROUND(SUM(COALESCE(totalBilledBytes,
              0)) / POW(1024, 3), 2) AS totalGigabytesBilled,
        ROUND(SUM(COALESCE(totalBilledBytes,
              0)) / POW(1024, 4), 2) AS totalTerabytesBilled,
        ROUND(SAFE_DIVIDE(SUM(COALESCE(totalBilledBytes, 0)),
          POW(1024, 4)) * 6.25, 2) AS onDemandCost,
        labelKey,
        labelValue
    FROM jobsDeduplicated
    GROUP BY
      labelKey,
      labelValue
    ORDER BY
      onDemandCost DESC;
END