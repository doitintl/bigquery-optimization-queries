-- This query will return queries that cost the most based upon their billed byte count and the number of runs over
-- the specified timeframe.

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
    WITH src AS (
      SELECT
        query,
        job_id AS jobId,
        SHA256(query) AS hashed,
        COALESCE(total_bytes_billed, 0) AS totalBytesBilled,
        ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
      FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE
        creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
        AND CURRENT_TIMESTAMP()
        AND job_type = "QUERY"
        AND total_slot_ms IS NOT NULL
        AND state = "DONE" ),
    jobsDeduplicated AS (
      SELECT
        * EXCEPT(_rnk)
      FROM
        src
      WHERE
        _rnk = 1
    ),
    hashedQueries AS (
        SELECT
          query,
          hashed,
          SUM(totalBytesBilled) AS totalBytesBilled,
          COUNT(*) AS queryCount
        FROM
          jobsDeduplicated
        GROUP BY
          hashed, query
    )

SELECT
  query,
  queryCount,
  ROUND(COALESCE(totalBytesBilled,
    0), 2) AS totalBytesBilled,
  ROUND(COALESCE(totalBytesBilled,
    0) / POW(1024, 2), 2) AS totalMegabytesBilled,
  ROUND(COALESCE(totalBytesBilled,
    0) / POW(1024, 3), 2) AS totalGigabytesBilled,
  ROUND(COALESCE(totalBytesBilled,
    0) / POW(1024, 4), 2) AS totalTerabytesBilled,
  ROUND(SAFE_DIVIDE(totalBytesBilled,
        POW(1024, 4)) * 6.25, 2) AS onDemandCost
FROM
  hashedQueries
ORDER BY
  onDemandCost DESC;
END