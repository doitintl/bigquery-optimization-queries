  -- This query will count the amount of times a specific query is run over the specified timeframe.

  -- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

BEGIN
WITH
  src AS (
    SELECT
      query,
      SHA256(query) AS hashed,
      total_bytes_billed AS totalBytesBilled,
      ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY end_time DESC) AS _rnk
    FROM
      `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE
      creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
      AND CURRENT_TIMESTAMP()
      AND job_type = "QUERY"
      AND total_slot_ms IS NOT NULL
      AND state = "DONE"),
  jobsDeduplicated AS (
      SELECT
        * EXCEPT(_rnk)
      FROM
        src
      WHERE
        _rnk = 1 ),
  hashedQueries AS (
    SELECT
      src1.query AS query,
      src1.hashed AS hashed
    FROM
      jobsDeduplicated AS src1,
      jobsDeduplicated AS src2
    WHERE
      src1.hashed = src2.hashed ),
  pricedQueries AS (
    SELECT
      hashed,
      SUM(totalBytesBilled) AS totalBytesBilled
    FROM
      jobsDeduplicated
    GROUP BY
      hashed ),
  countQueries AS (
    SELECT
      query,
      hashed,
      COUNT(hashed) AS queryCount
    FROM
      hashedQueries
    GROUP BY
      hashed,
      query ),
  countedAndPricedQueries AS (
    SELECT
      query,
      totalBytesBilled,
      queryCount
    FROM
      countQueries
    JOIN
      pricedQueries
    ON
      countQueries.hashed = pricedQueries.hashed )

SELECT
  query,
  queryCount,
  ROUND(SAFE_DIVIDE(totalBytesBilled, 102400000000) * 5, 2) AS onDemandCost,
  ROUND(COALESCE(totalBytesBilled, 0), 2) AS totalBytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / 1000000, 2) AS totalMegabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / 1000000000, 2) AS totalGigabytesBilled,
  ROUND(COALESCE(totalBytesBilled, 0) / 1000000000000, 2) AS totalTerabytesBilled
FROM
  countedAndPricedQueries
ORDER BY
  queryCount DESC;
END