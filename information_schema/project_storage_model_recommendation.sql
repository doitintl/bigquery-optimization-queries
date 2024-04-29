/*
 *  This query will look at a single project (by default in US multi-region) and
 *  calculate the logical and physical billing prices for each dataset inside of it
 *  then provide a recommendation on whether to keep it on logical storage
 *  or switch to the physical billing model.
 *
 *  Physical (also called compressed) Storage will be GA and released for public
 *  consumption on July 5, 2023.
 *
 *  It also includes inside of the storage CTE lots of extra values that can be used
 *  for other calculations that are being left in here to assist you as the customer
 *  make the best decision or to see additional information about your tables/datasets.
 *
 *  Note it targets the US multi-region by default. If needing to change the region
 *  then change `region-us` below to whichever region the data exists in. Also uncomment
 *  the DECLARE values below for the EU region or if you are using a non-multi-region
 *  then refer here: https://cloud.google.com/bigquery/pricing#storage
 *  for the correct pricing and update accordingly.
 */

-- These values are for the US multi-region
-- Comment these out and uncomment below if using the EU multi-region
DECLARE active_logical_price_per_gb NUMERIC DEFAULT 0.02;
DECLARE long_term_logical_price_per_gb NUMERIC DEFAULT 0.01;
DECLARE active_physical_price_per_gb NUMERIC DEFAULT 0.04;
DECLARE long_term_physical_price_per_gb NUMERIC DEFAULT 0.02;

-- These values are for the EU multi-region
-- Comment these out and uncomment above if using the US multi-region
/*
DECLARE active_logical_price_per_gb NUMERIC DEFAULT 0.02;
DECLARE long_term_logical_price_per_gb NUMERIC DEFAULT 0.01;
DECLARE active_physical_price_per_gb NUMERIC DEFAULT 0.044;
DECLARE long_term_physical_price_per_gb NUMERIC DEFAULT 0.022;
*/

WITH storage AS
(
  SELECT DISTINCT
    tb.table_name,
    tb.table_schema AS dataset,
    total_rows,
    total_partitions,
    
    -- Uncompressed bytes
    total_logical_bytes AS total_uncompressed_bytes,
    total_logical_bytes/POW(1024, 3) AS total_logical_gibytes,
    total_logical_bytes/POW(1024, 4) AS total_logical_tibytes,
    active_logical_bytes AS active_uncompressed_bytes,
    active_logical_bytes/POW(1024, 3) AS active_uncompressed_gibytes,
    active_logical_bytes/POW(1024, 4) AS active_uncompressed_tibytes,
    long_term_logical_bytes AS long_term_uncompressed_bytes,
    long_term_logical_bytes/POW(1024, 3) AS long_term_uncompressed_gibytes,
    long_term_logical_bytes/POW(1024, 4) AS long_term_uncompressed_tibytes,

    -- Compressed bytes
    total_physical_bytes AS total_compressed_bytes,
    total_physical_bytes/POW(1024, 3) AS total_compressed_gibytes,
    total_physical_bytes/POW(1024, 4) AS total_compressed_tibytes,
    -- Note that active physical bytes includes time travel so need to remove that
    active_physical_bytes-time_travel_physical_bytes AS active_compressed_bytes,
    (active_physical_bytes-time_travel_physical_bytes)/POW(1024, 3) AS active_compressed_gibytes,
    (active_physical_bytes-time_travel_physical_bytes)/POW(1024, 4) AS active_compressed_tibytes,
    long_term_physical_bytes AS long_term_compressed_bytes,
    long_term_physical_bytes/POW(1024, 3) AS long_term_compressed_gibytes,
    long_term_physical_bytes/POW(1024, 4) AS long_term_compressed_tibytes,
    time_travel_physical_bytes AS time_travel_compressed_bytes,
    time_travel_physical_bytes/POW(1024, 3) AS time_travel_compressed_gibytes,
    time_travel_physical_bytes/POW(1024, 4) AS time_travel_compressed_tibytes,
    fail_safe_physical_bytes AS fail_safe_physical_bytes,
    fail_safe_physical_bytes/POW(1024, 3) AS fail_safe_compressed_gibytes,
    fail_safe_physical_bytes/POW(1024, 4) AS fail_safe_compressed_tibytes,
    
    -- Compression ratios
    SAFE_DIVIDE(total_logical_bytes, total_physical_bytes) AS total_compression_ratio,  -- Defined as uncompressed size/compressed size
    SAFE_DIVIDE(long_term_logical_bytes, long_term_physical_bytes) AS long_term_compression_ratio,
    SAFE_DIVIDE(active_logical_bytes, active_physical_bytes) AS active_compression_ratio,

    -- Pricing
    ((active_logical_bytes/POW(1024, 3))*active_logical_price_per_gb) +
      ((long_term_logical_bytes/POW(1024, 3))*long_term_logical_price_per_gb) AS total_uncompressed_price,
    ((active_logical_bytes/POW(1024, 3))*active_logical_price_per_gb) AS active_uncompressed_price,
    ((long_term_logical_bytes/POW(1024, 3))*long_term_logical_price_per_gb) AS long_term_uncompressed_price,
    (((active_physical_bytes-time_travel_physical_bytes)/POW(1024, 3))*active_physical_price_per_gb) +
      ((long_term_physical_bytes/POW(1024, 3))*long_term_physical_price_per_gb) AS total_compressed_price,
    (((active_physical_bytes-time_travel_physical_bytes)/POW(1024, 3))*active_physical_price_per_gb) AS active_compressed_price,
    (long_term_physical_bytes/POW(1024, 3))*long_term_physical_price_per_gb AS long_term_compressed_price,
    (time_travel_physical_bytes/POW(1024, 3))*active_physical_price_per_gb AS time_travel_compressed_price,
    (fail_safe_physical_bytes/POW(1024, 3))*active_physical_price_per_gb AS fail_safe_compressed_price
  FROM
    `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLE_STORAGE AS tb

    -- Need to join on TABLES for existing tables to remove any temporary or job result tables
    -- Note due to this information being in the TABLE_STORAGE view this means it cannot be
    -- performed across an entire organization without checking the TABLES view in each project.
  JOIN `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLES AS t
    ON t.table_catalog = tb.project_id
      AND t.table_name = tb.table_name
  WHERE
    tb.deleted = false
),
calculations AS
(
  SELECT
    dataset,
    SUM(active_uncompressed_price) AS active_uncompressed_price,
    SUM(active_compressed_price) AS active_compressed_price,
    SUM(long_term_uncompressed_price) AS long_term_uncompressed_price,
    SUM(long_term_compressed_price) AS long_term_compressed_price,
    SUM(time_travel_compressed_price) AS time_travel_compressed_price,
    SUM(fail_safe_compressed_price) AS fail_safe_compressed_price
  FROM
    storage
  GROUP BY
    dataset
),
final_data AS
(
  SELECT
    dataset,

    -- Price differences, note that >0 means physical storage is cheaper before adding in time travel and failsafe
    active_uncompressed_price-active_compressed_price AS active_price_difference,
    long_term_uncompressed_price-long_term_compressed_price AS long_term_price_difference,
    
    -- Time travel and fail safe storage reductions
    (time_travel_compressed_price+fail_safe_compressed_price) AS additional_costs_for_physical_storage,

    -- Totals for each model
    active_uncompressed_price+long_term_uncompressed_price AS logical_storage_price,
    (active_compressed_price+long_term_compressed_price)+
      (time_travel_compressed_price+fail_safe_compressed_price) AS physical_storage_price,
    
    -- Difference in values (logical - active)
    (active_uncompressed_price+long_term_uncompressed_price) -
    (
      (active_compressed_price+long_term_compressed_price)+
      (time_travel_compressed_price+fail_safe_compressed_price)
    ) AS difference
  FROM
    calculations
)

SELECT
    dataset,

    -- Logical prices and base (before adding in time travel and fail-safe reductions) physical price
    CONCAT('$ ',FORMAT("%'.2f", active_uncompressed_price)) AS logical_active_price,
    CONCAT('$ ',FORMAT("%'.2f", active_compressed_price)) AS base_physical_price,
    CONCAT('$ ',FORMAT("%'.2f", long_term_uncompressed_price)) AS logical_long_term_price,
    CONCAT('$ ',FORMAT("%'.2f", long_term_compressed_price)) AS base_long_term_price,

    -- Time travel and fail safe storage reductions
    CONCAT('$ ',FORMAT("%'.2f", additional_costs_for_physical_storage)) AS additional_costs_for_physical_storage,

    -- Totals for each model
    -- Note physical_storage_price is the total price with the time-travel and fail-safe reductions factored in
    CONCAT('$ ',FORMAT("%'.2f", logical_storage_price)) AS logical_storage_price,
    CONCAT('$ ',FORMAT("%'.2f", physical_storage_price)) AS physical_storage_price,

    -- Difference between logical storage and physical storage (logical - active)
    -- Note that a negative value means logica/uncompressed is cheaper
    CONCAT('$ ',FORMAT("%'.2f", difference)) AS difference_in_price_if_physical_is_chosen,

    -- Recommendation
    IF(logical_storage_price < physical_storage_price,
      'Keep dataset on logical storage', 'Change dataset to physical storage') AS recommendation,
    
    -- If you wish to get the raw values that are not formatted uncomment the below line
    --final_data.* EXCEPT(dataset)
  FROM
    final_data
;