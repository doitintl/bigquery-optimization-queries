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

-- Do not modify these two lines
DECLARE location_query_template STRING DEFAULT "SET @@location = '<dataset-region>';\n";
DECLARE physical_storage_query_template STRING DEFAULT "ALTER SCHEMA `<dataset>` SET OPTIONS(storage_billing_model = 'PHYSICAL')";
DECLARE logical_storage_query_template STRING DEFAULT "ALTER SCHEMA `<dataset>` SET OPTIONS(storage_billing_model = 'LOGICAL')";

WITH storage AS
(
  SELECT DISTINCT
    tb.table_schema,
    tb.table_name,
    CONCAT(tb.PROJECT_ID, '.', tb.table_schema) AS dataset,
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
    -- End user: Change to reflect your project and region
    `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLE_STORAGE AS tb

    -- Need to join on TABLES for existing tables to remove any temporary or job result tables
    -- Note due to this information being in the TABLE_STORAGE view this means it cannot be
    -- performed across an entire organization without checking the TABLES view in each project.
  -- End user: Change to reflect your project and region
  JOIN `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLES AS t
    ON t.table_catalog = tb.project_id
      AND t.table_name = tb.table_name
  WHERE
    tb.deleted = false
    -- Only look at the BASE TABLE type, as this is what Google uses in their billing data to
    -- bill on even if there are clones, snapshots, etc.
    AND t.table_type = 'BASE TABLE'
),
schemata_options AS
(
  /*
  * Extract the storage billing model
  * Note that if it's not listed then it's logical, or if it was converted before ~August 26, 2023
  * then it might be physical, but Google did not backfill the schemata view showing the change.
  */
  SELECT
    schema_name,
    option_value
  FROM
    -- End user: Change to reflect your project and region
    `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
  WHERE
    option_name = 'storage_billing_model'
),
storage_and_dataset_billing_type AS
(
  -- Gets project name and dataset name
  SELECT
    S.* EXCEPT(table_schema), -- Excluding table_schema to make grouping easier below and to remove redundant data
    COALESCE(SO.option_value, 'LOGICAL') AS current_storage_model
  FROM
    storage AS S
    LEFT OUTER JOIN schemata_options AS SO
      ON S.table_schema = SO.schema_name
),
calculations AS
(
  SELECT
    dataset,
    current_storage_model,
    SUM(active_uncompressed_price) AS active_uncompressed_price,
    SUM(active_compressed_price) AS active_compressed_price,
    SUM(long_term_uncompressed_price) AS long_term_uncompressed_price,
    SUM(long_term_compressed_price) AS long_term_compressed_price,
    SUM(time_travel_compressed_price) AS time_travel_compressed_price,
    SUM(fail_safe_compressed_price) AS fail_safe_compressed_price
  FROM
    storage_and_dataset_billing_type
  GROUP BY
    dataset, current_storage_model
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
    ) AS difference,

    current_storage_model,

    active_uncompressed_price,
    active_compressed_price,
    long_term_uncompressed_price,
    long_term_compressed_price
  FROM
    calculations
)

SELECT
    dataset,

    -- Logical prices and base (before adding in time travel and fail-safe reductions) physical price
    CONCAT('$ ',FORMAT("%'.2f", active_uncompressed_price)) AS logical_active_price,
    --CONCAT('$ ',FORMAT("%'.2f", active_compressed_price)) AS base_physical_price,
    CONCAT('$ ',FORMAT("%'.2f", long_term_uncompressed_price)) AS logical_long_term_price,
    /* renamed both physical storage types to match the logical naming conventions and grouped them together */
    CONCAT('$ ',FORMAT("%'.2f", active_compressed_price)) AS physical_active_price,
    --CONCAT('$ ',FORMAT("%'.2f", long_term_compressed_price)) AS base_long_term_price,
    CONCAT('$ ',FORMAT("%'.2f", long_term_compressed_price)) AS physical_long_term_price,

    -- Time travel and fail safe storage reductions
    CONCAT('$ ',FORMAT("%'.2f", additional_costs_for_physical_storage)) AS additional_costs_for_physical_storage,

    -- Totals for each model
    -- Note physical_storage_price is the total price with the time-travel and fail-safe reductions factored in
    CONCAT('$ ',FORMAT("%'.2f", logical_storage_price)) AS logical_storage_price,
    CONCAT('$ ',FORMAT("%'.2f", physical_storage_price)) AS physical_storage_price,

    -- Difference between logical storage and physical storage (logical - active)
    -- Note that a negative value means logica/uncompressed is cheaper
    --CONCAT('$ ',FORMAT("%'.2f", difference)) AS difference_in_price_if_physical_is_chosen,
    CONCAT('$ ',FORMAT("%'.2f", difference)) AS savings_in_price_if_physical_is_chosen,

    -- Saves or Costs More Money

    -- Current storage model
    /*
     *  Note this may be incorrect if you changed your billing model to physical prior to August 21, 2023.
     *  The reason is that Google didn't backfill the billing model to customers that were early adopters of the
     *  public version of physical storage.
     *
     *  Bugtracker to see this: https://issuetracker.google.com/issues/296794707
    */
    current_storage_model,

    -- Recommendation for what to do
    /*
     *  Writing this in SQL makes it more complex than it is, but it's relatively easy.
     *  If currrently on logical and physical storage is cheaper than recommend changing to physical storage. Otherwise recommend to not change.
     *  If currrently on physical and logical storage is cheaper than recommend changing to back to logical storage. Otherwise recommend to not change.
     */
    IF(
      current_storage_model = 'LOGICAL',
      -- Is on logical storage currently
      IF(
        logical_storage_price < physical_storage_price,
        'Do nothing as logical storage is the best option (currently logical).',
        'Change dataset to physical storage for additional savings.'
      ),
        -- Is on physical storage currently
      IF(logical_storage_price < physical_storage_price,
         'Dataset is currently using physical storage and costing you more money than logical storage. Change dataset back to logical storage.',
         'Do nothing as physical storage is the best option (currently physical).')
    ) AS recommendation,
    
    -- Query to run
    /*
     *  This looks complex again due to SQL, but uses same logic as above statement but emits SQL to make the change.
     */
    IF(
      current_storage_model = 'LOGICAL',
      -- Is on logical storage currently
      IF(
        logical_storage_price < physical_storage_price,
        -- Do nothing
        NULL,
        CONCAT(
          -- Add in the location
          REPLACE(location_query_template, 'region-', ''),
          -- Use the change to physical storage query
          REPLACE(physical_storage_query_template, '<dataset>', dataset))
      ),
      -- Is on physical storage currently
      IF(
        logical_storage_price < physical_storage_price,
        CONCAT(
          -- Add in the location
          REPLACE(location_query_template, 'region-', ''),
          -- Use the change to logical storage query
          REPLACE(logical_storage_query_template, '<dataset>', dataset))
        ,
         -- Do nothing
         NULL
        )
    ) AS recommendation_change_SQL
    
    -- If you wish to get the raw values that are not formatted uncomment the below line
    --final_data.* EXCEPT(dataset)
  FROM
    final_data
;
