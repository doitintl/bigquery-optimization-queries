/*
 *  This query retrieves the table(s) each job references and joins it to that table's storage size along with storing the last queried time during the range.
 *  It then calculates the price for that table and displays it along with the storage data.

 *  Instructions for use:
 *  1. Modify the <project-name> and <dataset-region> placeholders below to match your required values. 
 *  2. Change the interval_in_days value to travel back further in time. By default this is 14 days.

 *  Note: If not using the US or EU multi-regions, then the storage costs may be different.
 *    Change the storage price values below to match the actual cost listed here:
 *    https://cloud.google.com/bigquery/pricing?hl=en#storage-pricing
 */

-- Change this value to change how far in the past the query will search
DECLARE interval_in_days INT64 DEFAULT 14;

-- Update this with the correct values for your region (default is for US multi-region)
-- EU multi-region values are below.
-- This is for price per gib monhtly (not hourly)
-- Can be found here: https://cloud.google.com/bigquery/pricing?hl=en#storage-pricing
DECLARE active_logical_price NUMERIC DEFAULT 0.02;
DECLARE long_term_logical_price NUMERIC DEFAULT 0.01;
DECLARE active_physical_price NUMERIC DEFAULT 0.04;
DECLARE long_term_physical_price NUMERIC DEFAULT 0.02;

-- These values are for the EU multi-region
-- Comment these out and uncomment above if using the US multi-region
/*
DECLARE active_logical_price NUMERIC DEFAULT 0.02;
DECLARE long_term_logical_price NUMERIC DEFAULT 0.01;
DECLARE active_physical_price NUMERIC DEFAULT 0.044;
DECLARE long_term_physical_price NUMERIC DEFAULT 0.022;
*/

WITH tables AS (
    -- Basic table data and last query usage
    SELECT
        -- Create the fully qualified table name for joining
        t_ref.project_id,
        t_ref.dataset_id,
        t_ref.table_id,
        
        -- Using creation time as the last queried time since this is the partitioned column in the JOBS view
        MAX(j.creation_time) AS last_queried_time
    FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT AS j,
        -- Extract the referenced tables
        UNNEST(j.referenced_tables) AS t_ref
    WHERE
        -- Only grab successful query jobs
        j.job_type = 'QUERY'
        AND j.state = 'DONE'

        -- Filter on the given range
        AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
          AND CURRENT_TIMESTAMP()
    GROUP BY 1, 2, 3
),
table_billing_type AS (
    -- Table billing type
    SELECT
        ts.project_id,
        ts.table_schema AS dataset_id,
        ts.table_name AS table_id,
        
        -- Get the billing model option. Defaults to LOGICAL if the option is not explicitly set.
        IFNULL(topt.option_value, 'LOGICAL') AS storage_billing_model,
        
        -- Conditionally determine the Total Billed Bytes
        CASE
            -- Use PHYSICAL bytes if the option is explicitly set to PHYSICAL
            WHEN topt.option_value = 'PHYSICAL' THEN ts.total_physical_bytes
            -- Otherwise (LOGICAL or NULL/default), use LOGICAL bytes
            ELSE ts.total_logical_bytes 
        END AS total_billed_bytes,

        -- Conditionally determine the Active Billed Bytes
        CASE
            WHEN topt.option_value = 'PHYSICAL' THEN ts.active_physical_bytes
            ELSE ts.active_logical_bytes 
        END AS active_billed_bytes,

        -- Conditionally determine the Long-Term Billed Bytes
        CASE
            WHEN topt.option_value = 'PHYSICAL' THEN ts.long_term_physical_bytes
            ELSE ts.long_term_logical_bytes 
        END AS long_term_billed_bytes
        
    FROM
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLE_STORAGE AS ts
    INNER JOIN
        -- Join with TABLES view to get the original creation time
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLES AS t
        ON ts.project_id = t.table_catalog
        AND ts.table_schema = t.table_schema
        AND ts.table_name = t.table_name
    LEFT JOIN
        -- Left join TABLE_OPTIONS to find the storage billing model (it might be NULL if the default is used or it was converted in July 2023 before this option was added)
        `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLE_OPTIONS AS topt
        ON ts.project_id = topt.table_catalog
        AND ts.table_schema = topt.table_schema
        AND ts.table_name = topt.table_name
        AND topt.option_name = 'storage_billing_model'
)

SELECT
    m.project_id,
    m.dataset_id,
    m.table_id,
    -- Build a nice message instead of throwing a NULL value in for last queried time
    IF(u.last_queried_time IS NULL, CONCAT('Prior to ', STRING(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY))), STRING(u.last_queried_time)) AS last_queried_time,
    m.storage_billing_model,
    
    -- Total storage price
    CONCAT('$ ',
      FORMAT("%'.2f",
        -- Adds active + long-term price (accounting for physical or logical)
        -- Calculate active price
        CASE
          WHEN m.storage_billing_model = 'LOGICAL' THEN
            (m.active_billed_bytes/POW(1024, 3)) * active_logical_price
          ELSE
            (m.active_billed_bytes/POW(1024, 3)) * active_physical_price
        END
        +
        -- Calculate long-term price
        CASE
          WHEN m.storage_billing_model = 'LOGICAL' THEN
            (m.long_term_billed_bytes/POW(1024, 3)) * long_term_logical_price
          ELSE
            (m.long_term_billed_bytes/POW(1024, 3)) * long_term_physical_price
        END
      )
    ) AS table_storage_price,

    -- Total Billed Size
    FORMAT('%.2f %s', 
        m.total_billed_bytes / POW(1024, 
            CASE
                WHEN m.total_billed_bytes >= POW(1024, 4) THEN 4 -- TiB
                WHEN m.total_billed_bytes >= POW(1024, 3) THEN 3 -- GiB
                WHEN m.total_billed_bytes >= POW(1024, 2) THEN 2 -- MiB
                ELSE 1 -- KB
            END
        ),
        CASE
            WHEN m.total_billed_bytes >= POW(1024, 4) THEN 'TiB'
            WHEN m.total_billed_bytes >= POW(1024, 3) THEN 'GiB'
            WHEN m.total_billed_bytes >= POW(1024, 2) THEN 'MiB'
            ELSE 'KB'
        END
    ) AS readable_total_billed_size,
    m.total_billed_bytes,

    -- Active Size
    FORMAT('%.2f %s', 
        m.active_billed_bytes / POW(1024, 
            CASE
                WHEN m.active_billed_bytes >= POW(1024, 4) THEN 4 
                WHEN m.active_billed_bytes >= POW(1024, 3) THEN 3 
                WHEN m.active_billed_bytes >= POW(1024, 2) THEN 2 
                ELSE 1
            END
        ),
        CASE
            WHEN m.active_billed_bytes >= POW(1024, 4) THEN 'TiB'
            WHEN m.active_billed_bytes >= POW(1024, 3) THEN 'GiB'
            WHEN m.active_billed_bytes >= POW(1024, 2) THEN 'MiB'
            ELSE 'KB'
        END
    ) AS readable_active_size,
    m.active_billed_bytes,

    -- Long-Term Size
    FORMAT('%.2f %s', 
        m.long_term_billed_bytes / POW(1024, 
            CASE
                WHEN m.long_term_billed_bytes >= POW(1024, 4) THEN 4 
                WHEN m.long_term_billed_bytes >= POW(1024, 3) THEN 3 
                WHEN m.long_term_billed_bytes >= POW(1024, 2) THEN 2 
                ELSE 1
            END
        ),
        CASE
            WHEN m.long_term_billed_bytes >= POW(1024, 4) THEN 'TiB'
            WHEN m.long_term_billed_bytes >= POW(1024, 3) THEN 'GiB'
            WHEN m.long_term_billed_bytes >= POW(1024, 2) THEN 'MiB'
            ELSE 'KB'
        END
    ) AS readable_long_term_size
FROM
    table_billing_type AS m
LEFT JOIN
    tables AS u 
      ON m.project_id = u.project_id
        AND m.dataset_id = u.dataset_id
        AND m.table_id = u.table_id
ORDER BY
    -- Sort by last queried time (oldest first, with NULLs first)
    u.last_queried_time ASC NULLS FIRST,
    -- Secondary sort by the determined billed size descending
    total_billed_bytes DESC;