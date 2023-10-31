/*
Query that calculates both logical and physical storage billing costs
along with compression ratio (>2.0 means cost savings in physical
billing model).  Does not take into account 10gb free storage per month
given that is weighted equally between logical/physical so it cancels
out when it comes to considering which provides the most savings.

All numbers are grouped by dataset and considered estimates only.
*/
DECLARE active_logical_gb_price FLOAT64 DEFAULT 0.023;
DECLARE long_term_logical_gb_price FLOAT64 DEFAULT 0.016;
DECLARE active_physical_gb_price FLOAT64 DEFAULT 0.04;
DECLARE long_term_physical_gb_price FLOAT64 DEFAULT 0.02;

with storage_sizes as
(
  select
    a.project_id
    ,a.table_schema
    ,SUM(a.active_logical_bytes) / power(1000, 3) AS active_logical_gb
    ,SUM(a.long_term_logical_bytes) / power(1000, 3) AS long_term_logical_gb
    ,SUM(a.active_physical_bytes) / power(1000, 3) AS active_physical_gb
    ,SUM(a.long_term_physical_bytes) / power(1000, 3) AS long_term_physical_gb
    ,SUM(a.total_physical_bytes) / power(1000, 3) AS total_physical_gb
    ,SUM(a.total_logical_bytes) / power(1000, 3) AS total_logical_gb
  from
  `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT a
  join `<project-name>`.`<dataset-region>`.INFORMATION_SCHEMA.TABLES b
  on a.project_id = b.table_catalog
  and a.table_schema = b.table_schema
  and a.table_name = b.table_name
  WHERE a.total_logical_bytes > 0
  AND a.total_physical_bytes > 0
  GROUP BY a.project_id, a.table_schema
)
,cost_calcs as
(
  select
    project_id
    ,table_schema
    ,active_logical_gb
    ,active_logical_gb*active_logical_gb_price as active_logical_cost
    ,long_term_logical_gb
    ,long_term_logical_gb*long_term_logical_gb_price as long_term_logical_cost
    ,active_physical_gb
    ,active_physical_gb*active_physical_gb_price as active_physical_cost
    ,long_term_physical_gb
    ,long_term_physical_gb*long_term_physical_gb_price as long_term_physical_cost
    ,total_logical_gb / total_physical_gb AS compression_ratio
  from storage_sizes
)
select
  project_id
  ,table_schema
  ,active_logical_gb
  ,long_term_logical_gb
  ,(active_logical_cost+long_term_logical_cost) as total_logical_cost
  ,active_physical_gb
  ,long_term_physical_gb
  ,(active_physical_cost+long_term_physical_cost) as total_physical_cost
  ,compression_ratio
from
cost_calcs;
