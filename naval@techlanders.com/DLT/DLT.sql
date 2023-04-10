-- Databricks notebook source
-- create streaming table from raw files

create or refresh streaming live table raw_table 
 tblproperties("quality" = "mapping")
 comment "The raw data from files"
 AS SELECT * FROM cloud_files("/mnt/shelladlstech/raw/DeltaLive/","csv", map("header","true","cloudFiles.inferColumnTypes","false","CloudeFiles.schemaEvolution", "rescue"));

-- COMMAND ----------

-- create enforced schema streaming table from raw streming table

create or refresh streaming live table enforced_schema_table 
 tblproperties("quality" = "bronze")
 comment "The table with required schema"
 AS SELECT
 cast(order_id as int), cast(order_date as date), cast(region as string), cast(iteam as string), cast(cost as double), _rescued_data as error 
 FROM stream (Live.raw_table);

-- COMMAND ----------

-- create cleaned table with all the constraints 

CREATE OR REFRESH LIVE TABLE clean_table
(
CONSTRAINT valid_order_id EXPECT(order_id IS NOT NULL) ON VIOLATION drop row,
CONSTRAINT valid_cost EXPECT (cost IS NOT NULL) ON VIOLATION drop row
)
TBLPROPERTIES("quality"="silver")
COMMENT "cleaned table"
AS SELECT distinct(*) FROM(SELECT order_id, order_date, region, iteam, cost from (live.enforced_schema_table));

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE duplicate_orders
COMMENT "duplicate records"
AS SELECT order_id, order_date, region, iteam, cost, count (*) as no_of_repetition FROM (live.enforced_schema_table) group by order_id, order_date, region, iteam, cost having count (*) > 1;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE invalid_orders
COMMENT "table with invalid records"
AS SELECT * FROM (live.enforced_schema_table) where order_id is null or cost is null;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE final_table
TBLPROPERTIES("quality" = "gold")
COMMENT "Aggregated table"
AS SELECT region, sum(cost) as total_sale from live.clean_table group by region;

-- COMMAND ----------

