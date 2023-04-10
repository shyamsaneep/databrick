-- Databricks notebook source
drop table raw

-- COMMAND ----------

CREATE TABLE raw AS SELECT * FROM csv.`/mnt/shelladlstech/raw/streaminput/` option(header true)

-- COMMAND ----------

CREATE TABLE demo
(Id int,
name String,
Gender String,
Salary int,
Country string,
Date date
)
using csv
options( path "/mnt/shelladlstech/raw/streaminput/Jan.csv", header true)


-- COMMAND ----------

select * from demo 

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from raw

-- COMMAND ----------

create or refresh live table raw_table 
 tblproperties("quality" = "mapping")
 comment "The raw data from files"
 AS SELECT * FROM csv.`/mnt/shelladlstech/raw/streaminput/`  options("header"="true")

-- COMMAND ----------

create or refresh live table enforced_schema_table 
 tblproperties("quality" = "bronze")
 comment "The table with required schema"
 AS SELECT *,current_timestamp() as current_time
 FROM Live.raw_table;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE clean_table
TBLPROPERTIES("quality"="silver")
COMMENT "cleaned table"
AS SELECT distinct(*) FROM(SELECT Id,Name,Salary,Country from (live.enforced_schema_table));

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE final_table
TBLPROPERTIES("quality" = "gold")
COMMENT "Aggregated table"
AS SELECT Country, sum(Salary) as Salary from live.clean_table group by Country;