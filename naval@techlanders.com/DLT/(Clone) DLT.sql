-- Databricks notebook source


-- COMMAND ----------

-- create streaming table from raw files

create or refresh streaming live table raw_table 
 tblproperties("quality" = "mapping")
 comment "The raw data from files"
 AS SELECT * FROM cloud_files("/mnt/shelladlstech/raw/streaminput/","csv", 
 map("header","true", "cloudFiles.inferColumnTypes","true", "cloudeFiles.schemaEvolutionMode", "rescue"));

-- COMMAND ----------

-- create enforced schema streaming table from raw streming table

create or refresh live table enforced_schema_table 
 tblproperties("quality" = "bronze")
 comment "The table with required schema"
 AS SELECT *
 FROM STREAM(Live.raw_table);

-- COMMAND ----------

-- create cleaned table with all the constraints 

CREATE OR REFRESH LIVE TABLE clean_table
(
CONSTRAINT valid_id EXPECT(Id IS NOT NULL) ON VIOLATION drop row
)
TBLPROPERTIES("quality"="silver")
COMMENT "cleaned table"
AS SELECT distinct(*) FROM(SELECT Id,Name,Salary,Country from (live.enforced_schema_table));

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE final_table
TBLPROPERTIES("quality" = "gold")
COMMENT "Aggregated table"
AS SELECT Country, sum(Salary) as Salary from live.clean_table group by Country;

-- COMMAND ----------

