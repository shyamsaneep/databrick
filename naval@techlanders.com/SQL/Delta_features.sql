-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.  History and Versioning with version option 
-- MAGIC 2.  Time Travel with timestamp option
-- MAGIC 3.  Vacuum

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TABLE new.demo (
-- MAGIC                               pk1 INT,
-- MAGIC                               pk2 STRING,
-- MAGIC                               dim1 INT,
-- MAGIC                               dim2 INT,
-- MAGIC                               dim3 INT,
-- MAGIC                               dim4 INT,
-- MAGIC                               active_status STRING,
-- MAGIC                               start_date TIMESTAMP,
-- MAGIC                               end_date TIMESTAMP
-- MAGIC                                  )
-- MAGIC                          USING DELTA 
-- MAGIC                          LOCATION '/mnt/shelladlstech/raw/schema/sample/demo'

-- COMMAND ----------

select * from new.demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import *

-- COMMAND ----------

use new

-- COMMAND ----------

insert into demo values (111,"unit_1", 10,20,30,40,"Y",current_timestamp(),'2020-1-1');
insert into demo values (222,"unit_2", 50,60,Null,45,"N",current_timestamp(),'2021-1-1');
insert into demo values (333,"unit_3", 15,25,35,65,"Y",current_timestamp(),'2022-1-1')

-- COMMAND ----------

select * from new.demo

-- COMMAND ----------

update new.demo
set dim1=100 where pk1=111

-- COMMAND ----------

select * from new.demo

-- COMMAND ----------

insert into demo values (444,"unit_4", 10,20,30,40,"Y",current_timestamp(),'2020-1-1');
insert into demo values (555,"unit_5", 50,60,Null,45,"N",current_timestamp(),'2021-1-1');
insert into demo values (666,"unit_6", 15,25,35,65,"Y",current_timestamp(),'2022-1-1')

-- COMMAND ----------

select * from new.demo

-- COMMAND ----------

update new.demo
set dim1=777 where pk1=555

-- COMMAND ----------

delete from demo where pk1= 333

-- COMMAND ----------

describe history new.demo

-- COMMAND ----------

select * from new.demo version as of 4

-- COMMAND ----------

select * from new.demo timestamp as of '2023-03-28T06:18:01.000+0000'

-- COMMAND ----------

select * from new.demo

-- COMMAND ----------

describe extended new.demo

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/shelladlstech/raw/schema/sample/demo/_delta_log/

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/shelladlstech/raw/schema/sample/demo/_delta_log/00000000000000000012.json

-- COMMAND ----------

describe history demo

-- COMMAND ----------

Vacuum new.demo

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

select * from new.demo

-- COMMAND ----------

select * from new.demo version as of 12

-- COMMAND ----------

delete from demo where pk1=111

-- COMMAND ----------

select * from demo

-- COMMAND ----------

describe history demo

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

select * from demo version as of 12

-- COMMAND ----------

