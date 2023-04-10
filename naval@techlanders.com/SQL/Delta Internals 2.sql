-- Databricks notebook source
drop table employee_demo

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )

-- COMMAND ----------

INSERT INTO employee_demo values(100, "a", "M", 10000, "DE")

-- COMMAND ----------

INSERT INTO employee_demo values(200, "b", "F", 10000, "DE")

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000000.json

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000001.json

-- COMMAND ----------

dbfs:/user/hive/warehouse/employee_demo/part-00000-d05968a8-9db5-42f8-8ed0-024c4e8307d4-c000.snappy.parquet

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000002.json

-- COMMAND ----------

delete from employee_demo where emp_id =100

-- COMMAND ----------

INSERT INTO employee_demo values(100, "b", "F", 10000, "DE"),
(200, "c", "F", 10000, "DE"),
(300, "d", "M", 10000, "DE")

-- COMMAND ----------

describe extended employee_demo

-- COMMAND ----------

describe history employee_demo

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

delete from employee_demo where emp_id =300

-- COMMAND ----------

