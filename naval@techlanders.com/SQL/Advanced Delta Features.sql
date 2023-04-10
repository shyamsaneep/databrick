-- Databricks notebook source
-- MAGIC %md
-- MAGIC 0. External and Managed Table
-- MAGIC 1. Time Travel
-- MAGIC 2. History of table
-- MAGIC 3. Optimize
-- MAGIC 4. Vacuum 
-- MAGIC 5. Zordering 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )using delta
-- MAGIC  LOCATION '/mnt/shelladlstech/raw/schema/deltafeature/employee'

-- COMMAND ----------

describe extended employee

-- COMMAND ----------

INSERT INTO employee values(400, "d", "F", 20000, "DS");
INSERT INTO employee values(500, "e", "M", 22000, "DE");
INSERT INTO employee values(600, "f", "M", 25000, "DA")

-- COMMAND ----------

UPDATE employee
set salary = salary+1111
where emp_id=400

-- COMMAND ----------

select * from employee

-- COMMAND ----------

delete from employee where emp_id= 600

-- COMMAND ----------

describe history employee

-- COMMAND ----------

select * from employee version as of 6

-- COMMAND ----------

select * from employee@v1

-- COMMAND ----------

delete from employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

describe history employee

-- COMMAND ----------

RESTORE table employee to version as of 8

-- COMMAND ----------

select * from employee

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

describe history employee

-- COMMAND ----------

optimize employee

-- COMMAND ----------

describe history employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

delete from employee where emp_id= 400

-- COMMAND ----------

select * from employee

-- COMMAND ----------

describe history employee

-- COMMAND ----------

restore table employee to version as of 13

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- MAGIC %fs ls '/mnt/shelladlstech/raw/schema/deltafeature/employee'

-- COMMAND ----------

Vacuum employee

-- COMMAND ----------

-- MAGIC %fs ls '/mnt/shelladlstech/raw/schema/deltafeature/employee'

-- COMMAND ----------

vacuum employee retain 240 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false 

-- COMMAND ----------

vacuum employee retain 0 hours

-- COMMAND ----------

vacuum employee retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls '/mnt/shelladlstech/raw/schema/deltafeature/employee'

-- COMMAND ----------

select * from employee version as of 10

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/shelladlstech/raw/schema/deltafeature/employee/_delta_log/'

-- COMMAND ----------

optimize employee 
zorder by emp_id

-- COMMAND ----------

select * from employee

-- COMMAND ----------

