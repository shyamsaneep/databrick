-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo_ext (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )
-- MAGIC options(path "/mnt/shelladlstech/raw/delta/employe_demo")

-- COMMAND ----------

describe extended employee_demo_ext

-- COMMAND ----------

INSERT INTO employee_demo_ext values(100,"a","M",1000,"DE")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo_ext_par (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )
-- MAGIC using parquet
-- MAGIC options(path "/mnt/shelladlstech/raw/part/employee")

-- COMMAND ----------

INSERT INTO employee_demo_ext_par values(100,"a","M",1000,"DE")

-- COMMAND ----------

CREATE DATABASE new 
location "/mnt/shelladlstech/raw/schema/sample"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS new.employee_demo (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )
-- MAGIC options(path "/mnt/shelladlstech/raw/schema/sample/employee_demo")

-- COMMAND ----------

drop table new.employee_demo

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS new.employee_demo_sample (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo_sample1 (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )
-- MAGIC options (path "dbfs:/user/hive/warehouse/demo")

-- COMMAND ----------

describe extended employee_demo_sample2

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo_sample2 (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )
-- MAGIC options (path "/FileStore/tables/demo")

-- COMMAND ----------



-- COMMAND ----------

describe extended new.employee_demo_sample

-- COMMAND ----------

use new;
show tables

-- COMMAND ----------

drop table employee_demo_sample

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS new.employee_demo_test (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )
-- MAGIC options(path "/mnt/shelladlstech/raw/delta/new")

-- COMMAND ----------

describe extended new.employee_demo_test

-- COMMAND ----------

select * from new.employee_demo_test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").option("path", "/mnt/adls").saveAsTable("demo")

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended employee_demo

-- COMMAND ----------

drop table employee_demo

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended employee_demo_ext

-- COMMAND ----------

drop table employee_demo_ext

-- COMMAND ----------

show tables

-- COMMAND ----------

