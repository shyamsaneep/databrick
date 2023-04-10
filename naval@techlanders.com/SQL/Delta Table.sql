-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DeltaTable.createIfNotExists(spark)\
-- MAGIC     .tableName("emp_demo") \
-- MAGIC     .addColumn("emp_id", "INT") \
-- MAGIC     .addColumn("emp_name", "STRING") \
-- MAGIC     .addColumn("gender", "STRING") \
-- MAGIC     .addColumn("salary", "INT")\
-- MAGIC     .addColumn("department", "string")\
-- MAGIC     .property("description", "First Delta Table ")\
-- MAGIC     .execute()

-- COMMAND ----------

DESCRIBE TABLE EXTENDED emp_demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. SQL

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )using delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. using dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("path)
-- MAGIC df.write.option("overwrite").saveAsTable("Table")

-- COMMAND ----------

DeltaTable.create(spark)\
    .tableName("emp_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT")\
    .addColumn("department", "string")\
    .property("description", "First Delta Table ")\
    .location("/FileStore/tables/delta/createTable")\
    .execute()