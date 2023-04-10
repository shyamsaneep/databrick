-- Databricks notebook source
-- MAGIC %fs ls 'dbfs:/mnt/shelladlstech/raw/output/partition/year&gender/Year=2007/'

-- COMMAND ----------

CONVERT TO DELTA default.constructor_parCONVERT TO DELTA default.constructor_par

-- COMMAND ----------

CONVERT TO DELTA default.constructor_parquet

-- COMMAND ----------

CONVERT TO DELTA default.constructor_parq_deepthi

-- COMMAND ----------

CONVERT TO DELTA parquet.`/mnt/shelladlstech/raw/output/contructor`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("dbfs:/mnt/shelladlstech/raw/output/partition/year&gender/Year=2007/*")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=df.withColumnRenamed("First Name","firstName")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").partitionBy("County","firstName").parquet("dbfs:/mnt/shelladlstech/raw/output/partition/county")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("/mnt/shelladlstech/raw/output/partition/year&maxfiles/Year=2014/")
-- MAGIC display(df)

-- COMMAND ----------

CONVERT TO DELTA parquet.`/mnt/shelladlstech/raw/output/partition/year&maxfiles/Year=2014/`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("/mnt/shelladlstech/raw/output/partition/")
-- MAGIC display(df)

-- COMMAND ----------

CONVERT TO DELTA parquet.`dbfs:/mnt/shelladlstech/raw/output/partition/county/`

-- COMMAND ----------

