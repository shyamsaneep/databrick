# Databricks notebook source
# MAGIC %md
# MAGIC Step 1: Reading CSV file

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/shelladlstech/raw/demo/EmployeesTable.CSV")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestion_Date",current_timestamp())

# COMMAND ----------

df1.write.format("delta").mode("overwrite").option("path","dbfs:/mnt/shelladlstech/raw/jobs/first").saveAsTable("sample_emp_deepthi")

# COMMAND ----------

df1.display()

# COMMAND ----------

