# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell21task",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell21task/input

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

shell21sup = StructType([
    StructField("rowId", IntegerType()),
    StructField("orderId", StringType()),
    StructField("orderDate", StringType()),
    StructField("shipDate", StringType()),
    StructField("shipMode", StringType()),
    StructField("customerId", StringType()),
    StructField("customerName", StringType()),
    StructField("segment", StringType()),
    StructField("countryRegion", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("postalCode", IntegerType()),
    StructField("region", StringType()),
    StructField("productId", StringType()),
    StructField("category", StringType()),
    StructField("subCategory", StringType()),
    StructField("productName", StringType()),
    StructField("sales", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("discount", DoubleType()),
    StructField("profit", DoubleType())
])

# COMMAND ----------

shell21df = spark.read.option("header", True).schema(shell21sup).csv("/mnt/shell21task/input/Superstore data.csv")

# COMMAND ----------

display(shell21df)

# COMMAND ----------

shell21df.write.option("path","dbfs:/mnt/shell21task/output/shell21_task1/deltatable").saveAsTable("shell21table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from shell21table

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, sum(sales) from shell21table group by segment 

# COMMAND ----------

shell21df.write.save("dbfs:/mnt/output/shell21_task1")