# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://task@shelladlstech.blob.core.windows.net",
    mount_point = "/mnt/assgt/task",
    extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

https://shelladlstech.blob.core.windows.net/task?sp=r&st=2023-04-05T04:52:09Z&se=2023-04-05T12:52:09Z&spr=https&sv=2021-12-02&sr=c&sig=dbGiog9zJTKcHjfZ1%2B6vF0k7kpMom3OUGMHNcyWjnQY%3D

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/assgt/task

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/assgt/task/input/Superstore data.csv")
display(df)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

shell23Schema = StructType([
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

df = spark.read.option("header", True).schema(shell23Schema).csv("/mnt/assgt/task/input/Superstore data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/output/%fs ls dbfs:/mnt/task/output/

# COMMAND ----------

df.write.parquet("dbfs:/mnt/assgt/task/output/shell23")