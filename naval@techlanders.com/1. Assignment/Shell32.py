# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/task1",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="}
)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

shell32 = StructType([
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

df = spark.read.option("header",True).schema(shell32).csv('/mnt/task1/input/Superstore data.csv')
display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

df.write.save("dbfs:/mnt/task1/output/shell32_task1")

# COMMAND ----------

outputdf = df.select("*").groupby("segment").sum("sales")
display(outputdf)

# COMMAND ----------

