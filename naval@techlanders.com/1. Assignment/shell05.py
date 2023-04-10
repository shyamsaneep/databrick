# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell05task",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell05task/output

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

dfsahi=spark.read.option("header",True).schema(s05Schema).csv("dbfs:/mnt/shell05task/input/Superstore data.csv")

# COMMAND ----------

dfsahi=spark.read.option("header",True).csv("dbfs:/mnt/shell05task/input/Superstore data.csv")

# COMMAND ----------

dfsahi.printSchema()

# COMMAND ----------

display(dfsahi)

# COMMAND ----------

s05Schema = StructType([
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

dfsahi.write.option("path","dbfs:/mnt/shell05task/output/shell05/assignmenttable").saveAsTable("shell05tabletask")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from shell05tabletask

# COMMAND ----------

# MAGIC %sql
# MAGIC select Segment, sum(Sales) from shell05tabletask group by Segment

# COMMAND ----------

outputdfsahi = dfsahi.select("*").groupby("segment").sum("sales")
display(outputdfsahi)

# COMMAND ----------

