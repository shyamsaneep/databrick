# Databricks notebook source
# MAGIC %md
# MAGIC Step-2: Mount ADLS container to databricks using Access Key

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell14",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell14/input

# COMMAND ----------

from pyspark.sql.types import *
#schema(users_schema)

# COMMAND ----------

Schema14 = StructType([
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

df14 = spark.read.option("header",True).schema(Schema14).csv("dbfs:/mnt/shell14/input/Superstore data.csv")

# COMMAND ----------

display(df14)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Read CSV file from container and convert into external delta table

# COMMAND ----------

#df14.write.save("dbfs:/mnt/shell14/output/shell14")
df14.write.option("path","dbfs:/mnt/shell14/output/shell14/deltatable").saveAsTable("taskshell14")

# COMMAND ----------

display(dbutils.fs.ls("abfss://task@shelladlstech.dfs.core.windows.net/input/Superstore data.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, sum(sales) from taskshell14 group by segment