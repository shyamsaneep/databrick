# Databricks notebook source
# MAGIC %md
# MAGIC Step1: Load the Sample superstore data in ALDS Gen 2 in new container
# MAGIC Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)
# MAGIC Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL)
# MAGIC Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/task/shell33",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":
                   "H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="}
)


# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df_superstr = StructType([
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

df_superstr1 = spark.read.option("header",True).schema(df_superstr).csv("/mnt/task/shell33/input/Superstore data.csv")



# COMMAND ----------

df_superstr1.write.option("path","dbfs:/mnt/task/shell33/output/shell33/deltatable").saveAsTable("shell33_superstore")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from shell33_superstore