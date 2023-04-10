# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC - Step1: Load the Sample superstore data in ALDS Gen 2 in new container

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The data is loaded in shelladlstech/task/input

# COMMAND ----------

# MAGIC %md
# MAGIC - Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell40task",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell40task/input

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL) 

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

shell40Schema = StructType([
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

shell40df = spark.read.option("header", True).schema(shell40Schema).csv("/mnt/shell40task/input/Superstore data.csv")

# COMMAND ----------

display(shell40df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/output/%fs ls dbfs:/mnt/task/output/

# COMMAND ----------

shell40df.write.option("path","dbfs:/mnt/shell40task/output/shell40_task1/deltatable").saveAsTable("taskshell40")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taskshell40

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, sum(sales) from taskshell40 group by segment 

# COMMAND ----------

shell40df.write.save("dbfs:/mnt/shell40task/output/shell40_task1")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

outputdf = shell40df.select("*").groupby("segment").sum("sales")
display(outputdf)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

