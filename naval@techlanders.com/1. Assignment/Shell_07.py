# Databricks notebook source

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell07",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell07/input

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/output/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/task/input

# COMMAND ----------

storage_account_name = 
client_id            = 
tenant_id            = 
client_secret        = 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/shell07/input/Superstore data.csv")


# COMMAND ----------

shell07Schema = StructType([
    StructField("RowId", IntegerType()),
    StructField("OrderId", StringType()),
    StructField("OrderDate", StringType()),
    StructField("ShipDate", StringType()),
    StructField("ShipMode", StringType()),
    StructField("CustomerId", StringType()),
    StructField("CustomerName", StringType()),
    StructField("Segment", StringType()),
    StructField("CountryRegion", StringType()),
    StructField("City", StringType()),
    StructField("State", StringType()),
    StructField("PostalCode", IntegerType()),
    StructField("Region", StringType()),
    StructField("ProductId", StringType()),
    StructField("Category", StringType()),
    StructField("SubCategory", StringType()),
    StructField("ProductName", StringType()),
    StructField("Sales", DoubleType()),
    StructField("Quantity", IntegerType()),
    StructField("Discount", DoubleType()),
    StructField("Profit", DoubleType())
])


# COMMAND ----------

shell07df = spark.read.option("header", True).schema(shell07Schema).csv("/mnt/shell07/input/Superstore data.csv")

# COMMAND ----------

display(shell07df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/output/

# COMMAND ----------

shell07df.write.option("path","dbfs:/mnt/shell07/output/shell07/deltatable").saveAsTable("shell07_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from shell07_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, sum(sales) from shell07_data group by segment 

# COMMAND ----------

shell07df.write.save("dbfs:/mnt/shell07/output/shell07")


# COMMAND ----------

outputdf = shell07df.select("*").groupby("segment").sum("sales")
display(outputdf)

# COMMAND ----------

