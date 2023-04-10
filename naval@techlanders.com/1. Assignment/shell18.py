# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shell18adls.blob.core.windows.net",
  mount_point = "/mnt/shell18task",
  extra_configs = {"fs.azure.account.key.shell18adls.blob.core.windows.net":"g+PuZaikMu1wK5nqtP/rC/p/Hi/FbP/IN7tE8qzw+SUCm//5MScz9udCypcZlVqKxdoT7MSnNG8D+AStCSiiOw=="})

# COMMAND ----------

df_ssd = spark.read.option("header",True).option('inferSchema',True).csv("dbfs:/mnt/shell18task/input/Superstore data.csv")

# COMMAND ----------

display(df_ssd)

# COMMAND ----------

import pyspark.sql.functions as F

NewColumns=(column.replace(' ', '_').replace('/', '_') for column in df_ssd.columns)
df_ssd_rename = df_ssd.toDF(*NewColumns)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/mnt/shelladlstech/raw/streaminput/` option("header",True) 

# COMMAND ----------

