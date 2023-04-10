# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/task",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A==""})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/input/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/input/

# COMMAND ----------

from pyspark.sql import *;


# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/task/input/input/Superstore data.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/output/

# COMMAND ----------



# COMMAND ----------

df.write.format("delta").parquet("dbfs:/mnt/task/input/output/shell08/")

# COMMAND ----------

df1=df.select("segment","sales")

# COMMAND ----------


df1.write.format("delta").option("path","dbfs:/mnt/shell40task/output/shell08_delta").saveAsTable("shell08")

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment,Sum(sales) from shell08 group by segment