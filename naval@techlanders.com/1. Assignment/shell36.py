# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/36task",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/36task/input

# COMMAND ----------

df1 = (
    spark.read.format("csv")
    .option("header", "true")
    .load("dbfs:/mnt/36task/input/Superstore data.csv")
)

# COMMAND ----------

display(df1)

# COMMAND ----------

from.pyspark.sql.types import *
from pysaprk.sql.functions import *

# COMMAND ----------

df2 = df1.withColumn("Sales",df1.Sales.cast("double"))

# COMMAND ----------

df3 = df2.groupby("Segment").sum("Sales").withColumnRenamed("sum(Sales)","Sales")

# COMMAND ----------

df3.write.format("delta").save("/mnt/task/output/shell36/Superstore")