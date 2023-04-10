# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell01task",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/input/

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

