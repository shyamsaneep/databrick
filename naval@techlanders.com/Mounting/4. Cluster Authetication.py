# Databricks notebook source
# MAGIC %md
# MAGIC ##### Set this config on Cluter

# COMMAND ----------

fs.azure.account.key.shelladlstech.dfs.core.windows.net H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A==

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@shelladlstech.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@shelladlstech.dfs.core.windows.net/demo/"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shelladlstech/

# COMMAND ----------

dbutils.fs.unmount()