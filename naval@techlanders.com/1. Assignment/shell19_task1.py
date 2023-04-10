# Databricks notebook source
# MAGIC %md
# MAGIC ###Step1: Load the Sample superstore data in ALDS Gen 2 in new container
# MAGIC ###Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)
# MAGIC ###Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL)
# MAGIC ###Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell19_task1",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/shell19_task1

# COMMAND ----------

