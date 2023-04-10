# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC - Step1: Load the Sample superstore data in ALDS Gen 2 in new container
# MAGIC - Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)
# MAGIC - Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL) 
# MAGIC - Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://task@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/shell_31",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

df = spark.read.csv("abfss://task@shelladlstech.dfs.core.windows.net/input/Superstore data.csv",header=True)

# COMMAND ----------

df = spark.read.csv("/mnt/shell_31/input/Superstore data.csv",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

NewColumns=(column.replace(' ', '_') for column in df.columns)
df = df.toDF(*NewColumns)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://task@shelladlstech.dfs.core.windows.net/output/Shell_31/Superstore_output")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://task@shelladlstech.dfs.core.windows.net/output/Shell_31/Superstore_output`

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment,sum(sales) from 
# MAGIC delta.`abfss://task@shelladlstech.dfs.core.windows.net/output/Shell_31/Superstore_output`
# MAGIC group by segment