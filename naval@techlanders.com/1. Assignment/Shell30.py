# Databricks notebook source
#Step1: Load the Sample superstore data in ALDS Gen 2 in new container
#Step 2: Mount ADLS container to databricks using (Access Key or SAS or Service Principal)
#Step 3: Read CSV file from container and convert into external delta table (creating dataframe or Querying using SQL)
#Step 4: Query Delta Table to get the grouped data of segments with sum of sales

# COMMAND ----------

# DBTITLE 1,Mount Gen2 Using Access Key
dbutils.fs.mount(
  source = "wasbs://input@sablobnly.blob.core.windows.net",
  mount_point = "/mnt/task",
  extra_configs = {"fs.azure.account.key.sablobnly.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/input/input/

# COMMAND ----------

dfsuperstore30=spark.read.option("header",True).csv("dbfs:/mnt/task/input/input/Superstore data.csv")

# COMMAND ----------

dfsuperstore30.display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/task/output/shell30

# COMMAND ----------

dfsuperstore30.write.format("delta").option("delta.columnMapping.mode", "name").option("path", "/mnt/task/output/shell30").saveAsTable("dfsuperstore30")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dfsuperstore30

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC segment,
# MAGIC sum(sales)
# MAGIC from
# MAGIC dfsuperstore30
# MAGIC group by 
# MAGIC segment