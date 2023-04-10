# Databricks notebook source
# MAGIC %md
# MAGIC -- Access key 

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://input@shelladlstech.blob.core.windows.net",
  mount_point = "/mnt/inputnly/sample",
  extra_configs = {"fs.azure.account.key.shelladlstech.blob.core.windows.net":"H+CTOQGANmZsqKWE0e5ebnSRa7+ltSys5qghG9KP5aVsf7vAEdAPfblz/vX5GPEduDVWYYB7t/mo+ASt1WjL3A=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/inputnly/sample

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/inputnly/sample")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/inputnly/sample/parquet")

# COMMAND ----------

dbutils.fs.unmount("/mnt/inputnly/sample")

# COMMAND ----------

