# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://input@sablobnly.blob.core.windows.net",
  mount_point = "/mnt/input",
  extra_configs = {"fs.azure.account.key.sablobnly.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-gb/azure/databricks/storage/wasb-blob

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://input@sablobnly.blob.core.windows.net",
  mount_point = "/mnt/input",
  extra_configs = {"fs.azure.account.key.sablobnly.blob.core.windows.net":
                   "lD/SDrU1kSTAAqr591TFmCqQKs3eQdOY+/pPwwJyuOMQTKjPuGNsIor+PpFjdznWDVVchqbedT+r+AStI7rCFg=="}
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/input/

# COMMAND ----------

dbutils.fs.unmount("/mnt/input")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://input@sablobnly.blob.core.windows.net",
  mount_point = "/mnt/input",
  extra_configs = {"fs.azure.account.key.sablobnly.blob.core.windows.net":
                   "lD/SDrU1kSTAAqr591TFmCqQKs3eQdOY+/pPwwJyuOMQTKjPuGNsIor+PpFjdznWDVVchqbedT+r+AStI7rCFg=="}
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://input@sablobnly.blob.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@shelladlstech.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@shelladlstech.dfs.core.windows.net/demo"))

# COMMAND ----------

