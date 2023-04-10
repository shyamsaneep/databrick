# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/input/

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://input@sablobnly.blob.core.windows.net",
  mount_point = "/mnt/sablobnly/input",
  extra_configs = {"fs.azure.account.key.sablobnly.blob.core.windows.net":
                   "lD/SDrU1kSTAAqr591TFmCqQKs3eQdOY+/pPwwJyuOMQTKjPuGNsIor+PpFjdznWDVVchqbedT+r+AStI7rCFg=="}
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sablobnly/input/

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/sablobnly/input/Superstore data.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df1=df.withColumnRenamed("Row ID","Row_id")
display(df1)

# COMMAND ----------

dffinal=df.select("Segment","City","Sales","Quantity")

# COMMAND ----------

display(dffinal)

# COMMAND ----------

df.write.parquet("dbfs:/mnt/sablobnly/input/parquet")

# COMMAND ----------

df.write.parquet("dbfs:/mnt/sablobnly/input/output/file.parquet")

# COMMAND ----------

dffinal.write.saveAsTable("superstore")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From superstore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE sample

# COMMAND ----------

# MAGIC %sql
# MAGIC USE sample

# COMMAND ----------

dffinal.write.format("delta").saveAsTable("superstore")

# COMMAND ----------

