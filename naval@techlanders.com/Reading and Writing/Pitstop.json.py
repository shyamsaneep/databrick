# Databricks notebook source
df=spark.read.json("dbfs:/mnt/sajay34/container34/pit_stops.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/shelladlstech/raw/demo/pit_stops.json")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(df1)

# COMMAND ----------

df.write.parquet("")

# COMMAND ----------

df1.write.save("dbfs:/mnt/shelladlstech/raw/output/pitstop")

# COMMAND ----------

