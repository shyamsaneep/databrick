# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/shelladlstech/raw/streaminput/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/shelladlstech/raw/streaminput/")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

user_schema= StructType([StructField("Id",IntegerType()),
                         StructField("Name",StringType()),
                         StructField("Gender",StringType()),
                         StructField("Salary",IntegerType()),
                         StructField("Country",StringType()),
                         StructField("Date",StringType())
    
])

# COMMAND ----------

df=spark.readStream.option("header",True).schema(user_schema).csv("dbfs:/mnt/shelladlstech/raw/streaminput/")

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream.format("delta").trigger(processingTime="2 minutes").option("checkpointlocation","/mnt/shelladlstech/raw/streamoutput/checkpointdelta").table("firststream")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended firststream

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table firststream_mangaed

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(processingTime="2 seconds").option("checkpointlocation","/user/hive/warehouse/streamoutput/checkpointdelta").table("firststream_mangaed")

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(processingTime="2 seconds").option("checkpointlocation","/user/hive/warehouse/streamoutput/checkpointdelta2").table("firststream_mangaed_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from firststream

# COMMAND ----------

df.writeStream.format("parquet").option("path","/mnt/shelladlstech/raw/streamoutput/streasample").option("checkpointlocation","/mnt/shelladlstech/raw/streamoutput/checkpoint").start()

# COMMAND ----------

dfcheck=spark.read.parquet("/mnt/shelladlstech/raw/streamoutput/streasample")

# COMMAND ----------

display(dfcheck)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Use writeStream to convert to a delta table