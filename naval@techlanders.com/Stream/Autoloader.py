# Databricks notebook source
# MAGIC %md
# MAGIC Syntax
# MAGIC spark.readStream
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", <source_format>)
# MAGIC .load('/path/to/files’)
# MAGIC .writeStream
# MAGIC .option("checkpointLocation", <checkpoint_directory>)
# MAGIC .table(<table_name>)

# COMMAND ----------

(spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format","csv")
     .option("cloudFiles.schemaLocation","/mnt/shelladlstech/raw/streamoutput/autoloader/schemaLocation")
     .load("/mnt/shelladlstech/raw/streaminput/")
 .writeStream
     .option("checkpointLocation","/mnt/shelladlstech/raw/streamoutput/autoloader") 
     .table("autoloader_table")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended autoloader_table

# COMMAND ----------

(spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format","csv")
     .option("cloudFiles.inferColumnTypes","True")
     .option("cloudFiles.schemaEvolutionMode","rescue")
     .option("cloudFiles.schemaLocation","/mnt/shelladlstech/raw/streamoutput/autoloader_new/schemaLocation")
     .load("/mnt/shelladlstech/raw/streaminput/")
 .writeStream
     .option("checkpointLocation","/mnt/shelladlstech/raw/streamoutput/autoloader_new") 
     .option("mergeSchema", "true")
     .table("autoloader_table_new")
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader_table_new

# COMMAND ----------

(spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format","csv")
     .option("cloudFiles.inferColumnTypes","True")
     .option("cloudFiles.schemaEvolutionMode","rescue")
     .option("cloudFiles.schemaLocation","/mnt/shelladlstech/raw/streamoutput/autoloader_new_schema/schemaLocation")
     .load("/mnt/shelladlstech/raw/streaminput/")
 .writeStream
     .option("checkpointLocation","/mnt/shelladlstech/raw/streamoutput/autoloader_new_schema") 
     .table("autoloader_table_new_schema")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader_table_new_schema

# COMMAND ----------

df = spark.read.format('csv').option("header",True).load('dbfs:/mnt/shelladlstech/task/input/Superstore data.csv')

# COMMAND ----------

