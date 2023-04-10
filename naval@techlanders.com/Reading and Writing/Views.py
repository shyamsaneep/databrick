# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")
display(df)

# COMMAND ----------

df.write.saveAsTable("bikes")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Database viewsdemo

# COMMAND ----------

# MAGIC %sql
# MAGIC show DATABASEs

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC use default

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables like 'sam*'

# COMMAND ----------

# MAGIC %md
# MAGIC #### VIEW
# MAGIC 1. Standard View
# MAGIC 2. Temp View
# MAGIC 3. Global View

# COMMAND ----------

# MAGIC %sql
# MAGIC use viewsdemo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW bikeview as 
# MAGIC Select * from bikes

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended bikeview

# COMMAND ----------

df.createOrReplaceTempView("biketempview")

# COMMAND ----------

df.registerTempTable("xyz")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from biketempview

# COMMAND ----------

spark.sql("select * from biketempview").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

df.createOrReplaceGlobalTempView("bikeglobalview")

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp 

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp like '%view%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.bikeglobalview

# COMMAND ----------

