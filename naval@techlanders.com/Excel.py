# Databricks notebook source
df=spark.read.format("xlsx").load("dbfs:/mnt/shelladlstech/raw/demo/New york Taxi.xlsx")

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/shelladlstech/raw/demo/New york Taxi.xlsx")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("header",True).format("com.crealytics.spark.excel").load("dbfs:/mnt/shelladlstech/raw/demo/New york Taxi.xlsx")

# COMMAND ----------

display(df)

# COMMAND ----------

