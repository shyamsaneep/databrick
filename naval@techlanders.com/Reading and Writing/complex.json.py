# Databricks notebook source
df=spark.read.option("multiline",True).json("dbfs:/mnt/shelladlstech/raw/demo/complex.json")

# COMMAND ----------

display(df)

# COMMAND ----------

